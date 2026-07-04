//! Page-granular Merkle radix tree over a VM memory image.
//!
//! A memory image is split into 4 KiB pages ([`PAGE`]). Each page is a leaf,
//! stored as a plain CAS blob. Pages are grouped [`FANOUT`]-at-a-time into inner
//! nodes (a `postcard`-encoded list of child hashes, itself a CAS blob), and
//! those are grouped again, until a single root remains. The tree height is a
//! pure function of the image length, so the structure is implicit: given a page
//! index, its path is the base-[`FANOUT`] digits of the index, most-significant
//! first.
//!
//! A [`MemTree`] is just `{ root, len }` — a single hash plus the byte length.
//! Two trees that share an untouched subtree share it *by hash*, so
//! [`update`]-ing a parent for a few dirty pages rewrites only the root-to-leaf
//! path for each dirty page and reuses every other subtree for free.
//!
//! Leaf vs. inner-node blobs are never confused: the traversal always knows the
//! current level (derived from `len`), so it knows whether a child hash points
//! at another node or at raw page bytes.

use std::future::Future;
use std::io;
use std::pin::Pin;

use futures_util::future::try_join_all;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::cas::{Cas, Hash};

/// Leaf size. Matches the guest page size, which is also the granularity at
/// which Firecracker tracks dirty pages and (later) faults under UFFD.
pub const PAGE: usize = 4096;

/// Children per inner node. 64 keeps the tree shallow (depth 3–5 across
/// 256 MiB–256 GiB images) while keeping inner nodes small (~2 KiB).
pub const FANOUT: usize = 64;

/// How many CAS `put`s to keep in flight at once. Each `put` is a blocking-pool
/// round-trip (stat/write/rename), so overlapping them hides per-op latency —
/// the difference between an O(pages) ingest being threadpool-latency-bound
/// (~100µs/page sequential) vs throughput-bound.
const PUT_CONCURRENCY: usize = 128;

/// Pages read per batch in [`build_from_reader`] before issuing a concurrent
/// put wave. Bounds peak memory at READ_BATCH × PAGE (here 4 MiB).
const READ_BATCH: usize = 1024;

/// A memory image as a Merkle tree: the root hash plus the exact byte length.
/// `len` is needed because the final page may be short and the height is
/// derived from it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemTree {
    pub root: Hash,
    pub len: u64,
}

/// Number of leaf pages for an image of `len` bytes (the last page may be
/// short). Zero-length images have zero pages.
fn page_count(len: u64) -> u64 {
    if len == 0 {
        0
    } else {
        len.div_ceil(PAGE as u64)
    }
}

/// Number of inner levels above the leaves: the smallest `h >= 1` such that
/// `FANOUT^h >= pages`. Always at least 1, so even a tiny image has one inner
/// node as its root (keeps build/assemble/update uniform — the root is always a
/// node, never a bare leaf).
fn height(pages: u64) -> u32 {
    let mut h = 1u32;
    let mut cap = FANOUT as u64;
    while cap < pages {
        cap = cap.saturating_mul(FANOUT as u64);
        h += 1;
    }
    h
}

/// Pages spanned by a single child of a node at `level` (level 1 = just above
/// leaves, where each child spans one page).
fn span(level: u32) -> u64 {
    (FANOUT as u64).pow(level - 1)
}

fn encode_node(children: &[Hash]) -> Vec<u8> {
    postcard::to_stdvec(children).expect("encoding a Vec<Hash> never fails")
}

fn decode_node(bytes: &[u8]) -> io::Result<Vec<Hash>> {
    postcard::from_bytes(bytes).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Build a tree from a complete in-memory image, writing every page and inner
/// node into `cas`. Identical pages dedup automatically. For large images
/// prefer [`build_from_reader`], which never holds the whole image in memory.
pub async fn build<C: Cas>(cas: &C, data: &[u8]) -> io::Result<MemTree> {
    let len = data.len() as u64;
    // Put pages in bounded concurrent waves of PUT_CONCURRENCY. Explicit loops
    // (no stream/closure) keep the futures' Send-ness provable across the host's
    // `tokio::spawn`: a borrowing closure over a stream trips a higher-ranked
    // "not general enough" lifetime error. `try_join_all` preserves order.
    let mut level: Vec<Hash> = Vec::with_capacity(page_count(len) as usize);
    for wave in data.chunks(PAGE * PUT_CONCURRENCY) {
        let mut futs = Vec::with_capacity(PUT_CONCURRENCY);
        for page in wave.chunks(PAGE) {
            futs.push(cas.put(page));
        }
        level.extend(try_join_all(futs).await?);
    }
    fold_to_root(cas, level, len).await
}

/// Build a tree by streaming an image from `reader` in batches — only
/// READ_BATCH pages plus the growing list of leaf hashes are held in memory, so
/// a multi-GiB mem file ingests without being loaded whole. Each batch's puts
/// run concurrently. This is the real capture-path entry point. Produces the
/// identical tree to [`build`] on the same bytes.
pub async fn build_from_reader<C, R>(cas: &C, reader: &mut R) -> io::Result<MemTree>
where
    C: Cas,
    R: AsyncRead + Unpin,
{
    let mut level: Vec<Hash> = Vec::new();
    let mut total: u64 = 0;
    // One large read per iteration (READ_BATCH pages), then put those pages
    // concurrently. Reading in 512 KiB chunks instead of per-page turns ~262k
    // tiny serial reads (for a 1 GiB image) into ~2k big ones — the reads were
    // the serial floor that bounded concurrency couldn't hide.
    let mut buf = vec![0u8; PAGE * READ_BATCH];
    loop {
        let filled = read_full(reader, &mut buf).await?;
        if filled == 0 {
            break;
        }
        total += filled as u64;
        let mut futs = Vec::with_capacity(READ_BATCH);
        for page in buf[..filled].chunks(PAGE) {
            futs.push(cas.put(page));
        }
        level.extend(try_join_all(futs).await?);
        if filled < buf.len() {
            break; // short read → EOF
        }
    }
    fold_to_root(cas, level, total).await
}

/// Fill `buf` from `reader` across short reads; returns bytes read (0 at EOF).
async fn read_full<R: AsyncRead + Unpin>(reader: &mut R, buf: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buf.len() {
        let n = reader.read(&mut buf[filled..]).await?;
        if n == 0 {
            break;
        }
        filled += n;
    }
    Ok(filled)
}

/// Convenience wrapper around [`build_from_reader`] for a file path.
pub async fn build_from_path<C: Cas>(
    cas: &C,
    path: impl AsRef<std::path::Path>,
) -> io::Result<MemTree> {
    let mut file = tokio::fs::File::open(path).await?;
    build_from_reader(cas, &mut file).await
}

/// Fold a complete list of leaf hashes up into inner nodes until a single root
/// remains, returning the finished tree. Shared by [`build`] and
/// [`build_from_reader`] so both produce identical trees.
async fn fold_to_root<C: Cas>(cas: &C, mut level: Vec<Hash>, len: u64) -> io::Result<MemTree> {
    // Empty image: a single empty inner node is the root.
    if level.is_empty() {
        let root = cas.put(&encode_node(&[])).await?;
        return Ok(MemTree { root, len: 0 });
    }
    // Fold up `height` times; after the last fold exactly one node remains.
    // Encode each level's nodes synchronously, then put them concurrently
    // (order-preserving) so the fold isn't serialized on per-node I/O.
    let h = height(level.len() as u64);
    for _ in 0..h {
        let encoded: Vec<Vec<u8>> = level.chunks(FANOUT).map(encode_node).collect();
        let mut next = Vec::with_capacity(encoded.len());
        for wave in encoded.chunks(PUT_CONCURRENCY) {
            let mut futs = Vec::with_capacity(wave.len());
            for node in wave {
                futs.push(cas.put(node.as_slice()));
            }
            next.extend(try_join_all(futs).await?);
        }
        level = next;
    }
    debug_assert_eq!(level.len(), 1, "fold must collapse to a single root");
    Ok(MemTree {
        root: level[0],
        len,
    })
}

/// Reassemble a tree's bytes into `out`, in order. Iterative in-order DFS, so it
/// never holds more than O(FANOUT × height) node hashes at once and streams leaf
/// bytes straight through.
pub async fn assemble<C, W>(cas: &C, tree: &MemTree, out: &mut W) -> io::Result<()>
where
    C: Cas,
    W: AsyncWrite + Unpin + Send,
{
    if tree.len == 0 {
        return Ok(());
    }
    let h = height(page_count(tree.len));

    // Stack of (level, hash). Level 0 means "this hash is a leaf". Children are
    // pushed in reverse so the leftmost is popped (and written) first.
    let mut stack: Vec<(u32, Hash)> = vec![(h, tree.root)];
    while let Some((level, node)) = stack.pop() {
        if level == 0 {
            let bytes = cas.get(&node).await?;
            out.write_all(&bytes).await?;
        } else {
            let children = decode_node(&cas.get(&node).await?)?;
            for child in children.into_iter().rev() {
                stack.push((level - 1, child));
            }
        }
    }
    Ok(())
}

/// Produce a new tree from `parent` with the given dirty pages replaced,
/// sharing every untouched subtree with the parent by hash.
///
/// `dirty` yields `(page_index, page_bytes)` in any order. After the pages are
/// put, only the lightweight `(index, hash)` pairs drive the rebuild, so the
/// tree-shaping cost is O(dirty pages × 40 bytes), not O(dirty bytes). The pairs
/// are sorted internally (stably, so a page listed twice resolves
/// last-write-wins) before the tree is rebuilt.
///
/// Assumes the image length is unchanged (a step doesn't resize guest RAM), so
/// the tree shape matches the parent's.
pub async fn update<C, I, B>(cas: &C, parent: &MemTree, dirty: I) -> io::Result<MemTree>
where
    C: Cas,
    I: IntoIterator<Item = (u64, B)>,
    B: AsRef<[u8]>,
{
    let items: Vec<(u64, B)> = dirty.into_iter().collect();
    if items.is_empty() {
        return Ok(*parent);
    }
    // Validate every dirty page against the parent's geometry up front. An
    // out-of-range index would otherwise panic deep inside `rebuild` (inside
    // the host's spawned capture task, past its VM cleanup), and a short
    // non-final page would silently shift every byte after it at `assemble`
    // time — the tree has no per-leaf lengths, so `assemble` just concatenates.
    let pages = page_count(parent.len);
    if pages == 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "dirty pages supplied for a zero-length parent image",
        ));
    }
    let last_page_len = (parent.len - (pages - 1) * PAGE as u64) as usize;
    for (page, bytes) in &items {
        if *page >= pages {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("dirty page {page} out of range (parent has {pages} pages)"),
            ));
        }
        let expected = if *page == pages - 1 { last_page_len } else { PAGE };
        let got = bytes.as_ref().len();
        if got != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("dirty page {page} is {got} bytes, expected {expected}"),
            ));
        }
    }
    // Put dirty pages in bounded concurrent waves (same as `build`), pairing
    // each hash back to its page index. Awaiting one put per page is
    // threadpool-latency-bound (~one fs round-trip per page); waves of
    // PUT_CONCURRENCY make it throughput-bound. Explicit loops (no stream
    // combinator) keep the futures Send across the host's `tokio::spawn`.
    let mut leaves: Vec<(u64, Hash)> = Vec::with_capacity(items.len());
    for wave in items.chunks(PUT_CONCURRENCY) {
        let mut futs = Vec::with_capacity(wave.len());
        for (_, bytes) in wave {
            futs.push(cas.put(bytes.as_ref()));
        }
        let hashes = try_join_all(futs).await?;
        leaves.extend(wave.iter().map(|&(page, _)| page).zip(hashes));
    }
    // Sort by page index so `rebuild`'s partitioning is correct regardless of
    // input order. Stable, so a page supplied twice keeps last-write-wins.
    leaves.sort_by_key(|&(page, _)| page);

    let h = height(page_count(parent.len));
    let root = rebuild(cas, h, parent.root, 0, &leaves).await?;
    Ok(MemTree {
        root,
        len: parent.len,
    })
}

/// Rebuild one node, recursing only into children that contain dirty pages.
/// `node` is the parent's node hash for this position; `page_base` is the first
/// page index this subtree covers; `dirty` is the (ascending) slice of dirty
/// leaves falling within this subtree. Returns the new node hash.
///
/// Affected children are rebuilt concurrently: sibling subtrees are independent,
/// so a scattered dirty set (which touches many distinct nodes, each costing a
/// `get`+`put`) fans out instead of serializing one round-trip per node. The
/// per-level fan-out is naturally bounded by FANOUT and total in-flight I/O by
/// the runtime's blocking pool.
///
/// Boxed because it's recursive `async`. `node`/hashes are `Copy`, and `dirty`
/// is a sub-slice of the caller's slice, so no lifetime gymnastics are needed.
fn rebuild<'a, C: Cas>(
    cas: &'a C,
    level: u32,
    node: Hash,
    page_base: u64,
    dirty: &'a [(u64, Hash)],
) -> Pin<Box<dyn Future<Output = io::Result<Hash>> + Send + 'a>> {
    Box::pin(async move {
        let mut children = decode_node(&cas.get(&node).await?)?;
        if level == 1 {
            // Children are leaves: drop in the replacement hashes directly.
            // Checked indexing: `update` validated page ranges against
            // `parent.len`, but a stored node inconsistent with that length
            // (truncated/corrupt) must surface as an error, not a panic.
            for &(page, leaf) in dirty {
                let idx = (page - page_base) as usize;
                let slot = children.get_mut(idx).ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("node at page_base {page_base} has no child {idx} for page {page}"),
                    )
                })?;
                *slot = leaf;
            }
        } else {
            // Partition the (sorted) dirty slice by child subtree and recurse
            // only into the affected children, concurrently — sibling subtrees
            // are independent, so this fans out rather than serializing a
            // round-trip per node. `children[idx]` (the old child hash) is Copy,
            // so the futures own it and don't borrow `children`.
            let child_span = span(level);
            let mut idxs: Vec<usize> = Vec::new();
            let mut futs = Vec::new();
            let mut rest = dirty;
            while let Some(&(first, _)) = rest.first() {
                let idx = ((first - page_base) / child_span) as usize;
                let child_base = page_base + idx as u64 * child_span;
                let end = rest.partition_point(|&(p, _)| p < child_base + child_span);
                let (group, tail) = rest.split_at(end);
                idxs.push(idx);
                futs.push(rebuild(cas, level - 1, children[idx], child_base, group));
                rest = tail;
            }
            for (idx, hash) in idxs.into_iter().zip(try_join_all(futs).await?) {
                children[idx] = hash;
            }
        }
        cas.put(&encode_node(&children)).await
    })
}

/// A blob a holder is missing, with where it sits in the tree so a puller can
/// place it: `level == 0` is a leaf (page) whose bytes go at `page_base * PAGE`;
/// `level >= 1` is an inner node covering `FANOUT^level` pages from `page_base`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Missing {
    pub hash: Hash,
    pub level: u32,
    pub page_base: u64,
}

/// The blobs a holder lacks for some tree, in top-down order (parents before
/// children) so a puller can decode an inner node before its children arrive.
#[derive(Debug, Default, Clone)]
pub struct Diff {
    pub missing: Vec<Missing>,
}

impl Diff {
    /// Just the hashes (the push side: what to upload / fetch).
    pub fn missing_blobs(&self) -> Vec<Hash> {
        self.missing.iter().map(|m| m.hash).collect()
    }
}

/// Compute the blobs `holder` lacks to hold `tree`, reading node bytes from
/// `src` to expand the subtrees the holder is missing.
///
/// `src` is where the tree's blobs live (used to fetch a missing node's child
/// list); `holder` is what we diff against (asked only `has`). Any subtree whose
/// root the holder already has is shared by hash and pruned wholesale — no
/// descent, no transfer. So a held subtree costs one `has`, a missing inner node
/// one `get`, a missing leaf neither. Affected siblings are expanded
/// concurrently, mirroring [`rebuild`].
///
/// "Previous state" is whatever the holder holds (an ancestor, ≥ the base), so
/// this is the previous→current delta. For a single store use [`diff`]; in a
/// real push/pull `src` and `holder` differ — swapping them silently transfers
/// everything.
pub async fn diff_between<S: Cas, H: Cas>(
    src: &S,
    holder: &H,
    tree: &MemTree,
) -> io::Result<Diff> {
    let h = height(page_count(tree.len));
    let missing = diff_node(src, holder, h, tree.root, 0).await?;
    Ok(Diff { missing })
}

/// [`diff_between`] where the tree's blobs and the holder are the same store —
/// always empty (you have everything). A plumbing/negative check; the meaningful
/// cases pass distinct `src`/`holder`.
pub async fn diff<C: Cas>(cas: &C, tree: &MemTree) -> io::Result<Diff> {
    diff_between(cas, cas, tree).await
}

/// One node of the diff walk. Boxed because it's recursive `async`; `Send` so it
/// survives the host's `tokio::spawn` (guarded by `diff_future_is_spawnable`).
fn diff_node<'a, S: Cas, H: Cas>(
    src: &'a S,
    holder: &'a H,
    level: u32,
    hash: Hash,
    page_base: u64,
) -> Pin<Box<dyn Future<Output = io::Result<Vec<Missing>>> + Send + 'a>> {
    Box::pin(async move {
        if holder.has(&hash).await? {
            return Ok(Vec::new());
        }
        let mut missing = vec![Missing {
            hash,
            level,
            page_base,
        }];
        if level >= 1 {
            let children = decode_node(&src.get(&hash).await?)?;
            let child_span = span(level);
            let mut futs = Vec::with_capacity(children.len());
            for (i, &child) in children.iter().enumerate() {
                futs.push(diff_node(
                    src,
                    holder,
                    level - 1,
                    child,
                    page_base + i as u64 * child_span,
                ));
            }
            for sub in try_join_all(futs).await? {
                missing.extend(sub);
            }
        }
        Ok(missing)
    })
}

/// Where a piece of content already lives locally: an opaque `source` plus the
/// page offset within it where the matching content begins. `source` is whatever
/// the caller uses to name a local image (a path, an id, …).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Located<S> {
    pub source: S,
    pub src_page_base: u64,
}

/// "Is this subtree's content available locally, and where?" — keyed by the
/// subtree's content hash, so it answers for whole subtrees and single leaves
/// alike. `Some` means the *entire* subtree under `hash` can be cloned from the
/// returned location (lineage *or* cross-lineage — the hash is all that matters).
pub trait Locate: Sync {
    type Source: Copy + Send;
    fn locate(&self, hash: &Hash) -> Option<Located<Self::Source>>;
}

/// One step of materializing an image: clone a contiguous run already held
/// locally, or fetch a run that isn't.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Op<S> {
    /// Clone `pages` pages from `src` into the destination starting at page
    /// `dest_page_base` (a reflink-range for a real run; a copy for one page).
    Clone {
        src: Located<S>,
        dest_page_base: u64,
        pages: u64,
    },
    /// The `pages` pages at `dest_page_base` (subtree/leaf `hash`) aren't local
    /// — fetch them. In P3 (no network yet) any `Gap` is an error.
    Gap {
        hash: Hash,
        dest_page_base: u64,
        pages: u64,
    },
}

/// Pages covered by a subtree rooted at `level` (level 0 = one page).
fn subtree_pages(level: u32) -> u64 {
    (FANOUT as u64).pow(level)
}

/// Plan how to materialize `tree`'s image from locally-held content (`loc`),
/// fetching only what isn't local. `nodes` supplies the tree's inner-node blobs
/// to walk. A subtree whose hash the locator knows becomes one [`Op::Clone`];
/// otherwise we descend, recovering any matching sub-subtrees (including
/// cross-lineage) and emitting [`Op::Gap`] only for genuinely-absent leaves.
///
/// The walk is sequential (planning is cheap — O(affected nodes) — and the plan
/// is built top-down, left-to-right so ops apply in a deterministic order).
pub async fn plan_materialize<C: Cas, L: Locate>(
    nodes: &C,
    tree: &MemTree,
    loc: &L,
) -> io::Result<Vec<Op<L::Source>>> {
    let total_pages = page_count(tree.len);
    let h = height(total_pages);
    plan_node(nodes, loc, h, tree.root, 0, total_pages).await
}

fn plan_node<'a, C: Cas, L: Locate>(
    nodes: &'a C,
    loc: &'a L,
    level: u32,
    hash: Hash,
    page_base: u64,
    total_pages: u64,
) -> Pin<Box<dyn Future<Output = io::Result<Vec<Op<L::Source>>>> + Send + 'a>> {
    Box::pin(async move {
        // Pages this subtree actually covers (the final subtree may be partial).
        let pages = subtree_pages(level).min(total_pages.saturating_sub(page_base));
        if pages == 0 {
            return Ok(Vec::new());
        }
        if let Some(src) = loc.locate(&hash) {
            return Ok(vec![Op::Clone {
                src,
                dest_page_base: page_base,
                pages,
            }]);
        }
        if level == 0 {
            return Ok(vec![Op::Gap {
                hash,
                dest_page_base: page_base,
                pages: 1,
            }]);
        }
        let children = decode_node(&nodes.get(&hash).await?)?;
        let child_span = span(level);
        let mut ops = Vec::new();
        for (i, &child) in children.iter().enumerate() {
            let sub = plan_node(
                nodes,
                loc,
                level - 1,
                child,
                page_base + i as u64 * child_span,
                total_pages,
            )
            .await?;
            ops.extend(sub);
        }
        Ok(ops)
    })
}

/// Enumerate every blob of `tree` with where it sits: `(hash, level, page_base)`
/// for each inner node and leaf. A materializer builds its content index
/// (hash → location) from this, so it can later clone a subtree it already holds
/// by hash. Iterative (O(blobs) memory) — see the index-size note in the plan.
pub async fn index_blobs<C: Cas>(cas: &C, tree: &MemTree) -> io::Result<Vec<(Hash, u32, u64)>> {
    let total = page_count(tree.len);
    let h = height(total);
    let mut out = Vec::new();
    let mut stack = vec![(h, tree.root, 0u64)];
    while let Some((level, node, page_base)) = stack.pop() {
        out.push((node, level, page_base));
        if level >= 1 {
            let children = decode_node(&cas.get(&node).await?)?;
            let child_span = span(level);
            for (i, &child) in children.iter().enumerate() {
                stack.push((level - 1, child, page_base + i as u64 * child_span));
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cas::LocalCas;
    use std::collections::{HashMap, HashSet};
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// In-memory CAS that counts `put` calls and how many of them were actual
    /// writes (cache misses), so tests can assert dedup and structural sharing.
    #[derive(Default)]
    struct MemCas {
        map: Mutex<HashMap<Hash, Vec<u8>>>,
        puts: AtomicUsize,
        writes: AtomicUsize,
    }

    impl MemCas {
        fn writes(&self) -> usize {
            self.writes.load(Ordering::Relaxed)
        }
        fn puts(&self) -> usize {
            self.puts.load(Ordering::Relaxed)
        }
    }

    impl Cas for MemCas {
        async fn put(&self, bytes: &[u8]) -> io::Result<Hash> {
            self.puts.fetch_add(1, Ordering::Relaxed);
            let h = Hash::of(bytes);
            let mut map = self.map.lock().unwrap();
            if !map.contains_key(&h) {
                self.writes.fetch_add(1, Ordering::Relaxed);
                map.insert(h, bytes.to_vec());
            }
            Ok(h)
        }
        async fn get(&self, hash: &Hash) -> io::Result<Vec<u8>> {
            self.map
                .lock()
                .unwrap()
                .get(hash)
                .cloned()
                .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "missing blob"))
        }
        async fn has(&self, hash: &Hash) -> io::Result<bool> {
            Ok(self.map.lock().unwrap().contains_key(hash))
        }
    }

    /// Deterministic pseudo-random bytes (xorshift) so tests don't pull a RNG
    /// crate and are reproducible.
    fn pseudo(seed: u64, n: usize) -> Vec<u8> {
        let mut x = seed | 1;
        let mut v = Vec::with_capacity(n);
        for _ in 0..n {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            v.push((x & 0xff) as u8);
        }
        v
    }

    async fn assemble_vec<C: Cas>(cas: &C, tree: &MemTree) -> Vec<u8> {
        let mut out = Vec::new();
        assemble(cas, tree, &mut out).await.unwrap();
        out
    }

    #[tokio::test]
    async fn roundtrip_across_sizes() {
        let cas = MemCas::default();
        // Sizes chosen to exercise: empty, sub-page, page boundaries, a short
        // final page, exactly-full level-1 (64 pages), spilling to level 2
        // (65 pages), and a deeper level-2 tree (100 pages).
        let sizes = [
            0usize,
            1,
            PAGE - 1,
            PAGE,
            PAGE + 1,
            63 * PAGE,
            64 * PAGE,
            64 * PAGE + 1,
            65 * PAGE,
            100 * PAGE + 123,
        ];
        for &n in &sizes {
            let data = pseudo(n as u64 + 1, n);
            let tree = build(&cas, &data).await.unwrap();
            assert_eq!(tree.len, n as u64);
            let out = assemble_vec(&cas, &tree).await;
            assert_eq!(out, data, "roundtrip failed for size {n}");
        }
    }

    #[tokio::test]
    async fn dedup_identical_pages() {
        let cas = MemCas::default();
        // An image of 10 identical pages should store exactly ONE leaf blob.
        let one = pseudo(42, PAGE);
        let data: Vec<u8> = std::iter::repeat(one.iter().copied())
            .take(10)
            .flatten()
            .collect();
        build(&cas, &data).await.unwrap();
        // 10 leaf puts but only 1 distinct leaf write.
        assert!(cas.puts() >= 10, "expected >=10 puts, got {}", cas.puts());
        // 10 pages fit one level-1 node (fanout 64), so height is 1 and that
        // node is the root. Distinct writes: 1 leaf + 1 root node = 2.
        assert_eq!(cas.writes(), 2, "identical pages must dedup to one leaf");
    }

    #[tokio::test]
    async fn update_rejects_out_of_range_page() {
        let cas = MemCas::default();
        let data = pseudo(7, 4 * PAGE);
        let tree = build(&cas, &data).await.unwrap();
        // Page 4 is one past the end of a 4-page image.
        let err = update(&cas, &tree, [(4u64, vec![0u8; PAGE])])
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn update_rejects_short_non_final_page() {
        let cas = MemCas::default();
        let data = pseudo(9, 4 * PAGE);
        let tree = build(&cas, &data).await.unwrap();
        // A short page anywhere but the final slot would shift every byte
        // after it at assemble time — must be rejected, not stored.
        let err = update(&cas, &tree, [(1u64, vec![0u8; 100])])
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
    }

    #[tokio::test]
    async fn update_rejects_wrong_final_page_len() {
        let cas = MemCas::default();
        // Image with a short final page (123 bytes): updates to that page must
        // be exactly 123 bytes — a full-PAGE replacement would grow the image
        // without updating len.
        let data = pseudo(11, 2 * PAGE + 123);
        let tree = build(&cas, &data).await.unwrap();
        let err = update(&cas, &tree, [(2u64, vec![0u8; PAGE])])
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidInput);
        // The exact final-page length is accepted.
        let ok = update(&cas, &tree, [(2u64, vec![0u8; 123])]).await.unwrap();
        let out = assemble_vec(&cas, &ok).await;
        assert_eq!(out.len(), data.len());
        assert_eq!(&out[2 * PAGE..], &vec![0u8; 123][..]);
    }

    #[tokio::test]
    async fn update_roundtrips_and_shares_subtrees() {
        let cas = MemCas::default();
        let pages = 100; // height 2, two level-1 nodes (0..63 and 64..99)
        let data = pseudo(7, pages * PAGE);
        let parent = build(&cas, &data).await.unwrap();

        // Dirty one page in each level-1 node: page 3 and page 70.
        let mut expected = data.clone();
        let dirty_idx = [3usize, 70];
        let mut dirty: Vec<(u64, Vec<u8>)> = Vec::new();
        for &i in &dirty_idx {
            let new_page = pseudo(9000 + i as u64, PAGE);
            expected[i * PAGE..(i + 1) * PAGE].copy_from_slice(&new_page);
            dirty.push((i as u64, new_page));
        }

        let writes_before = cas.writes();
        let child = update(&cas, &parent, dirty).await.unwrap();
        let new_writes = cas.writes() - writes_before;

        // Correctness: child reassembles to the modified image.
        let out = assemble_vec(&cas, &child).await;
        assert_eq!(out, expected);

        // Sharing: rewriting 2 pages touches only their two level-1 nodes and
        // the root — 2 leaves + 2 nodes + 1 root = 5 new blobs, NOT ~100.
        assert_eq!(new_writes, 5, "update wrote {new_writes} blobs; expected 5");

        // Parent is untouched and still reassembles to the original.
        let parent_out = assemble_vec(&cas, &parent).await;
        assert_eq!(parent_out, data);
    }

    #[tokio::test]
    async fn update_noop_when_no_dirty() {
        let cas = MemCas::default();
        let data = pseudo(3, 5 * PAGE);
        let parent = build(&cas, &data).await.unwrap();
        let empty: Vec<(u64, Vec<u8>)> = Vec::new();
        let child = update(&cas, &parent, empty).await.unwrap();
        assert_eq!(child, parent, "empty dirty set must return the parent tree");
    }

    #[tokio::test]
    async fn build_and_assemble_over_local_cas() {
        // Exercise the real filesystem backend end-to-end, not just MemCas.
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCas::new(dir.path()).unwrap();
        let data = pseudo(123, 70 * PAGE + 7);
        let tree = build(&cas, &data).await.unwrap();
        let out = assemble_vec(&cas, &tree).await;
        assert_eq!(out, data);
    }

    /// THE load-bearing invariant: a frame's root is a pure function of its
    /// content, regardless of how it was produced. `update`-ing a parent to
    /// reach image B must yield the *identical* root to `build`-ing B from
    /// scratch — otherwise frames with the same memory wouldn't dedup.
    /// Covers same-node dirties (3,5), a cross-node dirty (70), and the last
    /// page (99).
    #[tokio::test]
    async fn build_and_update_converge_to_same_root() {
        let cas = MemCas::default();
        let pages = 100;
        let a = pseudo(1, pages * PAGE);
        let tree_a = build(&cas, &a).await.unwrap();

        let mut b = a.clone();
        let changed = [3usize, 5, 70, 99];
        let mut dirty = Vec::new();
        for &i in &changed {
            let np = pseudo(500 + i as u64, PAGE);
            b[i * PAGE..(i + 1) * PAGE].copy_from_slice(&np);
            dirty.push((i as u64, np));
        }

        let built = build(&cas, &b).await.unwrap();
        let updated = update(&cas, &tree_a, dirty).await.unwrap();

        assert_eq!(updated.root, built.root, "update must converge to build");
        assert_eq!(updated.len, built.len);
        assert_eq!(assemble_vec(&cas, &updated).await, b);
    }

    /// Exercise a height-3 tree (>4096 pages), so the multi-level fold in
    /// `build` and the recursion/partitioning in `rebuild` run at depth >2.
    /// Dirties a deep-left page, the last page of the first level-2 subtree,
    /// and the lone page on the right spine.
    #[tokio::test]
    async fn height_three_build_update_assemble() {
        let cas = MemCas::default();
        let pages = 4097; // 64^2 = 4096 < 4097 → height 3
        let a = pseudo(11, pages * PAGE);
        let tree_a = build(&cas, &a).await.unwrap();
        assert_eq!(height(page_count(tree_a.len)), 3, "expected a height-3 tree");
        assert_eq!(assemble_vec(&cas, &tree_a).await, a);

        let mut b = a.clone();
        let changed = [5usize, 4095, 4096];
        let mut dirty = Vec::new();
        for &i in &changed {
            let np = pseudo(7000 + i as u64, PAGE);
            b[i * PAGE..(i + 1) * PAGE].copy_from_slice(&np);
            dirty.push((i as u64, np));
        }

        let built = build(&cas, &b).await.unwrap();
        let updated = update(&cas, &tree_a, dirty).await.unwrap();
        assert_eq!(updated.root, built.root, "height-3 update must converge");
        assert_eq!(assemble_vec(&cas, &updated).await, b);
    }

    /// Dirty boundary pages: index 0, the page just before a child boundary
    /// (63), the page at the boundary (64), and the final page. Two of these
    /// share the first level-1 node; converging with `build` proves the
    /// partition + last-(partial-)node indexing is correct.
    #[tokio::test]
    async fn update_boundary_pages_converge() {
        let cas = MemCas::default();
        let pages = 100;
        let a = pseudo(21, pages * PAGE);
        let tree_a = build(&cas, &a).await.unwrap();

        let mut b = a.clone();
        let changed = [0usize, 63, 64, 99];
        let mut dirty = Vec::new();
        for &i in &changed {
            let np = pseudo(800 + i as u64, PAGE);
            b[i * PAGE..(i + 1) * PAGE].copy_from_slice(&np);
            dirty.push((i as u64, np));
        }

        let built = build(&cas, &b).await.unwrap();
        let updated = update(&cas, &tree_a, dirty).await.unwrap();
        assert_eq!(updated.root, built.root);
        assert_eq!(assemble_vec(&cas, &updated).await, b);
    }

    /// Replace every page in one shot; the result must equal a fresh build of
    /// the new content, same root.
    #[tokio::test]
    async fn update_all_pages_converges() {
        let cas = MemCas::default();
        let pages = 70;
        let a = pseudo(31, pages * PAGE);
        let tree_a = build(&cas, &a).await.unwrap();

        let b = pseudo(32, pages * PAGE);
        let dirty: Vec<(u64, Vec<u8>)> = (0..pages)
            .map(|i| (i as u64, b[i * PAGE..(i + 1) * PAGE].to_vec()))
            .collect();

        let built = build(&cas, &b).await.unwrap();
        let updated = update(&cas, &tree_a, dirty).await.unwrap();
        assert_eq!(updated.root, built.root);
        assert_eq!(assemble_vec(&cas, &updated).await, b);
    }

    /// Updating the short final page (partial page) replaces it correctly and
    /// converges with a build of the new content.
    #[tokio::test]
    async fn update_short_final_page() {
        let cas = MemCas::default();
        let len = 5 * PAGE + 100; // final page is 100 bytes
        let data = pseudo(8, len);
        let parent = build(&cas, &data).await.unwrap();
        let last = page_count(parent.len) - 1;

        let mut b = data.clone();
        let new_tail = pseudo(99, 100);
        b[last as usize * PAGE..].copy_from_slice(&new_tail);

        let updated = update(&cas, &parent, vec![(last, new_tail)]).await.unwrap();
        assert_eq!(assemble_vec(&cas, &updated).await, b);
        let built = build(&cas, &b).await.unwrap();
        assert_eq!(updated.root, built.root);
    }

    /// "Dirtying" pages with their existing bytes must not change the root —
    /// content addressing means identical content collapses to the same tree.
    #[tokio::test]
    async fn update_with_identical_bytes_keeps_root() {
        let cas = MemCas::default();
        let data = pseudo(5, 70 * PAGE);
        let parent = build(&cas, &data).await.unwrap();
        let dirty = vec![
            (1u64, data[PAGE..2 * PAGE].to_vec()),
            (2u64, data[2 * PAGE..3 * PAGE].to_vec()),
        ];
        let child = update(&cas, &parent, dirty).await.unwrap();
        assert_eq!(child.root, parent.root, "rewriting identical bytes must keep the root");
    }

    /// Streaming build must produce the identical tree to the in-memory build
    /// across sizes (including short final pages and multi-level trees).
    #[tokio::test]
    async fn build_from_reader_matches_build() {
        let cas = MemCas::default();
        let sizes = [0usize, 1, PAGE - 1, PAGE, PAGE + 1, 65 * PAGE, 100 * PAGE + 123];
        for &n in &sizes {
            let data = pseudo(n as u64 + 1, n);
            let t_slice = build(&cas, &data).await.unwrap();
            // `&[u8]` implements tokio's AsyncRead, consuming as it's read.
            let mut reader: &[u8] = &data;
            let t_stream = build_from_reader(&cas, &mut reader).await.unwrap();
            assert_eq!(t_slice, t_stream, "stream vs slice mismatch at size {n}");
        }
    }

    /// `build_from_path` over a real file matches the in-memory build.
    #[tokio::test]
    async fn build_from_path_matches_build() {
        let cas = MemCas::default();
        let data = pseudo(5, 70 * PAGE + 7);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("mem");
        tokio::fs::write(&path, &data).await.unwrap();

        let t_path = build_from_path(&cas, &path).await.unwrap();
        let t_slice = build(&cas, &data).await.unwrap();
        assert_eq!(t_path, t_slice);

        // And it reassembles to the original file's bytes.
        assert_eq!(assemble_vec(&cas, &t_path).await, data);
    }

    /// Reproduces the host's requirement: `build_from_path` over a concrete
    /// `LocalCas` must produce a `Send` future, because the host awaits it
    /// inside a `tokio::spawn`ed task (`tokio::spawn` requires `Send + 'static`).
    /// If the ingest future isn't `Send`, THIS TEST FAILS TO COMPILE — same
    /// error the host build hits, but caught locally.
    #[tokio::test]
    async fn ingest_future_is_spawnable() {
        let dir = tempfile::tempdir().unwrap();
        let cas = std::sync::Arc::new(LocalCas::new(dir.path()).unwrap());
        let path = dir.path().join("mem");
        tokio::fs::write(&path, vec![7u8; 5 * PAGE]).await.unwrap();

        // Owns `cas` + `path` ('static); borrows them across the await inside —
        // exactly the shape of the host's spawned step_frame.
        let handle = tokio::spawn(async move { build_from_path(&*cas, &path).await });
        let tree = handle.await.unwrap().unwrap();
        assert_eq!(tree.len, (5 * PAGE) as u64);
    }

    /// `update` over a concrete `LocalCas` must also be `Send`-spawnable — the
    /// host calls it inside the spawned `step_frame`. Compile-fails if not.
    #[tokio::test]
    async fn update_future_is_spawnable() {
        let dir = tempfile::tempdir().unwrap();
        let cas = std::sync::Arc::new(LocalCas::new(dir.path()).unwrap());
        let parent = build(&*cas, &pseudo(1, 10 * PAGE)).await.unwrap();
        let dirty: Vec<(u64, Vec<u8>)> = vec![(2, pseudo(99, PAGE))];
        let handle = tokio::spawn(async move { update(&*cas, &parent, dirty).await });
        let child = handle.await.unwrap().unwrap();
        assert_eq!(child.len, parent.len);
    }

    /// `build` is deterministic: same bytes → same root, twice.
    #[tokio::test]
    async fn build_is_deterministic() {
        let cas = MemCas::default();
        let data = pseudo(77, 65 * PAGE + 9);
        let t1 = build(&cas, &data).await.unwrap();
        let t2 = build(&cas, &data).await.unwrap();
        assert_eq!(t1, t2);
    }

    /// Out-of-order dirty input must still converge (we sort internally).
    #[tokio::test]
    async fn update_unsorted_dirty_converges() {
        let cas = MemCas::default();
        let pages = 100;
        let a = pseudo(61, pages * PAGE);
        let tree_a = build(&cas, &a).await.unwrap();

        let mut b = a.clone();
        // Deliberately scrambled order, spanning both level-1 nodes.
        let changed = [70usize, 3, 99, 64, 0, 5];
        let mut dirty = Vec::new();
        for &i in &changed {
            let np = pseudo(1200 + i as u64, PAGE);
            b[i * PAGE..(i + 1) * PAGE].copy_from_slice(&np);
            dirty.push((i as u64, np));
        }

        let built = build(&cas, &b).await.unwrap();
        let updated = update(&cas, &tree_a, dirty).await.unwrap();
        assert_eq!(updated.root, built.root, "unsorted input must converge");
        assert_eq!(assemble_vec(&cas, &updated).await, b);
    }

    /// Property test: across many random sizes (heights 1–3), partial tails,
    /// and random dirty subsets, `update` must always converge with `build`
    /// and reassemble correctly. This is the real confidence-builder — it
    /// hits partition/boundary combinations no hand-written case enumerates.
    #[tokio::test]
    async fn randomized_update_always_converges() {
        let cas = MemCas::default();
        let mut seed = 0x1234_5678_9abc_def0u64;
        let mut rng = || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };

        for iter in 0..200 {
            let pages = (rng() % 300 + 1) as usize; // 1..=300 → heights 1..3
            let tail = (rng() % PAGE as u64) as usize; // partial final page
            let len = (pages - 1) * PAGE + if tail == 0 { PAGE } else { tail };

            let a = pseudo(rng(), len);
            let tree_a = build(&cas, &a).await.unwrap();

            // Random subset of pages to dirty (often includes 0, last, and
            // varying densities; sometimes empty).
            let mut b = a.clone();
            let mut dirty: Vec<(u64, Vec<u8>)> = Vec::new();
            for p in 0..pages {
                if rng() % 3 == 0 {
                    let start = p * PAGE;
                    let end = (start + PAGE).min(len);
                    let np = pseudo(rng(), end - start);
                    b[start..end].copy_from_slice(&np);
                    dirty.push((p as u64, np));
                }
            }

            let built = build(&cas, &b).await.unwrap();
            let updated = update(&cas, &tree_a, dirty).await.unwrap();
            assert_eq!(
                updated.root, built.root,
                "iter {iter}: diverged (pages={pages}, len={len})"
            );
            assert_eq!(
                assemble_vec(&cas, &updated).await,
                b,
                "iter {iter}: reassembly mismatch (pages={pages}, len={len})"
            );
        }
    }

    // ---- Phase 1: tree-diff ----

    /// A holder that owns a set of hashes (answers `has`). `get`/`put` are never
    /// called by `diff` and panic if hit — modelling the source/holder split.
    struct SetHolder {
        held: HashSet<Hash>,
    }
    impl Cas for SetHolder {
        async fn put(&self, _bytes: &[u8]) -> io::Result<Hash> {
            unreachable!("diff never puts to the holder")
        }
        async fn get(&self, _hash: &Hash) -> io::Result<Vec<u8>> {
            unreachable!("diff never gets from the holder")
        }
        async fn has(&self, hash: &Hash) -> io::Result<bool> {
            Ok(self.held.contains(hash))
        }
    }

    /// Every distinct blob hash reachable from `tree` (nodes + leaves), walking
    /// like `assemble` but recording hashes instead of bytes.
    async fn collect_blobs<C: Cas>(cas: &C, tree: &MemTree) -> HashSet<Hash> {
        let mut set = HashSet::new();
        let h = height(page_count(tree.len));
        let mut stack = vec![(h, tree.root)];
        while let Some((level, node)) = stack.pop() {
            set.insert(node);
            if level >= 1 {
                let children = decode_node(&cas.get(&node).await.unwrap()).unwrap();
                for c in children {
                    stack.push((level - 1, c));
                }
            }
        }
        set
    }

    #[tokio::test]
    async fn diff_self_is_empty() {
        let cas = MemCas::default();
        let tree = build(&cas, &pseudo(7, 100 * PAGE)).await.unwrap();
        let d = diff(&cas, &tree).await.unwrap();
        assert!(
            d.missing.is_empty(),
            "a full holder must miss nothing; missed {}",
            d.missing.len()
        );
    }

    #[tokio::test]
    async fn diff_against_empty_holder_is_all_blobs() {
        let cas = MemCas::default();
        let tree = build(&cas, &pseudo(11, 70 * PAGE)).await.unwrap();
        let all = collect_blobs(&cas, &tree).await;
        let holder = SetHolder {
            held: HashSet::new(),
        };
        let d = diff_between(&cas, &holder, &tree).await.unwrap();
        let got: HashSet<Hash> = d.missing_blobs().into_iter().collect();
        assert_eq!(got, all, "an empty holder must miss every distinct blob");
    }

    /// THE core claim: diffing child F against a holder that has ancestor X's
    /// blobs yields exactly blobs(F) \ blobs(X) — the previous→current delta.
    #[tokio::test]
    async fn diff_lineage_delta() {
        let cas = MemCas::default();
        let pages = 100;
        let tree_x = build(&cas, &pseudo(7, pages * PAGE)).await.unwrap();
        let x_blobs = collect_blobs(&cas, &tree_x).await;

        let dirty: Vec<(u64, Vec<u8>)> =
            [3u64, 70].iter().map(|&i| (i, pseudo(9000 + i, PAGE))).collect();
        let tree_f = update(&cas, &tree_x, dirty).await.unwrap();
        let f_blobs = collect_blobs(&cas, &tree_f).await;

        let expected: HashSet<Hash> = f_blobs.difference(&x_blobs).copied().collect();

        let holder = SetHolder { held: x_blobs };
        let d = diff_between(&cas, &holder, &tree_f).await.unwrap();
        let got: HashSet<Hash> = d.missing_blobs().into_iter().collect();
        assert_eq!(got, expected, "diff must equal blobs(F) \\ blobs(X)");
    }

    /// Applying the missing LEAVES at their `page_base` offsets, starting from
    /// X's image, must reproduce F's image — proving `Missing`'s placement.
    #[tokio::test]
    async fn diff_placement_round_trips() {
        let cas = MemCas::default();
        let pages = 100;
        let a = pseudo(7, pages * PAGE);
        let tree_x = build(&cas, &a).await.unwrap();
        let x_blobs = collect_blobs(&cas, &tree_x).await;

        let mut b = a.clone();
        let mut dirty: Vec<(u64, Vec<u8>)> = Vec::new();
        for &i in &[3usize, 70, 99] {
            let np = pseudo(9000 + i as u64, PAGE);
            b[i * PAGE..(i + 1) * PAGE].copy_from_slice(&np);
            dirty.push((i as u64, np));
        }
        let tree_f = update(&cas, &tree_x, dirty).await.unwrap();

        let holder = SetHolder { held: x_blobs };
        let d = diff_between(&cas, &holder, &tree_f).await.unwrap();

        let mut out = a.clone();
        for m in &d.missing {
            if m.level == 0 {
                let bytes = cas.get(&m.hash).await.unwrap();
                let off = m.page_base as usize * PAGE;
                out[off..off + bytes.len()].copy_from_slice(&bytes);
            }
        }
        assert_eq!(out, b, "applying missing leaves at page_base must reproduce F");
    }

    #[tokio::test]
    async fn diff_zero_len() {
        let cas = MemCas::default();
        let tree = build(&cas, &[]).await.unwrap();
        assert_eq!(tree.len, 0);
        assert!(diff(&cas, &tree).await.unwrap().missing.is_empty());
        let holder = SetHolder {
            held: HashSet::new(),
        };
        let d = diff_between(&cas, &holder, &tree).await.unwrap();
        assert_eq!(d.missing.len(), 1);
        assert_eq!(d.missing[0].hash, tree.root);
    }

    /// Compile-time `Send` check: the diff future must survive `tokio::spawn`,
    /// like the existing ingest/update spawnable tests.
    #[tokio::test]
    async fn diff_future_is_spawnable() {
        let cas = std::sync::Arc::new(MemCas::default());
        let tree = build(&*cas, &pseudo(1, 10 * PAGE)).await.unwrap();
        let handle = tokio::spawn(async move { diff(&*cas, &tree).await });
        let d = handle.await.unwrap().unwrap();
        assert!(d.missing.is_empty());
    }

    // ---- Phase 3: materialize planner ----

    /// A locator backed by a hash→location map; `Source` is a fake image id.
    struct MapLocate {
        map: HashMap<Hash, Located<u32>>,
    }
    impl Locate for MapLocate {
        type Source = u32;
        fn locate(&self, hash: &Hash) -> Option<Located<u32>> {
            self.map.get(hash).copied()
        }
    }

    /// Execute a plan into a fresh image buffer: `Clone` copies a run out of the
    /// named source image; `Gap` fetches the page by hash from `cas`. (Stands in
    /// for the host executor's FICLONERANGE / range-read.)
    async fn execute_plan<C: Cas>(
        plan: &[Op<u32>],
        sources: &HashMap<u32, Vec<u8>>,
        cas: &C,
        len: usize,
    ) -> Vec<u8> {
        let mut out = vec![0u8; len];
        for op in plan {
            match *op {
                Op::Clone {
                    src,
                    dest_page_base,
                    pages,
                } => {
                    let s = &sources[&src.source];
                    let src_off = src.src_page_base as usize * PAGE;
                    let dst_off = dest_page_base as usize * PAGE;
                    let n = (pages as usize * PAGE).min(len - dst_off);
                    out[dst_off..dst_off + n].copy_from_slice(&s[src_off..src_off + n]);
                }
                Op::Gap {
                    hash,
                    dest_page_base,
                    ..
                } => {
                    let bytes = cas.get(&hash).await.unwrap();
                    let off = dest_page_base as usize * PAGE;
                    out[off..off + bytes.len()].copy_from_slice(&bytes);
                }
            }
        }
        out
    }

    #[tokio::test]
    async fn plan_all_local_is_one_clone() {
        let cas = MemCas::default();
        let pages = 100;
        let tree = build(&cas, &pseudo(7, pages * PAGE)).await.unwrap();
        let map = HashMap::from([(
            tree.root,
            Located {
                source: 1u32,
                src_page_base: 0,
            },
        )]);
        let plan = plan_materialize(&cas, &tree, &MapLocate { map }).await.unwrap();
        assert_eq!(
            plan,
            vec![Op::Clone {
                src: Located {
                    source: 1,
                    src_page_base: 0
                },
                dest_page_base: 0,
                pages: 100,
            }]
        );
    }

    #[tokio::test]
    async fn plan_nothing_local_is_all_gaps() {
        let cas = MemCas::default();
        let pages = 100u64;
        let tree = build(&cas, &pseudo(7, pages as usize * PAGE)).await.unwrap();
        let loc = MapLocate { map: HashMap::new() };
        let plan = plan_materialize(&cas, &tree, &loc).await.unwrap();
        assert_eq!(plan.len(), pages as usize);
        let mut covered: Vec<u64> = plan
            .iter()
            .map(|op| match op {
                Op::Gap {
                    dest_page_base,
                    pages: 1,
                    ..
                } => *dest_page_base,
                _ => panic!("expected only single-page gaps, got {op:?}"),
            })
            .collect();
        covered.sort();
        assert_eq!(covered, (0..pages).collect::<Vec<_>>());
    }

    /// THE Phase-3 claim: a subtree available locally — even at a *different*
    /// offset in an unrelated (cross-lineage) source image — is cloned by hash,
    /// and the plan + a gap-fetch reconstruct F exactly.
    #[tokio::test]
    async fn plan_cross_offset_clone_round_trips() {
        let cas = MemCas::default();
        let pages = 100;
        let f_bytes = pseudo(7, pages * PAGE);
        let tree_f = build(&cas, &f_bytes).await.unwrap();

        // F's level-1 node #0 covers pages 0..64.
        let node0 = decode_node(&cas.get(&tree_f.root).await.unwrap()).unwrap()[0];

        // Unrelated source G holds F's pages 0..64 at G's pages 64..128.
        let mut g = pseudo(999, 200 * PAGE);
        g[64 * PAGE..128 * PAGE].copy_from_slice(&f_bytes[0..64 * PAGE]);

        let map = HashMap::from([(
            node0,
            Located {
                source: 7u32,
                src_page_base: 64,
            },
        )]);
        let plan = plan_materialize(&cas, &tree_f, &MapLocate { map })
            .await
            .unwrap();

        let clones: Vec<&Op<u32>> = plan.iter().filter(|op| matches!(op, Op::Clone { .. })).collect();
        assert_eq!(clones.len(), 1);
        assert_eq!(
            *clones[0],
            Op::Clone {
                src: Located {
                    source: 7,
                    src_page_base: 64
                },
                dest_page_base: 0,
                pages: 64,
            }
        );

        let sources = HashMap::from([(7u32, g)]);
        let out = execute_plan(&plan, &sources, &cas, f_bytes.len()).await;
        assert_eq!(out, f_bytes, "clone-from-G + gap-fetch must reconstruct F");
    }

    #[tokio::test]
    async fn plan_future_is_spawnable() {
        let cas = std::sync::Arc::new(MemCas::default());
        let tree = build(&*cas, &pseudo(1, 10 * PAGE)).await.unwrap();
        let loc = std::sync::Arc::new(MapLocate { map: HashMap::new() });
        let handle = tokio::spawn(async move { plan_materialize(&*cas, &tree, &*loc).await });
        let plan = handle.await.unwrap().unwrap();
        assert_eq!(plan.len(), 10);
    }

    #[tokio::test]
    async fn index_blobs_covers_tree_and_locates_pages() {
        let cas = MemCas::default();
        let pages = 100;
        let data = pseudo(7, pages * PAGE);
        let tree = build(&cas, &data).await.unwrap();
        let entries = index_blobs(&cas, &tree).await.unwrap();

        // Every distinct blob is present.
        let blobs: HashSet<Hash> = entries.iter().map(|&(h, _, _)| h).collect();
        assert_eq!(blobs, collect_blobs(&cas, &tree).await);

        // Each leaf's recorded page_base points at its actual bytes.
        for &(h, level, base) in &entries {
            if level == 0 {
                let bytes = cas.get(&h).await.unwrap();
                let off = base as usize * PAGE;
                assert_eq!(bytes, &data[off..off + PAGE]);
            }
        }
    }
}
