//! Phase 3: materialize a frame's mem image from locally-held content.
//!
//! The page-trees of the images this machine already holds are indexed by
//! content hash → (which image, page offset). To reconstruct a target frame we
//! plan against that index ([`store::memtree::plan_materialize`]): every subtree
//! whose hash we hold is cloned by reflink — from *any* image, same lineage or
//! not — and stragglers are single-page copies. Anything not held locally is a
//! `Gap`, which is an error here (no network until P4).
//!
//! Not yet wired into the live restore path: on a single machine, capture
//! already leaves the image on disk, so nothing triggers a from-content
//! materialize until the cross-machine path (P4) exists. Exercised by the test
//! below — run `cargo test -p host` on a Linux box.
#![allow(dead_code)]

use std::collections::HashMap;
use std::io;
use std::os::unix::fs::FileExt;
use std::os::unix::io::AsRawFd;
use std::path::{Path, PathBuf};

use store::memtree::{self, Locate, Located, Op};
use store::{Cas, Hash, MemTree};

/// FICLONERANGE: reflink a byte range from one file into another.
/// `_IOW(0x94, 13, struct file_clone_range)`.
const FICLONERANGE: libc::c_ulong = 0x4020_940D;

#[repr(C)]
struct FileCloneRange {
    src_fd: i64,
    src_offset: u64,
    src_length: u64,
    dest_offset: u64,
}

/// A content index over the images this machine holds: hash → (image, page
/// offset within it). `Source` is a small `Copy` id — an index into `images`.
#[derive(Default)]
pub struct LocalIndex {
    images: Vec<PathBuf>,
    map: HashMap<Hash, Located<u32>>,
}

impl LocalIndex {
    /// Index every subtree/leaf of `image`'s tree so its content can be cloned
    /// by hash later. `cas` holds the tree's inner nodes.
    pub async fn add_image<C: Cas>(
        &mut self,
        cas: &C,
        image: PathBuf,
        tree: &MemTree,
    ) -> io::Result<()> {
        let source = self.images.len() as u32;
        for (hash, _level, page_base) in memtree::index_blobs(cas, tree).await? {
            // Last writer wins: any location holding this content is valid.
            self.map.insert(
                hash,
                Located {
                    source,
                    src_page_base: page_base,
                },
            );
        }
        self.images.push(image);
        Ok(())
    }
}

impl Locate for LocalIndex {
    type Source = u32;
    fn locate(&self, hash: &Hash) -> Option<Located<u32>> {
        self.map.get(hash).copied()
    }
}

/// Materialize `tree`'s image into `dest`, purely from locally-held content.
/// `nodes` supplies the tree's inner nodes (to walk). Errors if any page isn't
/// held locally (a `Gap` — needs the P4 fetch path). On success `dest` is a
/// complete `tree.len`-byte image, mostly assembled by reflink.
pub async fn materialize_local<C: Cas>(
    nodes: &C,
    tree: &MemTree,
    index: &LocalIndex,
    dest: &Path,
) -> io::Result<()> {
    let plan = memtree::plan_materialize(nodes, tree, index).await?;
    let images = index.images.clone();
    let dest = dest.to_path_buf();
    let len = tree.len;
    tokio::task::spawn_blocking(move || execute_plan(&plan, &images, &dest, len))
        .await
        .map_err(|e| io::Error::other(format!("materialize join: {e}")))?
}

fn execute_plan(plan: &[Op<u32>], images: &[PathBuf], dest: &Path, len: u64) -> io::Result<()> {
    let page = memtree::PAGE as u64;
    let dst = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest)?;
    dst.set_len(len)?; // sparse until written

    for op in plan {
        match *op {
            Op::Gap {
                dest_page_base,
                pages,
                ..
            } => {
                return Err(io::Error::other(format!(
                    "materialize gap at page {dest_page_base} (+{pages}) — not held locally; needs P4 fetch"
                )));
            }
            Op::Clone {
                src,
                dest_page_base,
                pages,
            } => {
                let src_file = std::fs::File::open(&images[src.source as usize])?;
                let src_off = src.src_page_base * page;
                let dst_off = dest_page_base * page;
                let mut bytes = pages * page;
                if dst_off + bytes > len {
                    bytes = len - dst_off; // clamp the final (possibly short) page
                }
                clone_range(&src_file, &dst, src_off, bytes, dst_off)?;
            }
        }
    }
    Ok(())
}

/// Reflink `len` bytes from `src`@`src_off` into `dst`@`dst_off` via
/// FICLONERANGE; fall back to a read+write copy when the fs can't reflink or the
/// length isn't page-aligned (a short final page). Correct on any fs — only the
/// speed differs.
fn clone_range(
    src: &std::fs::File,
    dst: &std::fs::File,
    src_off: u64,
    len: u64,
    dst_off: u64,
) -> io::Result<()> {
    let page = memtree::PAGE as u64;
    if len > 0 && len % page == 0 {
        let arg = FileCloneRange {
            src_fd: src.as_raw_fd() as i64,
            src_offset: src_off,
            src_length: len,
            dest_offset: dst_off,
        };
        let ret =
            unsafe { libc::ioctl(dst.as_raw_fd(), FICLONERANGE, &arg as *const FileCloneRange) };
        if ret == 0 {
            return Ok(());
        }
        // Otherwise fall through to a plain copy (unsupported fs, etc.).
    }
    let mut buf = vec![0u8; (1 << 20).min(len.max(1) as usize)];
    let mut done = 0u64;
    while done < len {
        let n = ((len - done) as usize).min(buf.len());
        src.read_exact_at(&mut buf[..n], src_off + done)?;
        dst.write_all_at(&buf[..n], dst_off + done)?;
        done += n as u64;
    }
    Ok(())
}

/// Materialize `tree`'s image into `dest` with the network in the loop:
/// locally-held content is cloned by reflink (via `index`), and every `Gap`
/// is batch-fetched from `central` — work.md §7's "Gap ops → filled by the
/// store's patch stream". Gap fetches run in bounded waves so a fully-cold
/// image (a seed restored on a fresh machine) never buffers more than
/// [`GAP_BATCH_PAGES`] pages in memory.
///
/// Returns `(cloned_pages, fetched_pages)`.
pub async fn materialize_fetch<C: Cas>(
    nodes: &C,
    tree: &MemTree,
    index: &LocalIndex,
    central: &dyn store::ContentStore,
    dest: &Path,
) -> io::Result<(u64, u64)> {
    /// 16k pages = 64 MiB per fetch wave.
    const GAP_BATCH_PAGES: usize = 16 * 1024;

    let page = memtree::PAGE as u64;
    let plan = memtree::plan_materialize(nodes, tree, index).await?;

    let mut clones: Vec<Op<u32>> = Vec::new();
    let mut gaps: Vec<(Hash, u64)> = Vec::new(); // (hash, byte offset)
    let mut cloned_pages = 0u64;
    for op in &plan {
        match *op {
            Op::Clone { pages, .. } => {
                cloned_pages += pages;
                clones.push(*op);
            }
            Op::Gap {
                hash,
                dest_page_base,
                ..
            } => gaps.push((hash, dest_page_base * page)),
        }
    }

    // Clones first — this also creates `dest` and sets its (sparse) length,
    // so the gap waves below only ever pwrite into an existing file.
    {
        let images = index.images.clone();
        let dest = dest.to_path_buf();
        let len = tree.len;
        tokio::task::spawn_blocking(move || execute_plan(&clones, &images, &dest, len))
            .await
            .map_err(|e| io::Error::other(format!("materialize clones join: {e}")))??;
    }

    let fetched_pages = gaps.len() as u64;
    for wave in gaps.chunks(GAP_BATCH_PAGES) {
        let hashes: Vec<Hash> = wave.iter().map(|&(h, _)| h).collect();
        let bytes = central.get_blobs(&hashes).await?;
        let writes: Vec<(u64, Vec<u8>)> = wave
            .iter()
            .map(|&(_, off)| off)
            .zip(bytes)
            .collect();
        let dest = dest.to_path_buf();
        tokio::task::spawn_blocking(move || -> io::Result<()> {
            let f = std::fs::OpenOptions::new().write(true).open(&dest)?;
            for (off, b) in &writes {
                f.write_all_at(b, *off)?;
            }
            Ok(())
        })
        .await
        .map_err(|e| io::Error::other(format!("gap write join: {e}")))??;
    }
    Ok((cloned_pages, fetched_pages))
}

/// Timing + hit/miss breakdown of a [`reconstruct`], for measurement.
#[derive(Debug)]
pub struct ReconStats {
    /// Walk the tree + classify (clone vs gap).
    pub plan_ms: u64,
    /// Read gap pages from the CAS — the local stand-in for the P4 network fetch.
    pub fetch_ms: u64,
    /// Clone located runs (reflink/copy) + write the fetched gap pages.
    pub exec_ms: u64,
    /// Pages cloned from a local image (reflink-by-content hit).
    pub cloned_pages: u64,
    /// Pages that had to be filled from the CAS (would be a network fetch).
    pub gap_pages: u64,
}

/// Reconstruct `tree`'s image into `dest` from `index` (reflink located
/// subtrees) plus `cas` (fill the rest — the single-machine stand-in for P4's
/// network fetch). Returns a timing + clone/gap breakdown for measurement.
pub async fn reconstruct<C: Cas>(
    cas: &C,
    tree: &MemTree,
    index: &LocalIndex,
    dest: &Path,
) -> io::Result<ReconStats> {
    let page = memtree::PAGE as u64;

    let t = std::time::Instant::now();
    let plan = memtree::plan_materialize(cas, tree, index).await?;
    let plan_ms = t.elapsed().as_millis() as u64;

    // Split the plan; fetch gap pages from the CAS (stand-in for the network).
    let t = std::time::Instant::now();
    let mut clones: Vec<Op<u32>> = Vec::new();
    let mut gaps: Vec<(u64, Vec<u8>)> = Vec::new();
    let mut cloned_pages = 0u64;
    for op in &plan {
        match *op {
            Op::Clone { pages, .. } => {
                cloned_pages += pages;
                clones.push(*op);
            }
            Op::Gap {
                hash,
                dest_page_base,
                ..
            } => {
                gaps.push((dest_page_base * page, cas.get(&hash).await?));
            }
        }
    }
    let gap_pages = gaps.len() as u64;
    let fetch_ms = t.elapsed().as_millis() as u64;

    let images = index.images.clone();
    let dest = dest.to_path_buf();
    let len = tree.len;
    let t = std::time::Instant::now();
    tokio::task::spawn_blocking(move || exec_reconstruct(&clones, &gaps, &images, &dest, len))
        .await
        .map_err(|e| io::Error::other(format!("reconstruct join: {e}")))??;
    let exec_ms = t.elapsed().as_millis() as u64;

    Ok(ReconStats {
        plan_ms,
        fetch_ms,
        exec_ms,
        cloned_pages,
        gap_pages,
    })
}

fn exec_reconstruct(
    clones: &[Op<u32>],
    gaps: &[(u64, Vec<u8>)],
    images: &[PathBuf],
    dest: &Path,
    len: u64,
) -> io::Result<()> {
    let page = memtree::PAGE as u64;
    let dst = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dest)?;
    dst.set_len(len)?;
    for op in clones {
        if let Op::Clone {
            src,
            dest_page_base,
            pages,
        } = *op
        {
            let src_file = std::fs::File::open(&images[src.source as usize])?;
            let src_off = src.src_page_base * page;
            let dst_off = dest_page_base * page;
            let mut bytes = pages * page;
            if dst_off + bytes > len {
                bytes = len - dst_off;
            }
            clone_range(&src_file, &dst, src_off, bytes, dst_off)?;
        }
    }
    for (off, bytes) in gaps {
        dst.write_all_at(bytes, *off)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use store::LocalCas;

    const PAGE: usize = memtree::PAGE;

    fn bytes(seed: u64, n: usize) -> Vec<u8> {
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

    /// Materialize a target image from two unrelated local source images, each
    /// holding one of its level-1 subtrees — proving cross-image (cross-lineage)
    /// reflink-by-content reconstructs the target exactly. (Falls back to copy
    /// on a non-reflink fs, so it verifies correctness anywhere.)
    #[tokio::test]
    async fn materialize_from_two_local_sources() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCas::new(dir.path().join("cas")).unwrap();

        // Target: 128 pages = two full level-1 subtrees (pages 0..64, 64..128).
        let target = bytes(7, 128 * PAGE);
        let tree_t = memtree::build(&cas, &target).await.unwrap();

        // Source A = target's first 64 pages, source B = its second 64 pages.
        // Built alone, each is a height-1 tree whose root == the matching
        // level-1 subtree of the target (content-addressed → same hash).
        let a = target[0..64 * PAGE].to_vec();
        let b = target[64 * PAGE..128 * PAGE].to_vec();
        let a_path = dir.path().join("a.img");
        let b_path = dir.path().join("b.img");
        std::fs::write(&a_path, &a).unwrap();
        std::fs::write(&b_path, &b).unwrap();
        let tree_a = memtree::build(&cas, &a).await.unwrap();
        let tree_b = memtree::build(&cas, &b).await.unwrap();

        let mut index = LocalIndex::default();
        index.add_image(&cas, a_path, &tree_a).await.unwrap();
        index.add_image(&cas, b_path, &tree_b).await.unwrap();

        let dest = dir.path().join("target.img");
        materialize_local(&cas, &tree_t, &index, &dest)
            .await
            .unwrap();

        assert_eq!(
            std::fs::read(&dest).unwrap(),
            target,
            "materialized image must equal the target bytes"
        );
    }

    /// A page held by no indexed source is a `Gap` → error (no network yet).
    #[tokio::test]
    async fn materialize_errors_on_gap() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCas::new(dir.path().join("cas")).unwrap();
        let target = bytes(7, 64 * PAGE);
        let tree_t = memtree::build(&cas, &target).await.unwrap();
        let index = LocalIndex::default(); // nothing local
        let dest = dir.path().join("target.img");
        assert!(
            materialize_local(&cas, &tree_t, &index, &dest)
                .await
                .is_err(),
            "materialize with no local content must error (gap)"
        );
    }
}
