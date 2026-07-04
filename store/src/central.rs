//! The central (durable) store seam.
//!
//! A frame id is a *durability promise*: once a capture returns it, any
//! machine may restore that frame. [`ContentStore`] is the narrow interface
//! that promise flows through — batch-first (a wire implementation must never
//! pay a round trip per 4 KiB blob) and split, like the data itself, into
//! big dumb bytes (blobs: pages, tree nodes, boot artifacts) and tiny hot
//! metadata ([`FrameRecord`]s).
//!
//! [`DirStore`] is the first backing: a plain directory (on kokuzo: a tank
//! dataset) holding append-only *pack files*. Packs, not per-blob files,
//! because durable per-blob storage costs one fsync per blob — the exact
//! disease the local tier was cured of — while a pack is one sequential
//! write and one fsync per *commit*. The real network daemon (work.md §7)
//! slots in behind the same trait later.
//!
//! THE CONTRACT: [`ContentStore::commit`] returning MEANS the frame is
//! durable — not applied, not probably, not eventually. A frame id is only
//! released after commit returns, so any machine may cash it at any later
//! time. The production ContentStore (the store daemon) honors this
//! absolutely; how cheaply is the receiver's business (journal device,
//! group commit), never the caller's.
//!
//! [`DirStore`] — the dev backing — DELIBERATELY VIOLATES the contract:
//! see its type-level doc. Structural safety still leans on a **hard ZFS
//! coupling**: ZFS is ordering-preserving per dataset (txgs commit atomic
//! *prefixes* of the operation stream), so "index visible ⇒ pack durable"
//! and "record visible ⇒ everything durable" hold with zero barriers, and a
//! crash leaves a clean prefix — orphan packs or blobs-without-record are
//! harmless reclaimable garbage; a visible record never references missing
//! bytes. On a freely-reordering filesystem (ext4/XFS) even that is UNSAFE.
//! Full rationale + the step-latency plan: work.md §11.

use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};

use crate::cas::Hash;
use crate::memtree::MemTree;

/// Boxed future so the trait stays object-safe — the host holds an
/// `Arc<dyn ContentStore>` and swaps backings by config.
pub type BoxFut<'a, T> = Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>;

/// Everything needed to restore a frame on a machine that has nothing:
/// the memory tree (pages resolved by hash) plus the whole-file boot
/// artifacts (content-addressed blobs in the same store).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FrameRecord {
    pub id: String,
    /// Parent frame id — lineage metadata (affinity, GC roots later).
    pub parent: Option<String>,
    pub mem_tree: MemTree,
    pub created_unix_ms: u64,
    pub kernel: Hash,
    pub initrd: Hash,
    pub store_disk: Hash,
    pub cmdline: Hash,
    pub snapshot: Hash,
}

/// Where a blob's bytes come from at commit time. `FileRange` lets a commit
/// stream gigabytes (a seed's pages, a store_disk) straight from the files
/// they already live in, instead of buffering them in memory.
#[derive(Debug, Clone)]
pub enum BlobSrc {
    Mem(Vec<u8>),
    FileRange {
        path: PathBuf,
        offset: u64,
        len: u64,
    },
}

pub trait ContentStore: Send + Sync {
    /// Which of `hashes` does the store lack? (The "have" half of the
    /// want/have exchange; commit only what this returns.)
    fn missing<'a>(&'a self, hashes: &'a [Hash]) -> BoxFut<'a, Vec<Hash>>;

    /// Single-blob presence check — the holder side of a tree diff walk.
    fn has<'a>(&'a self, hash: &'a Hash) -> BoxFut<'a, bool>;

    /// Store `blobs`, then (if given) the frame record. THE CONTRACT: when
    /// this returns, the frame is durable — this call is the durability
    /// event behind a frame id, and there is no weaker tier of ack.
    /// Already-present blobs are skipped, and the whole call is safe to
    /// blind-retry (content addressing makes replay a no-op) — networks
    /// lose acks after the work succeeded.
    ///
    /// (DirStore, the dev backing, knowingly breaks this — see its doc.)
    fn commit<'a>(
        &'a self,
        blobs: Vec<(Hash, BlobSrc)>,
        record: Option<&'a FrameRecord>,
    ) -> BoxFut<'a, ()>;

    /// Fetch small blobs (pages, tree nodes) by hash, batched.
    fn get_blobs<'a>(&'a self, hashes: &'a [Hash]) -> BoxFut<'a, Vec<Vec<u8>>>;

    /// Stream one large blob (a boot artifact) to a file. Returns its length.
    fn get_blob_to_file<'a>(&'a self, hash: &'a Hash, dest: &'a Path) -> BoxFut<'a, u64>;

    fn get_frame<'a>(&'a self, id: &'a str) -> BoxFut<'a, Option<FrameRecord>>;

    fn list_frames<'a>(&'a self) -> BoxFut<'a, Vec<String>>;
}

/// Where a blob lives: which pack (index into `packs`) and where inside it.
#[derive(Clone, Copy)]
struct BlobLoc {
    pack: u32,
    offset: u64,
    len: u32,
}

/// Filesystem-backed [`ContentStore`]: `<root>/packs/*.pack` (+ `.idx`) and
/// `<root>/frames/<id>.json`.
///
/// ⚠ DEV BACKING — DELIBERATELY VIOLATES THE DURABILITY CONTRACT. Nothing
/// here fsyncs: commit returns once the writes are in the page cache, and
/// durability arrives when ZFS's next txg commits (~5s). A machine crash
/// (power loss, kernel panic — NOT process death) revokes the newest ~5s of
/// acked frames. The loss is suffix-shaped (ZFS prefix ordering): a lineage
/// rolls back to an earlier tip, never gets holes. Accepted for the dev
/// loop; the production ContentStore honors commit ⇒ durable absolutely.
///
/// Multi-writer safe by append-only construction: pack names are unique per
/// (startup, pid, seq), records land by atomic rename. A process only *sees*
/// packs that existed when it opened the store — stale `missing` answers
/// cause harmless duplicate uploads, never data loss. (The network daemon
/// removes that window; this backing is for one-box setups and tests.)
pub struct DirStore {
    packs_dir: PathBuf,
    frames_dir: PathBuf,
    index: Mutex<HashMap<Hash, BlobLoc>>,
    packs: Mutex<Vec<PathBuf>>,
    /// Uniquifier for pack filenames from this process.
    seq: AtomicU64,
    /// Startup timestamp, part of the pack-name uniquifier.
    opened_ms: u64,
}

/// One pack-index entry, postcard-encoded as `Vec<IdxEntry>` in the `.idx`.
#[derive(Serialize, Deserialize)]
struct IdxEntry {
    hash: Hash,
    offset: u64,
    len: u32,
}

impl DirStore {
    pub fn open(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        let packs_dir = root.join("packs");
        let frames_dir = root.join("frames");
        std::fs::create_dir_all(&packs_dir)?;
        std::fs::create_dir_all(&frames_dir)?;

        let mut index = HashMap::new();
        let mut packs = Vec::new();
        // Sorted for determinism; duplicate content across packs resolves to
        // whichever loads last (any copy is valid — content-addressed).
        let mut idx_files: Vec<PathBuf> = std::fs::read_dir(&packs_dir)?
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().is_some_and(|x| x == "idx"))
            .collect();
        idx_files.sort();
        for idx_path in idx_files {
            let pack_path = idx_path.with_extension("pack");
            if !pack_path.exists() {
                continue; // torn commit: idx without pack shouldn't happen, skip
            }
            let entries: Vec<IdxEntry> = postcard::from_bytes(&std::fs::read(&idx_path)?)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            let pack_id = packs.len() as u32;
            packs.push(pack_path);
            for e in entries {
                index.insert(
                    e.hash,
                    BlobLoc {
                        pack: pack_id,
                        offset: e.offset,
                        len: e.len,
                    },
                );
            }
        }
        let opened_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        Ok(Self {
            packs_dir,
            frames_dir,
            index: Mutex::new(index),
            packs: Mutex::new(packs),
            seq: AtomicU64::new(0),
            opened_ms,
        })
    }

    fn frame_path(&self, id: &str) -> PathBuf {
        self.frames_dir.join(format!("{id}.json"))
    }

    fn locate(&self, hash: &Hash) -> Option<(PathBuf, BlobLoc)> {
        let loc = *self.index.lock().unwrap().get(hash)?;
        let path = self.packs.lock().unwrap()[loc.pack as usize].clone();
        Some((path, loc))
    }
}

/// Read a `BlobSrc`'s bytes in `chunk`-sized pieces, feeding each to `sink`.
/// Returns the blake3 of everything read, for verification against the
/// declared hash — a commit must never make corrupt bytes durable under a
/// hash they don't match.
fn drain_src(
    src: &BlobSrc,
    mut sink: impl FnMut(&[u8]) -> io::Result<()>,
) -> io::Result<(Hash, u64)> {
    let mut hasher = blake3::Hasher::new();
    let total = match src {
        BlobSrc::Mem(bytes) => {
            hasher.update(bytes);
            sink(bytes)?;
            bytes.len() as u64
        }
        BlobSrc::FileRange { path, offset, len } => {
            use std::os::unix::fs::FileExt;
            let f = std::fs::File::open(path)?;
            let mut buf = vec![0u8; (1 << 20).min(*len as usize).max(1)];
            let mut done = 0u64;
            while done < *len {
                let n = ((*len - done) as usize).min(buf.len());
                f.read_exact_at(&mut buf[..n], offset + done)?;
                hasher.update(&buf[..n]);
                sink(&buf[..n])?;
                done += n as u64;
            }
            *len
        }
    };
    Ok((Hash::from_bytes(*hasher.finalize().as_bytes()), total))
}

/// Atomically place `bytes` at `path` (same-dir tmp + rename). NO fsync:
/// ordering comes from ZFS (see the module doc); durability comes from the
/// commit's single terminal fsync.
fn place(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use std::io::Write;
    let tmp = path.with_extension("tmp");
    let mut f = std::fs::File::create(&tmp)?;
    f.write_all(bytes)?;
    drop(f);
    std::fs::rename(&tmp, path)?;
    Ok(())
}

impl ContentStore for DirStore {
    fn missing<'a>(&'a self, hashes: &'a [Hash]) -> BoxFut<'a, Vec<Hash>> {
        Box::pin(async move {
            let index = self.index.lock().unwrap();
            // Preserve order, drop duplicates: a caller uploading the result
            // must not write the same blob twice into one pack.
            let mut seen = std::collections::HashSet::new();
            Ok(hashes
                .iter()
                .filter(|h| !index.contains_key(h) && seen.insert(**h))
                .copied()
                .collect())
        })
    }

    fn has<'a>(&'a self, hash: &'a Hash) -> BoxFut<'a, bool> {
        Box::pin(async move { Ok(self.index.lock().unwrap().contains_key(hash)) })
    }

    fn commit<'a>(
        &'a self,
        blobs: Vec<(Hash, BlobSrc)>,
        record: Option<&'a FrameRecord>,
    ) -> BoxFut<'a, ()> {
        Box::pin(async move {
            // Skip blobs we already hold (idempotent replay / racing writers).
            let todo: Vec<(Hash, BlobSrc)> = {
                let index = self.index.lock().unwrap();
                blobs
                    .into_iter()
                    .filter(|(h, _)| !index.contains_key(h))
                    .collect()
            };

            if !todo.is_empty() {
                let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                let name = format!("{:013}-{}-{seq}", self.opened_ms, std::process::id());
                let pack_path = self.packs_dir.join(format!("{name}.pack"));
                let idx_path = self.packs_dir.join(format!("{name}.idx"));
                let pack_path_cl = pack_path.clone();

                // The pack build is sequential large I/O — blocking pool.
                // No syncs anywhere in here: ZFS ordering makes the
                // pack-before-idx sequence crash-safe, and the commit's
                // terminal fsync (below) makes the whole prefix durable.
                let entries = tokio::task::spawn_blocking(move || -> io::Result<Vec<IdxEntry>> {
                    use std::io::Write;
                    let tmp = pack_path_cl.with_extension("pack.tmp");
                    let mut f = std::io::BufWriter::new(std::fs::File::create(&tmp)?);
                    let mut entries = Vec::with_capacity(todo.len());
                    let mut offset = 0u64;
                    for (hash, src) in &todo {
                        let (actual, len) = drain_src(src, |chunk| {
                            f.write_all(chunk)
                        })?;
                        if actual != *hash {
                            return Err(io::Error::new(
                                io::ErrorKind::InvalidData,
                                format!("blob declared {hash} hashes to {actual} — refusing to commit corrupt bytes"),
                            ));
                        }
                        entries.push(IdxEntry {
                            hash: *hash,
                            offset,
                            len: len as u32,
                        });
                        offset += len;
                    }
                    let f = f.into_inner().map_err(|e| e.into_error())?;
                    drop(f);
                    std::fs::rename(&tmp, &pack_path_cl)?;
                    let idx_bytes = postcard::to_stdvec(&entries)
                        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                    place(&idx_path, &idx_bytes)?;
                    Ok(entries)
                })
                .await
                .map_err(|e| io::Error::other(format!("pack join: {e}")))??;

                let mut packs = self.packs.lock().unwrap();
                let pack_id = packs.len() as u32;
                packs.push(pack_path);
                let mut index = self.index.lock().unwrap();
                for e in entries {
                    index.insert(
                        e.hash,
                        BlobLoc {
                            pack: pack_id,
                            offset: e.offset,
                            len: e.len,
                        },
                    );
                }
            }

            if let Some(rec) = record {
                let path = self.frame_path(&rec.id);
                let bytes = serde_json::to_vec_pretty(rec)?;
                tokio::task::spawn_blocking(move || place(&path, &bytes))
                    .await
                    .map_err(|e| io::Error::other(format!("record join: {e}")))??;
            }

            // NO fsync — the deliberate contract violation (see type doc).
            // Durability rides the next ZFS txg (~5s); ordering alone keeps
            // every crash-visible state structurally sound.
            Ok(())
        })
    }

    fn get_blobs<'a>(&'a self, hashes: &'a [Hash]) -> BoxFut<'a, Vec<Vec<u8>>> {
        Box::pin(async move {
            let mut located = Vec::with_capacity(hashes.len());
            for h in hashes {
                let (path, loc) = self.locate(h).ok_or_else(|| {
                    io::Error::new(io::ErrorKind::NotFound, format!("blob {h} not in store"))
                })?;
                located.push((path, loc));
            }
            tokio::task::spawn_blocking(move || -> io::Result<Vec<Vec<u8>>> {
                use std::os::unix::fs::FileExt;
                // One open per distinct pack per call.
                let mut open: HashMap<PathBuf, std::fs::File> = HashMap::new();
                let mut out = Vec::with_capacity(located.len());
                for (path, loc) in located {
                    let f = match open.entry(path) {
                        std::collections::hash_map::Entry::Occupied(e) => e.into_mut(),
                        std::collections::hash_map::Entry::Vacant(e) => {
                            let f = std::fs::File::open(e.key())?;
                            e.insert(f)
                        }
                    };
                    let mut buf = vec![0u8; loc.len as usize];
                    f.read_exact_at(&mut buf, loc.offset)?;
                    out.push(buf);
                }
                Ok(out)
            })
            .await
            .map_err(|e| io::Error::other(format!("get_blobs join: {e}")))?
        })
    }

    fn get_blob_to_file<'a>(&'a self, hash: &'a Hash, dest: &'a Path) -> BoxFut<'a, u64> {
        Box::pin(async move {
            let (path, loc) = self.locate(hash).ok_or_else(|| {
                io::Error::new(io::ErrorKind::NotFound, format!("blob {hash} not in store"))
            })?;
            let dest = dest.to_path_buf();
            tokio::task::spawn_blocking(move || -> io::Result<u64> {
                use std::io::Write;
                use std::os::unix::fs::FileExt;
                let src = std::fs::File::open(&path)?;
                let mut out = std::io::BufWriter::new(std::fs::File::create(&dest)?);
                let mut buf = vec![0u8; 1 << 20];
                let mut done = 0u64;
                let len = loc.len as u64;
                while done < len {
                    let n = ((len - done) as usize).min(buf.len());
                    src.read_exact_at(&mut buf[..n], loc.offset + done)?;
                    out.write_all(&buf[..n])?;
                    done += n as u64;
                }
                let out = out.into_inner().map_err(|e| e.into_error())?;
                // Pay for our own writes: a fetched store_disk is ~1 GiB, and
                // left buffered it throttles the next writer on the dest
                // volume (measured: the first steps after a cold fetch paid
                // seconds of balance_dirty_pages stalls for it).
                out.sync_data()?;
                Ok(len)
            })
            .await
            .map_err(|e| io::Error::other(format!("get_blob_to_file join: {e}")))?
        })
    }

    fn get_frame<'a>(&'a self, id: &'a str) -> BoxFut<'a, Option<FrameRecord>> {
        Box::pin(async move {
            match tokio::fs::read(self.frame_path(id)).await {
                Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
                Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
                Err(e) => Err(e),
            }
        })
    }

    fn list_frames<'a>(&'a self) -> BoxFut<'a, Vec<String>> {
        Box::pin(async move {
            let mut out = Vec::new();
            let mut rd = tokio::fs::read_dir(&self.frames_dir).await?;
            while let Some(entry) = rd.next_entry().await? {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if let Some(id) = name.strip_suffix(".json") {
                    out.push(id.to_owned());
                }
            }
            out.sort();
            Ok(out)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: &[u8]) -> Hash {
        Hash::of(b)
    }

    #[tokio::test]
    async fn dirstore_commit_fetch_reopen_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("central");

        // A file to source a FileRange blob from (with an offset, like a page
        // inside a mem image).
        let file_path = dir.path().join("image");
        let mut file_bytes = vec![0u8; 8192];
        file_bytes[4096..8192].copy_from_slice(&[7u8; 4096]);
        std::fs::write(&file_path, &file_bytes).unwrap();
        let page = &file_bytes[4096..8192];

        let store = DirStore::open(&root).unwrap();
        let blobs = vec![
            (h(b"alpha"), BlobSrc::Mem(b"alpha".to_vec())),
            (
                h(page),
                BlobSrc::FileRange {
                    path: file_path.clone(),
                    offset: 4096,
                    len: 4096,
                },
            ),
        ];
        let all: Vec<Hash> = blobs.iter().map(|(h, _)| *h).collect();

        // Everything is missing before, nothing after.
        assert_eq!(store.missing(&all).await.unwrap(), all);
        let rec = FrameRecord {
            id: "frm_test".into(),
            parent: None,
            mem_tree: MemTree {
                root: h(b"root"),
                len: 4096,
            },
            created_unix_ms: 1,
            kernel: h(b"k"),
            initrd: h(b"i"),
            store_disk: h(b"s"),
            cmdline: h(b"c"),
            snapshot: h(b"n"),
        };
        store.commit(blobs, Some(&rec)).await.unwrap();
        assert!(store.missing(&all).await.unwrap().is_empty());

        // Fetch back, both ways.
        let got = store.get_blobs(&all).await.unwrap();
        assert_eq!(got[0], b"alpha");
        assert_eq!(got[1], page);
        let out = dir.path().join("fetched");
        let n = store.get_blob_to_file(&all[1], &out).await.unwrap();
        assert_eq!(n, 4096);
        assert_eq!(std::fs::read(&out).unwrap(), page);

        // Records visible + listable.
        let back = store.get_frame("frm_test").await.unwrap().unwrap();
        assert_eq!(back.mem_tree.root, rec.mem_tree.root);
        assert_eq!(store.list_frames().await.unwrap(), vec!["frm_test"]);
        assert!(store.get_frame("frm_nope").await.unwrap().is_none());

        // Reopen: index rebuilt from pack indexes on disk.
        drop(store);
        let store2 = DirStore::open(&root).unwrap();
        assert!(store2.missing(&all).await.unwrap().is_empty());
        assert_eq!(store2.get_blobs(&all).await.unwrap()[0], b"alpha");

        // Re-committing known blobs writes no new pack.
        let packs_before = std::fs::read_dir(root.join("packs")).unwrap().count();
        store2
            .commit(vec![(h(b"alpha"), BlobSrc::Mem(b"alpha".to_vec()))], None)
            .await
            .unwrap();
        let packs_after = std::fs::read_dir(root.join("packs")).unwrap().count();
        assert_eq!(packs_before, packs_after);
    }

    #[tokio::test]
    async fn commit_rejects_corrupt_bytes() {
        let dir = tempfile::tempdir().unwrap();
        let store = DirStore::open(dir.path().join("c")).unwrap();
        let wrong = vec![(h(b"declared"), BlobSrc::Mem(b"actual".to_vec()))];
        let err = store.commit(wrong, None).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        // Nothing became visible.
        assert_eq!(
            store.missing(&[h(b"declared")]).await.unwrap(),
            vec![h(b"declared")]
        );
    }

    #[test]
    fn frame_record_json_uses_hex_hashes() {
        let rec = FrameRecord {
            id: "frm_x".into(),
            parent: Some("frm_p".into()),
            mem_tree: MemTree {
                root: h(b"r"),
                len: 42,
            },
            created_unix_ms: 7,
            kernel: h(b"k"),
            initrd: h(b"i"),
            store_disk: h(b"s"),
            cmdline: h(b"c"),
            snapshot: h(b"n"),
        };
        let json = serde_json::to_string(&rec).unwrap();
        assert!(json.contains(&h(b"k").to_hex()), "hashes must be hex in JSON: {json}");
        let back: FrameRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(back.kernel, rec.kernel);
        // And postcard (non-human-readable) still roundtrips.
        let pc = postcard::to_stdvec(&rec.mem_tree).unwrap();
        let tree: MemTree = postcard::from_bytes(&pc).unwrap();
        assert_eq!(tree, rec.mem_tree);
    }
}
