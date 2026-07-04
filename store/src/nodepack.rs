//! Append-only node log — the scratch tier's blob store.
//!
//! Tree inner nodes are ~2 KiB blobs written ~50–200 at a time per capture.
//! Storing each as its own file (the old `LocalCas` role) cost a
//! create/tmp/rename ceremony per blob — ~400 VFS/CoW-metadata operations
//! per step, which was the entire `update_ms` cost. Here a `put` is one
//! `pwrite` into a single per-run segment file plus a map insert, and a
//! `get` is one `pread` — the page cache does all caching, and its pages
//! are evictable under guest memory pressure (the blade rule: RAM holds
//! O(index), storage holds O(history)).
//!
//! Every `get` re-hashes the bytes it read: a mismatch is `InvalidData`,
//! which callers treat as a miss. That property is what will let this store
//! become *persistent* without ever needing crash consistency — a torn
//! entry after a crash is just a cache miss, refetched from central. Until
//! local-tier persistence lands (work.md §11), the segment lives and dies
//! with the host run and the index lives only in RAM; the on-disk payload
//! (concatenated raw blobs) already matches the central pack format, so
//! persistence later adds an `.idx` sidecar, not a migration.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::cas::{Cas, Hash};

pub struct NodePack {
    file: std::fs::File,
    /// Next append offset. Reserved atomically so concurrent puts never
    /// overlap; a reserved-but-unindexed range is invisible to readers.
    offset: AtomicU64,
    index: Mutex<HashMap<Hash, (u64, u32)>>,
}

impl NodePack {
    /// Create a fresh segment under `dir` (per-run: name is unique per
    /// process start).
    pub fn create(dir: impl AsRef<Path>) -> io::Result<Self> {
        std::fs::create_dir_all(dir.as_ref())?;
        let ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0);
        let path: PathBuf = dir
            .as_ref()
            .join(format!("{ms:013}-{}.pack", std::process::id()));
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)?;
        Ok(Self {
            file,
            offset: AtomicU64::new(0),
            index: Mutex::new(HashMap::new()),
        })
    }
}

impl Cas for NodePack {
    async fn put(&self, bytes: &[u8]) -> io::Result<Hash> {
        use std::os::unix::fs::FileExt;
        let hash = Hash::of(bytes);
        if self.index.lock().unwrap().contains_key(&hash) {
            return Ok(hash);
        }
        // Two concurrent puts of the same bytes may both append; the log
        // wastes a duplicate 2 KiB and the index keeps whichever insert
        // lands last — both locations hold identical, valid bytes.
        let off = self.offset.fetch_add(bytes.len() as u64, Ordering::Relaxed);
        // Page-cache write, microseconds; not worth a blocking-pool hop.
        self.file.write_all_at(bytes, off)?;
        self.index
            .lock()
            .unwrap()
            .insert(hash, (off, bytes.len() as u32));
        Ok(hash)
    }

    async fn get(&self, hash: &Hash) -> io::Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let (off, len) = *self.index.lock().unwrap().get(hash).ok_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, format!("node {hash} not in pack"))
        })?;
        let mut buf = vec![0u8; len as usize];
        self.file.read_exact_at(&mut buf, off)?;
        if Hash::of(&buf) != *hash {
            // Torn/corrupt entry: report as data corruption; callers that
            // can refetch treat it as a miss.
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("node {hash} failed read-back verification"),
            ));
        }
        Ok(buf)
    }

    async fn has(&self, hash: &Hash) -> io::Result<bool> {
        Ok(self.index.lock().unwrap().contains_key(hash))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::FileExt;

    #[tokio::test]
    async fn roundtrip_dedup_and_missing() {
        let dir = tempfile::tempdir().unwrap();
        let pack = NodePack::create(dir.path()).unwrap();

        let h = pack.put(b"some node bytes").await.unwrap();
        assert_eq!(h, Hash::of(b"some node bytes"));
        assert!(pack.has(&h).await.unwrap());
        assert_eq!(pack.get(&h).await.unwrap(), b"some node bytes");

        // Idempotent put: no growth.
        let before = pack.offset.load(Ordering::Relaxed);
        pack.put(b"some node bytes").await.unwrap();
        assert_eq!(pack.offset.load(Ordering::Relaxed), before);

        let missing = Hash::of(b"never stored");
        assert!(!pack.has(&missing).await.unwrap());
        assert_eq!(
            pack.get(&missing).await.unwrap_err().kind(),
            io::ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn corruption_is_detected_on_read() {
        let dir = tempfile::tempdir().unwrap();
        let pack = NodePack::create(dir.path()).unwrap();
        let h = pack.put(b"pristine bytes").await.unwrap();

        // Scribble over the stored bytes through the same file.
        pack.file.write_all_at(b"garbage!", 0).unwrap();

        let err = pack.get(&h).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }
}
