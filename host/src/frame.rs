//! Frame: an immutable, self-contained VM checkpoint on disk.
//!
//! Each frame is a directory containing the six files needed to relaunch
//! a VM in the exact state it was captured:
//!
//! ```text
//! <store_root>/frames/<frame_id>/
//!   ├── kernel       ├── snapshot
//!   ├── initrd       ├── mem
//!   ├── store_disk
//!   └── cmdline
//! ```
//!
//! Frames are produced by `ops::build_frame` and `ops::step_frame` and
//! registered with `FrameStore`. They are never mutated. Stepping a frame
//! reads its files and writes a brand-new frame; the parent is untouched,
//! which is the entire "cloning" affordance.
//!
//! The runtime VM configuration (vcpu count, memory, drives, vsock) is
//! reconstructed from scratch on every restore in `ops` — the per-op
//! vsock UDS differs each time, so persisting the original config would
//! be wrong anyway. The on-disk frame is therefore self-contained.

use std::{
    collections::HashMap,
    fmt, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use store::{LocalCas, MemTree};
use tempfile::TempDir;
use tokio::sync::RwLock;
use ulid::Ulid;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct FrameId(String);

impl FrameId {
    fn generate() -> Self {
        FrameId(format!("frm_{}", Ulid::new()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for FrameId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<String> for FrameId {
    fn from(s: String) -> Self {
        FrameId(s)
    }
}

pub struct Frame {
    pub id: FrameId,
    pub dir: PathBuf,
    /// Content-addressed page-tree of this frame's memory image. The on-disk
    /// `mem` file is the materialized full image (what restore loads); this is
    /// the canonical tree used to ingest a child in O(dirty) by `update`-ing it.
    pub mem_tree: MemTree,
}

impl Frame {
    pub fn kernel(&self) -> PathBuf {
        self.dir.join("kernel")
    }
    pub fn initrd(&self) -> PathBuf {
        self.dir.join("initrd")
    }
    pub fn store_disk(&self) -> PathBuf {
        self.dir.join("store_disk")
    }
    pub fn cmdline(&self) -> PathBuf {
        self.dir.join("cmdline")
    }
    pub fn snapshot(&self) -> PathBuf {
        self.dir.join("snapshot")
    }
    pub fn mem(&self) -> PathBuf {
        self.dir.join("mem")
    }
}

pub struct FrameStore {
    root: TempDir,
    frames: RwLock<HashMap<FrameId, Arc<Frame>>>,
    /// Content-addressed blob store under <root>/cas. Used (shadow, for now) to
    /// ingest captured mem images into the page-tree; will become the canonical
    /// frame storage as the build steps land.
    cas: LocalCas,
}

impl FrameStore {
    pub fn new() -> io::Result<Arc<Self>> {
        let root = tempfile::Builder::new().prefix("cradle-frames-").tempdir()?;
        std::fs::create_dir_all(root.path().join("frames"))?;
        // Per-VM serial captures land in <root>/serial/<jail_id>.log so we
        // always have the full guest serial transcript on disk regardless
        // of SSE consumers or tracing filters.
        std::fs::create_dir_all(root.path().join("serial"))?;
        let cas = LocalCas::new(root.path().join("cas"))?;
        // Log the backing path so we can confirm the CAS lives on a fast Linux
        // fs (e.g. /tmp) and not the slow /mnt/c 9p mount — the latter would
        // dominate ingest time regardless of concurrency.
        tracing::info!("frame store root: {}", root.path().display());
        Ok(Arc::new(Self {
            root,
            frames: RwLock::new(HashMap::new()),
            cas,
        }))
    }

    pub fn root(&self) -> &Path {
        self.root.path()
    }

    pub fn cas(&self) -> &LocalCas {
        &self.cas
    }

    /// Reserve a fresh frame id and create its empty directory. Callers
    /// populate the six expected files, then `finalize`.
    pub fn allocate(&self) -> io::Result<(FrameId, PathBuf)> {
        let id = FrameId::generate();
        let dir = self.root.path().join("frames").join(id.as_str());
        std::fs::create_dir_all(&dir)?;
        Ok((id, dir))
    }

    pub async fn finalize(&self, id: FrameId, dir: PathBuf, mem_tree: MemTree) -> Arc<Frame> {
        let frame = Arc::new(Frame {
            id: id.clone(),
            dir,
            mem_tree,
        });
        self.frames.write().await.insert(id, Arc::clone(&frame));
        frame
    }

    pub async fn get(&self, id: &FrameId) -> Option<Arc<Frame>> {
        self.frames.read().await.get(id).cloned()
    }

    pub async fn ids(&self) -> Vec<FrameId> {
        self.frames.read().await.keys().cloned().collect()
    }
}
