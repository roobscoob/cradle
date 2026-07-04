//! Frame: an immutable, self-contained VM checkpoint.
//!
//! Locally, each frame is a directory containing the six files needed to
//! relaunch a VM in the exact state it was captured:
//!
//! ```text
//! <store_root>/frames/<frame_id>/
//!   ├── kernel       ├── snapshot
//!   ├── initrd       ├── mem
//!   ├── store_disk
//!   └── cmdline
//! ```
//!
//! But the local tier is a *cache*: the durable form of a frame is its
//! [`store::FrameRecord`] in the central store — a memory tree plus five
//! artifact hashes, every byte resolvable by content. A frame id returned by
//! capture is a durability promise (the commit happened before the id was
//! handed out), so [`FrameStore::get_or_fetch`] can reconstruct any frame
//! this machine has never seen: artifacts stream out of the central store,
//! the memory image materializes by reflinking whatever content already
//! exists in local frames and fetching only the gaps.
//!
//! The runtime VM configuration (vcpu count, memory, drives, vsock) is
//! reconstructed from scratch on every restore in `ops` — the per-op
//! vsock UDS differs each time, so persisting the original config would
//! be wrong anyway.

use std::{
    collections::HashMap,
    fmt, io,
    path::{Path, PathBuf},
    sync::Arc,
};

use serde::{Deserialize, Serialize};
use store::{ContentStore, FrameRecord, Hash, MemTree, NodePack, memtree};
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

/// Content hashes of the five non-memory frame files. Children inherit
/// kernel/initrd/store_disk/cmdline from their parent (the files are
/// byte-identical down a lineage), so only the fresh snapshot is rehashed
/// per step.
#[derive(Debug, Clone, Copy)]
pub struct ArtifactHashes {
    pub kernel: Hash,
    pub initrd: Hash,
    pub store_disk: Hash,
    pub cmdline: Hash,
    pub snapshot: Hash,
}

pub struct Frame {
    pub id: FrameId,
    pub dir: PathBuf,
    /// Content-addressed page-tree of this frame's memory image. The on-disk
    /// `mem` file is the materialized full image (what restore loads); this is
    /// the canonical tree used to ingest a child in O(dirty) by `update`-ing it.
    pub mem_tree: MemTree,
    pub artifacts: ArtifactHashes,
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
    /// Scratch store for merkle *inner nodes* only (append-only log +
    /// in-RAM index) — page bytes live in the frames' mem images and are
    /// located by content via the tree.
    cas: NodePack,
    /// The durable tier. Every returned frame id has already been committed
    /// here; every unknown id is looked up here.
    central: Arc<dyn ContentStore>,
    /// Serializes cold fetches so two concurrent steps of the same unknown
    /// frame don't both materialize it.
    fetch_lock: tokio::sync::Mutex<()>,
}

impl FrameStore {
    pub fn new(central: Arc<dyn ContentStore>) -> io::Result<Arc<Self>> {
        let root = tempfile::Builder::new().prefix("cradle-frames-").tempdir()?;
        std::fs::create_dir_all(root.path().join("frames"))?;
        // Per-VM serial captures land in <root>/serial/<jail_id>.log so we
        // always have the full guest serial transcript on disk regardless
        // of SSE consumers or tracing filters.
        std::fs::create_dir_all(root.path().join("serial"))?;
        let cas = NodePack::create(root.path().join("nodes"))?;
        tracing::info!("frame store root: {}", root.path().display());
        Ok(Arc::new(Self {
            root,
            frames: RwLock::new(HashMap::new()),
            cas,
            central,
            fetch_lock: tokio::sync::Mutex::new(()),
        }))
    }

    pub fn root(&self) -> &Path {
        self.root.path()
    }

    pub fn cas(&self) -> &NodePack {
        &self.cas
    }

    pub fn central(&self) -> &Arc<dyn ContentStore> {
        &self.central
    }

    /// Reserve a fresh frame id and create its empty directory. Callers
    /// populate the six expected files, then `finalize`.
    pub fn allocate(&self) -> io::Result<(FrameId, PathBuf)> {
        let id = FrameId::generate();
        let dir = self.frame_dir(&id);
        std::fs::create_dir_all(&dir)?;
        Ok((id, dir))
    }

    fn frame_dir(&self, id: &FrameId) -> PathBuf {
        self.root.path().join("frames").join(id.as_str())
    }

    pub async fn finalize(
        &self,
        id: FrameId,
        dir: PathBuf,
        mem_tree: MemTree,
        artifacts: ArtifactHashes,
    ) -> Arc<Frame> {
        let frame = Arc::new(Frame {
            id: id.clone(),
            dir,
            mem_tree,
            artifacts,
        });
        self.frames.write().await.insert(id, Arc::clone(&frame));
        frame
    }

    pub async fn get(&self, id: &FrameId) -> Option<Arc<Frame>> {
        self.frames.read().await.get(id).cloned()
    }

    /// Every frame this host can serve: local ∪ central.
    pub async fn ids(&self) -> io::Result<Vec<FrameId>> {
        let mut set: std::collections::BTreeSet<String> = self
            .frames
            .read()
            .await
            .keys()
            .map(|k| k.as_str().to_owned())
            .collect();
        set.extend(self.central.list_frames().await?);
        Ok(set.into_iter().map(FrameId).collect())
    }

    /// Local hit, or reconstruct the frame from the central store: artifacts
    /// stream to files, tree nodes fetch into the node CAS, and the memory
    /// image materializes from local content (reflink) + fetched gaps.
    /// `Ok(None)` means the central store has never heard of the id either.
    pub async fn get_or_fetch(self: &Arc<Self>, id: &FrameId) -> io::Result<Option<Arc<Frame>>> {
        if let Some(f) = self.get(id).await {
            return Ok(Some(f));
        }
        let _guard = self.fetch_lock.lock().await;
        // Lost the race to another fetch of the same id?
        if let Some(f) = self.get(id).await {
            return Ok(Some(f));
        }
        let Some(rec) = self.central.get_frame(id.as_str()).await? else {
            return Ok(None);
        };

        let t0 = std::time::Instant::now();
        let dir = self.frame_dir(id);
        std::fs::create_dir_all(&dir)?;
        // Unreachable until inserted into the map — clean up on any error.
        let guard = FetchDirGuard(Some(dir.clone()));

        for (hash, name) in [
            (rec.kernel, "kernel"),
            (rec.initrd, "initrd"),
            (rec.store_disk, "store_disk"),
            (rec.cmdline, "cmdline"),
            (rec.snapshot, "snapshot"),
        ] {
            self.central.get_blob_to_file(&hash, &dir.join(name)).await?;
        }

        memtree::fetch_nodes(&self.cas, self.central.as_ref(), &rec.mem_tree).await?;

        // Index every local frame's image so the materialize clones whatever
        // content this machine already holds (the parent lineage, typically)
        // and fetches only the genuinely-missing pages.
        let mut index = crate::materialize::LocalIndex::default();
        for frame in self.frames.read().await.values() {
            index
                .add_image(&self.cas, frame.mem(), &frame.mem_tree)
                .await?;
        }
        let (cloned, fetched) = crate::materialize::materialize_fetch(
            &self.cas,
            &rec.mem_tree,
            &index,
            self.central.as_ref(),
            &dir.join("mem"),
        )
        .await?;
        tracing::info!(
            frame = %id, cloned_pages = cloned, fetched_pages = fetched,
            elapsed_ms = t0.elapsed().as_millis() as u64,
            "cold fetch: frame materialized from central store"
        );

        let artifacts = ArtifactHashes {
            kernel: rec.kernel,
            initrd: rec.initrd,
            store_disk: rec.store_disk,
            cmdline: rec.cmdline,
            snapshot: rec.snapshot,
        };
        let frame = self
            .finalize(id.clone(), dir, rec.mem_tree, artifacts)
            .await;
        std::mem::forget(guard); // dir now owned by the registered frame
        Ok(Some(frame))
    }

    /// Build the durable record for a frame (the shape the central store
    /// keeps). `parent` is lineage metadata.
    pub fn record(frame_id: &FrameId, parent: Option<&FrameId>, mem_tree: &MemTree, artifacts: &ArtifactHashes) -> FrameRecord {
        FrameRecord {
            id: frame_id.as_str().to_owned(),
            parent: parent.map(|p| p.as_str().to_owned()),
            mem_tree: *mem_tree,
            created_unix_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0),
            kernel: artifacts.kernel,
            initrd: artifacts.initrd,
            store_disk: artifacts.store_disk,
            cmdline: artifacts.cmdline,
            snapshot: artifacts.snapshot,
        }
    }
}

/// Removes a half-fetched frame directory on drop (fetch failed mid-way).
struct FetchDirGuard(Option<PathBuf>);

impl Drop for FetchDirGuard {
    fn drop(&mut self) {
        if let Some(dir) = self.0.take() {
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}
