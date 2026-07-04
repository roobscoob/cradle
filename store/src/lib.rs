//! Content-addressed store for VM frames.
//!
//! Two layers, both pure logic with no Firecracker/KVM dependency (so this
//! crate is cross-platform and unit-testable on any OS):
//!
//! - [`cas`]: a flat content-addressed blob store. Every blob is named by its
//!   `blake3` hash, so writing the same bytes twice is a no-op and identical
//!   content across frames dedups by construction.
//!
//! - [`memtree`]: a page-granular Merkle radix tree over a VM memory image.
//!   Leaves are 4 KiB pages (stored as plain CAS blobs); inner nodes are lists
//!   of child hashes (stored as `postcard`-encoded CAS blobs). Stepping a frame
//!   rewrites only the path from root to the dirtied leaves, so a child frame
//!   shares every untouched subtree with its parent by hash.
//!
//! The crate is generic over the [`cas::Cas`] backend, so the same tree logic
//! drives a local filesystem store today and a remote object store later.

pub mod cas;
pub mod central;
pub mod memtree;
pub mod nodepack;

pub use cas::{Cas, Hash, LocalCas};
pub use central::{BlobSrc, ContentStore, DirStore, FrameRecord};
pub use memtree::MemTree;
pub use nodepack::NodePack;
