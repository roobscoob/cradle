//! Content-addressed blob storage.
//!
//! A [`Hash`] is the `blake3` digest of some bytes, and *is* the blob's name.
//! The [`Cas`] trait is the storage interface: `put` is idempotent (re-storing
//! known bytes is a no-op), so dedup falls out for free. [`LocalCas`] is a
//! filesystem backend; a remote (object-store) backend slots in behind the same
//! trait later.

use std::fmt;
use std::future::Future;
use std::io;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

/// A 256-bit `blake3` content hash. This is the address of a blob.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, std::hash::Hash, Serialize, Deserialize)]
pub struct Hash([u8; 32]);

impl Hash {
    /// Hash some bytes. `Hash::of(b) == Hash::of(b)` always, which is what makes
    /// the store content-addressed.
    pub fn of(bytes: &[u8]) -> Hash {
        Hash(blake3::hash(bytes).into())
    }

    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }

    /// Lowercase hex, 64 chars. Used for on-disk paths and logging.
    pub fn to_hex(&self) -> String {
        let mut s = String::with_capacity(64);
        for b in self.0 {
            s.push(hex_digit(b >> 4));
            s.push(hex_digit(b & 0xf));
        }
        s
    }
}

fn hex_digit(nibble: u8) -> char {
    match nibble {
        0..=9 => (b'0' + nibble) as char,
        _ => (b'a' + (nibble - 10)) as char,
    }
}

impl fmt::Display for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.to_hex())
    }
}

impl fmt::Debug for Hash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Short prefix keeps tree dumps readable.
        write!(f, "Hash({}…)", &self.to_hex()[..12])
    }
}

/// A content-addressed blob store.
///
/// Implementors must guarantee: `get(put(b)) == b`, `put` is idempotent, and
/// the hash returned by `put(b)` equals `Hash::of(b)`.
///
/// The returned futures are `Send` and the trait is `Send + Sync` so callers can
/// hold a `&Cas` across `.await` inside `tokio::spawn`ed tasks (the host spawns
/// the whole capture/restore op). Implementing this with a plain `async fn` is
/// fine as long as the resulting future is `Send`.
pub trait Cas: Send + Sync {
    /// Store `bytes`, returning their hash. Idempotent: storing already-present
    /// bytes does no write.
    fn put(&self, bytes: &[u8]) -> impl Future<Output = io::Result<Hash>> + Send;

    /// Fetch the bytes for `hash`. Errors with `NotFound` if absent.
    fn get(&self, hash: &Hash) -> impl Future<Output = io::Result<Vec<u8>>> + Send;

    /// Whether `hash` is present, without fetching its bytes.
    fn has(&self, hash: &Hash) -> impl Future<Output = io::Result<bool>> + Send;
}

/// Filesystem-backed CAS. Blobs live at `<root>/<aa>/<bb>/<full-hex>`, a
/// two-level fan-out so no single directory holds millions of entries.
pub struct LocalCas {
    root: PathBuf,
}

impl LocalCas {
    pub fn new(root: impl Into<PathBuf>) -> io::Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path(&self, hash: &Hash) -> PathBuf {
        let hex = hash.to_hex();
        self.root.join(&hex[0..2]).join(&hex[2..4]).join(&hex)
    }
}

/// Process-local counter that makes temp filenames unique so concurrent `put`s
/// of different blobs never collide on the same scratch path.
static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

/// Removes the temp file on drop unless disarmed — covers write errors,
/// rename errors, and a caller cancelling the `put` future mid-write (all of
/// which would otherwise strand a `.tmp.*` file in the shard dir forever).
struct TmpGuard(Option<PathBuf>);

impl TmpGuard {
    fn disarm(&mut self) {
        self.0 = None;
    }
}

impl Drop for TmpGuard {
    fn drop(&mut self) {
        if let Some(p) = self.0.take() {
            let _ = std::fs::remove_file(&p);
        }
    }
}

impl Cas for LocalCas {
    async fn put(&self, bytes: &[u8]) -> io::Result<Hash> {
        let hash = Hash::of(bytes);
        let path = self.path(&hash);
        if tokio::fs::try_exists(&path).await? {
            return Ok(hash);
        }
        let dir = path
            .parent()
            .expect("blob path always has a parent directory");
        tokio::fs::create_dir_all(dir).await?;

        // Write to a unique temp file, then rename into place so a reader never
        // observes a half-written blob. fsync before the rename: on many
        // filesystems a rename can be persisted before the data it points at,
        // so a crash could otherwise leave a durable-but-empty blob that `has`
        // reports present and `get` serves corrupt — permanently, since a CAS
        // never re-fetches a hash it believes it holds.
        let seq = TMP_SEQ.fetch_add(1, Ordering::Relaxed);
        let tmp = dir.join(format!(".tmp.{}.{seq}", std::process::id()));
        let mut guard = TmpGuard(Some(tmp.clone()));
        let mut f = tokio::fs::File::create(&tmp).await?;
        f.write_all(bytes).await?;
        f.sync_data().await?;
        drop(f);
        match tokio::fs::rename(&tmp, &path).await {
            Ok(()) => guard.disarm(),
            // A concurrent put of the *same* bytes may have won the race; the
            // content is identical, so just drop our temp (the guard removes
            // it). (rename-over-existing also fails on Windows, so this branch
            // is the normal dedup path there.)
            Err(_) if tokio::fs::try_exists(&path).await.unwrap_or(false) => {}
            Err(e) => return Err(e),
        }
        Ok(hash)
    }

    async fn get(&self, hash: &Hash) -> io::Result<Vec<u8>> {
        tokio::fs::read(self.path(hash)).await
    }

    async fn has(&self, hash: &Hash) -> io::Result<bool> {
        tokio::fs::try_exists(self.path(hash)).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn local_put_get_has_dedup() {
        let dir = tempfile::tempdir().unwrap();
        let cas = LocalCas::new(dir.path()).unwrap();

        let h = cas.put(b"some bytes").await.unwrap();
        assert_eq!(h, Hash::of(b"some bytes"));
        assert!(cas.has(&h).await.unwrap());
        assert_eq!(cas.get(&h).await.unwrap(), b"some bytes");

        // Idempotent put: same hash, no error.
        let h2 = cas.put(b"some bytes").await.unwrap();
        assert_eq!(h, h2);

        // Unknown hash is absent.
        let missing = Hash::of(b"never stored");
        assert!(!cas.has(&missing).await.unwrap());
        assert!(cas.get(&missing).await.is_err());
    }

    #[test]
    fn hex_roundtrips_shape() {
        let h = Hash::of(b"abc");
        let hex = h.to_hex();
        assert_eq!(hex.len(), 64);
        assert!(hex.bytes().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn hash_matches_known_blake3_vectors() {
        // Official BLAKE3 test vectors — confirms we hash the right algorithm
        // AND that to_hex encodes correctly.
        assert_eq!(
            Hash::of(b"").to_hex(),
            "af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"
        );
        assert_eq!(
            Hash::of(b"abc").to_hex(),
            "6437b3ac38465133ffb63b75273a8db548c558465d79db03fd359c6cd5bd9d85"
        );
    }
}
