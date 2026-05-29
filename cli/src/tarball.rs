//! Tar+gzip a directory tree into an in-memory buffer suitable for multipart
//! upload. Cross-platform: tar uses POSIX path separators regardless of host.

use std::{io, path::Path};

use flate2::{Compression, write::GzEncoder};

/// Bundle `dir` into a `.tar.gz` byte buffer. The archive is rooted at `dir`
/// (paths inside are relative — e.g. `./flake.nix`, not the absolute host path).
pub fn pack_dir(dir: &Path) -> io::Result<Vec<u8>> {
    let buf: Vec<u8> = Vec::new();
    let gz = GzEncoder::new(buf, Compression::default());
    let mut tar = tar::Builder::new(gz);
    // Use "." as the in-archive root so the extracted tree lives at the
    // top level when untarred — the host's `extract_user_flake` accepts
    // either a flat layout or a single-child subdir, so this is fine.
    tar.append_dir_all(".", dir)?;
    let gz = tar.into_inner()?;
    gz.finish()
}
