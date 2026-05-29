#!/usr/bin/env bash
# Run cradle's host with the frame store on a reflink-capable (btrfs) filesystem
# and the P3 reconstruct measurement enabled. Run from the repo root.
#
# Why btrfs-on-loopback: /tmp is tmpfs, which can't reflink (FICLONE), so the
# capture path falls back to a full copy and there's nothing to measure. btrfs
# supports reflink even when its backing file lives on tmpfs/ext4 — the clone
# happens inside the btrfs fs.
#
# Override defaults via env: CRADLE_IMG / CRADLE_MNT / CRADLE_SIZE.
#
# Teardown when done:  sudo umount "$CRADLE_MNT" && rm -f "$CRADLE_IMG"
set -euo pipefail

IMG="${CRADLE_IMG:-/var/tmp/cradle-btrfs.img}"   # loopback image (kept between runs)
MNT="${CRADLE_MNT:-/mnt/cradle}"                 # mountpoint -> frame store lives here
SIZE="${CRADLE_SIZE:-32G}"

# 1. Ensure a btrfs filesystem is mounted at $MNT.
if ! mountpoint -q "$MNT"; then
  if [ ! -f "$IMG" ]; then
    echo ">> creating btrfs image $IMG ($SIZE)"
    truncate -s "$SIZE" "$IMG"
    nix shell nixpkgs#btrfs-progs -c mkfs.btrfs -q "$IMG"   # self-contained mkfs
  fi
  echo ">> mounting $IMG at $MNT"
  sudo mkdir -p "$MNT"
  sudo mount -o loop "$IMG" "$MNT"
fi
echo ">> $MNT is $(findmnt -no FSTYPE "$MNT") (want: btrfs)"

# 2. Launch the host: frame store on btrfs (TMPDIR), reconstruct measurement on.
#    The inline VARs pass through sudo into the host process's environment.
echo ">> launching host (TMPDIR=$MNT, CRADLE_RECONSTRUCT_TEST=1)"
# sudo CRADLE_RECONSTRUCT_TEST=1 TMPDIR="$MNT" nix run .#host
sudo TMPDIR="$MNT" nix run .#host
