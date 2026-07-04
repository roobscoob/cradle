#!/usr/bin/env bash
# Create + mount the btrfs loopback image that backs the frame store.
# Everything cradle writes on this machine lives inside this one file: the
# image is the disk fence (it can never grow past its size) and the teardown
# unit (scripts/store-teardown.sh: umount + rm).
#
# Why btrfs-on-loopback: the frame store needs FICLONERANGE (reflink) for
# O(dirty) captures and SEEK_HOLE fidelity (ops::probe_store_fs refuses fs
# that can't report holes). A loopback btrfs provides both regardless of the
# filesystem underneath. Same pattern as recon-test.sh.
set -euo pipefail

SIZE="${1:-${CRADLE_STORE_SIZE:-100G}}"
# Default backing: /mnt/cradle-backing (on kokuzo: the apps SSD pool's
# cradle dataset). Override with CRADLE_STORE_IMG — e.g. /dev/shm/... for a
# RAM-backed control run when isolating storage-device cost.
IMG="${CRADLE_STORE_IMG:-/mnt/cradle-backing/cradle-store.img}"
MNT="${CRADLE_STORE_MNT:-/mnt/cradle}"
# Jail-output scratch: a PLAIN fs (ext4) for firecracker's snapshot outputs.
# Sparse 4KiB diff writes pay CoW extent churn on btrfs; ext4 takes them
# cheaply and reports holes honestly (read_dirty_pages requires that).
# Not tmpfs: a diff is worst-case guest-sized, and blade RAM is spoken for.
OUT_SIZE="${CRADLE_OUT_SIZE:-8G}"
OUT_IMG="${CRADLE_OUT_IMG:-/mnt/cradle-backing/cradle-out.img}"
OUT_MNT="${CRADLE_JAIL_OUT:-/mnt/cradle-out}"

if ! mountpoint -q "$MNT"; then
  if [ ! -f "$IMG" ]; then
    echo ">> creating btrfs image $IMG ($SIZE)"
    truncate -s "$SIZE" "$IMG"
    nix shell nixpkgs#btrfs-progs -c mkfs.btrfs -q "$IMG"
  fi
  sudo mkdir -p "$MNT"
  sudo mount -o loop "$IMG" "$MNT"
  sudo chown "$(id -u):$(id -g)" "$MNT"
fi
echo ">> $MNT mounted ($(findmnt -no FSTYPE "$MNT"), max $SIZE)"

if ! mountpoint -q "$OUT_MNT"; then
  if [ ! -f "$OUT_IMG" ]; then
    echo ">> creating ext4 jail-out image $OUT_IMG ($OUT_SIZE)"
    truncate -s "$OUT_SIZE" "$OUT_IMG"
    nix shell nixpkgs#e2fsprogs -c mkfs.ext4 -q "$OUT_IMG"
  fi
  sudo mkdir -p "$OUT_MNT"
  sudo mount -o loop "$OUT_IMG" "$OUT_MNT"
  sudo chown "$(id -u):$(id -g)" "$OUT_MNT"
fi
echo ">> $OUT_MNT mounted ($(findmnt -no FSTYPE "$OUT_MNT"), max $OUT_SIZE)"
