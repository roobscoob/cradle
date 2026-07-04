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

if mountpoint -q "$MNT"; then
  echo ">> $MNT already mounted ($(findmnt -no FSTYPE "$MNT"))"
  exit 0
fi

if [ ! -f "$IMG" ]; then
  echo ">> creating btrfs image $IMG ($SIZE)"
  truncate -s "$SIZE" "$IMG"
  nix shell nixpkgs#btrfs-progs -c mkfs.btrfs -q "$IMG"
fi

sudo mkdir -p "$MNT"
sudo mount -o loop "$IMG" "$MNT"
sudo chown "$(id -u):$(id -g)" "$MNT"
echo ">> $MNT mounted ($(findmnt -no FSTYPE "$MNT"), max $SIZE)"
