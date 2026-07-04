#!/usr/bin/env bash
# Undo store-setup.sh: unmount the frame store and delete its backing image.
# After this (plus stopping cradle-host) the machine holds nothing of
# cradle's except the synced checkout and gc-able /nix store paths.
set -euo pipefail

IMG="${CRADLE_STORE_IMG:-/mnt/cradle-backing/cradle-store.img}"
MNT="${CRADLE_STORE_MNT:-/mnt/cradle}"
OUT_IMG="${CRADLE_OUT_IMG:-/mnt/cradle-backing/cradle-out.img}"
OUT_MNT="${CRADLE_JAIL_OUT:-/mnt/cradle-out}"

if systemctl is-active --quiet cradle-host.service; then
  echo "cradle-host is running — stop it first (just stop)" >&2
  exit 1
fi

if mountpoint -q "$MNT"; then
  sudo umount "$MNT"
fi
if mountpoint -q "$OUT_MNT"; then
  sudo umount "$OUT_MNT"
fi
rm -f "$IMG" "$OUT_IMG"
sudo rmdir "$MNT" "$OUT_MNT" 2>/dev/null || true
echo ">> frame store gone ($IMG + $OUT_IMG deleted)"
