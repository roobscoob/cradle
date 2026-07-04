#!/usr/bin/env bash
# (Re)start the cradle host on this machine as a hardened *transient* systemd
# unit. Nothing persists: `systemctl stop cradle-host` or a reboot returns the
# machine to its prior state — no unit files, no system config.
#
# Expects the repo at ~/cradle (override: CRADLE_DIR) with a debug build at
# target/debug/host (`just build`) and the frame-store btrfs mount at
# ~/cradle-store (`just store-setup`).
#
# Containment, for a box that cannot be broken:
#   - ProtectSystem=strict: the entire fs is read-only to the unit except the
#     frame store and the nix daemon socket. The jailer chroots live inside
#     the frame store (FrameStore roots at TMPDIR), so VM writes land there.
#   - MemoryMax/TasksMax/CPUQuota: a runaway VM fork storm OOM-kills this
#     unit, not the machine's real services. Guest kernels are built by
#     nix-daemon (its own cgroup), so the cap doesn't throttle those builds.
#   - DevicePolicy=closed: /dev/kvm (+ userfaultfd) only, beyond the standard
#     pseudo-devices. Guests are vsock-only, so no tun/tap, no NIC access.
#   - CRADLE_BIND_ADDR pins the API to the tailnet IP: never LAN-visible.
set -euo pipefail

DIR="${CRADLE_DIR:-$HOME/cradle}"
STORE_MNT="${CRADLE_STORE_MNT:-/mnt/cradle}"
# The durable tier: frame ids are committed here before capture returns.
# On kokuzo, $HOME rides the tank pool — spinning rust, but commits are
# sequential pack writes sized by the dirty set, which tank handles fine.
CENTRAL="${CRADLE_CENTRAL_STORE:-$HOME/cradle-central}"
OUT_MNT="${CRADLE_JAIL_OUT:-/mnt/cradle-out}"
BIN="$DIR/target/debug/host"
UNIT=cradle-host

[ -x "$BIN" ] || { echo "no host binary at $BIN — run 'just build' first" >&2; exit 1; }
mountpoint -q "$STORE_MNT" || { echo "$STORE_MNT not mounted — run 'just store-setup' first" >&2; exit 1; }
mountpoint -q "$OUT_MNT" || { echo "$OUT_MNT not mounted — run 'just store-setup' first" >&2; exit 1; }
# Scratch from ops that died with a previous host (bind mounts died with
# that unit's private mount namespace; only the dirs linger).
rm -rf "${OUT_MNT:?}"/* 2>/dev/null || true

TS_IP="$(tailscale ip -4 | head -n1)"
[ -n "$TS_IP" ] || { echo "no tailscale IPv4 address?" >&2; exit 1; }

# NOT 8080: k3s's svclb-traefik claims hostPort 8080/8443 via a prerouting
# dnat that steals traffic to ANY address on those ports — a bind still
# succeeds, but packets never arrive.
PORT="${CRADLE_PORT:-8322}"

# Writable HOME for root's nix eval caches etc. — /root is read-only under
# ProtectSystem=strict, and nix hard-fails on some unwritable cache paths.
SCRATCH_HOME="$STORE_MNT/unit-home"
mkdir -p "$SCRATCH_HOME" "$CENTRAL"

sudo systemctl stop "$UNIT.service" 2>/dev/null || true

sudo systemd-run --unit="$UNIT" --collect --quiet \
  --working-directory="$DIR" \
  -p MemoryMax=24G -p MemorySwapMax=0 -p TasksMax=4096 -p CPUQuota=2000% \
  -p ProtectSystem=strict \
  -p ReadWritePaths="$STORE_MNT" \
  -p ReadWritePaths="$OUT_MNT" \
  -p ReadWritePaths="$CENTRAL" \
  -p ReadWritePaths=/nix/var/nix/daemon-socket \
  -p PrivateTmp=yes \
  -p DevicePolicy=closed \
  -p DeviceAllow="/dev/kvm rwm" \
  -p DeviceAllow="/dev/net/tun rwm" \
  -p DeviceAllow="/dev/userfaultfd rwm" \
  -p Delegate=yes \
  -E PATH=/run/current-system/sw/bin \
  -E HOME="$SCRATCH_HOME" \
  -E TMPDIR="$STORE_MNT" \
  -E CRADLE_BIND_ADDR="$TS_IP:$PORT" \
  -E CRADLE_CENTRAL_STORE="$CENTRAL" \
  -E CRADLE_JAIL_OUT="$OUT_MNT" \
  "$BIN"

echo ">> $UNIT started: http://$TS_IP:$PORT, frame store in $STORE_MNT"
echo ">> logs: journalctl -fu $UNIT   stop: sudo systemctl stop $UNIT"
