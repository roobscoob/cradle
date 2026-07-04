# Cradle dev loop: edit on the Mac, build + run on the x86 KVM box.
#
# First-time setup on a fresh remote:
#   just sync store-setup build restart
# Daily loop:
#   just dev          # build + restart + tail logs
#   just cli          # TUI client against the remote host, from this machine

remote     := env_var_or_default("CRADLE_REMOTE", "rose@kokuzo")
remote_dir := env_var_or_default("CRADLE_REMOTE_DIR", "cradle")
# 8322, not 8080: k3s svclb-traefik steals hostPort 8080 on kokuzo.
api        := env_var_or_default("CRADLE_API", "http://kokuzo:8322")

_default:
    @just --list --unsorted

# One-shot mirror of the working tree to the remote checkout.
sync:
    rsync -az --delete \
      --exclude /target --exclude /.git --exclude /.direnv --exclude '/result*' \
      ./ {{remote}}:{{remote_dir}}/

# Continuous mirror: re-sync on every save.
watch:
    watchexec --watch . --ignore 'target/**' --ignore '.git/**' --ignore '.direnv/**' \
      --debounce 300ms --postpone -- just sync

# Incremental remote build of the host (first run populates the devShell).
# --features prod: kokuzo is bare-metal KVM, so the host passes CPUID through
# unchanged. The default (dev) template masks AVX-512 by editing CPUID leaf
# 7/subleaf 1, which doesn't exist on kokuzo's Haswell Xeon — KVM rejects it.
build: sync
    ssh {{remote}} "bash -lc 'cd {{remote_dir}} && nix develop --command cargo build -p host --features prod'"

# (Re)start the host as a hardened transient systemd unit. sudo prompts here.
restart: sync
    ssh -t {{remote}} "bash -lc '~/{{remote_dir}}/scripts/host-restart.sh'"

stop:
    ssh -t {{remote}} "bash -lc 'sudo systemctl stop cradle-host.service'"

status:
    ssh {{remote}} "bash -lc 'systemctl status cradle-host.service --no-pager'"

logs:
    ssh {{remote}} "bash -lc 'journalctl -fu cradle-host.service -n 100'"

# The inner loop.
dev: build restart
    @just logs

# Create + mount the btrfs loopback image backing the frame store (the only
# place cradle writes on the remote). sudo prompts for the mount.
store-setup size="100G": sync
    ssh -t {{remote}} "bash -lc '~/{{remote_dir}}/scripts/store-setup.sh {{size}}'"

# Unmount + delete the frame store image. The one-command undo.
store-teardown:
    ssh -t {{remote}} "bash -lc '~/{{remote_dir}}/scripts/store-teardown.sh'"

# Full nix build of the host package on the remote store (eval local, build
# remote, no copy-back).
remote-build:
    nix build .#packages.x86_64-linux.host \
      --eval-store auto --store ssh-ng://{{remote}} --no-link --print-out-paths -L

# Cross-check the Linux-only crates from the Mac (no linking, so no
# cross-linker needed). Dummy env vars satisfy the env!() consumers.
check:
    FIRECRACKER_BIN=/dev/null JAILER_BIN=/dev/null SNAPSHOT_EDITOR_BIN=/dev/null CRADLE_GUEST_FLAKE=/dev/null \
    nix shell --impure --expr 'let f = (builtins.getFlake "github:nix-community/fenix").packages.aarch64-darwin; in f.combine [ f.stable.cargo f.stable.rustc f.targets.x86_64-unknown-linux-gnu.stable.rust-std ]' \
      --command cargo check --target x86_64-unknown-linux-gnu -p host -p agent -p pty-bridge

# Run the TUI client locally against the remote host.
cli *ARGS:
    cargo run -p cli -- --host {{api}} {{ARGS}}
