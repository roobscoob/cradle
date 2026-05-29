# Base NixOS module imported by every nixosSystem that produces cradle
# artifacts — both the server's `guest` config and any per-request
# user-flake wrapper. Sets the firecracker invariants the host's microVM
# machinery relies on AND bakes the cradle-agent into the initrd, with
# `mkForce` on every required option so a user's own module can't
# accidentally remove them.
#
# `cradleAgent` is passed via `specialArgs` from the wrapping flake.
# It's a path (or derivation reference) to the statically-linked agent
# whose binary will be copied into the initrd at /bin/agent.
{ pkgs, lib, cradleAgent, cradlePtyBridge, ... }:
{
  microvm.hypervisor = "firecracker";

  # Reasonable defaults; user can still override in their own module.
  system.stateVersion = "24.11";
  networking.hostName = lib.mkDefault "cradle-guest";
  services.getty.autologinUser = lib.mkDefault "root";
  # Passwordless root for the dev guest (console autologin + anything that
  # checks the shadow field). `hashedPassword = ""` yields `root::…` in
  # /etc/shadow (a literal empty field), vs `password = ""` which would
  # hash the empty string into a real `$6$…` entry.
  users.users.root.hashedPassword = lib.mkDefault "";

  # `pty-bridge` is the in-guest binary the CLI runs as the interactive
  # step command: `pty-bridge --rows R --cols C -- <cmd>`. It allocates
  # the PTY the user's command runs under, bridging the PTY to its own
  # stdin/stdout (the agent's pipes) with a tiny framing for keystrokes
  # and window-size updates. cradle itself stays pipes-only — all the
  # terminal semantics live here. Installed into systemPackages so it's
  # reachable at `/run/current-system/sw/bin/pty-bridge`.
  environment.systemPackages = [ cradlePtyBridge ];

  # ---------------------------------------------------------------------------
  # Initrd.
  # ---------------------------------------------------------------------------
  # Use the legacy bash stage-1 init. It `exec`s into stage-2 (user's
  # systemd) keeping PID 1.
  #
  # Note: the agent does NOT run from initrd. NixOS's stage-1 ends with a
  # `kill -9` sweep of every non-kernel process whose cmdline doesn't
  # start with `@` (the freedesktop storage-daemon marker — see
  # <https://www.freedesktop.org/wiki/Software/systemd/RootStorageDaemons/>).
  # We could prefix with `@` to dodge it, but running the agent as a
  # plain systemd service in stage-2 is simpler and avoids ever depending
  # on NixOS-stage1-internal behavior. systemd then gives us Restart,
  # cgroup delegation, journald, and a normal lifecycle for free.
  boot.initrd.systemd.enable = lib.mkForce false;

  # Make sure the kernel modules the agent needs are loadable in stage-2.
  # `availableKernelModules` is a list and merges with the user's list, so
  # no mkForce — user can add more modules without clobbering ours.
  boot.initrd.availableKernelModules = [
    "vhost_vsock"
    "vmw_vsock_virtio_transport"
    "virtio_blk"
    "virtio_pci"
  ];

  # ---------------------------------------------------------------------------
  # cradle-agent.service — runs the agent under systemd.
  # ---------------------------------------------------------------------------
  # mkForce on the whole unit so a user's nixosModules.guest can't
  # accidentally turn it off (or override it into something useless).
  #
  # Delegate=yes gives the service its own cgroup v2 subtree at
  # `/sys/fs/cgroup/system.slice/cradle-agent.service/`, which the agent
  # then creates per-eval child cgroups under (see agent/src/cgroup.rs).
  # Without delegation, systemd would reap any cgroup we created outside
  # its tree.
  # `after = multi-user.target` makes the agent wait for the rest of
  # userspace to come up before binding its vsock listener. That way, a
  # successful vsock handshake from the host implies multi-user.target
  # was reached — a strictly stronger signal than the old serial sentinel,
  # and one we can observe over the only channel we actually use (vsock).
  systemd.services.cradle-agent = lib.mkForce {
    description = "cradle agent — runs guest commands on the host's behalf";
    wantedBy = [ "multi-user.target" ];
    after = [ "multi-user.target" ];
    serviceConfig = {
      Type = "simple";
      ExecStart = "${cradleAgent}/bin/agent";
      Restart = "always";
      RestartSec = "1s";
      Delegate = "yes";
    };
  };
}
