{
  description = "braid cradle — Firecracker VM orchestrator";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    microvm.url = "github:astro/microvm.nix";
    microvm.inputs.nixpkgs.follows = "nixpkgs";
    # HACK: spectrum-os.org no longer hosts a flake.nix at the rev microvm.nix
    # pins, so any evaluation that touches the spectrum input dies with a
    # misleading "source/flake.nix does not exist" error. Point it at nixpkgs
    # as a no-op stand-in — microvm doesn't dereference spectrum on the
    # firecracker path. If microvm ever does, eval will fail with a missing
    # attribute and we'll need a real fix (working spectrum rev or older
    # microvm pin).
    microvm.inputs.spectrum.follows = "nixpkgs";
  };

  outputs =
    { self, nixpkgs, flake-utils, microvm }:
    let
      guestSystem = "x86_64-linux";
      guestPkgs = nixpkgs.legacyPackages.${guestSystem};

      # The "default" guest — also the template for user-uploaded flakes:
      # imports our base module (which forces the agent into the initrd) plus
      # host/guest/configuration.nix (the demo userspace).
      #
      # User-flake builds use the same shape via a per-request wrapper that
      # imports `base-module.nix` + the user's `nixosModules.guest`. The
      # kernel + initrd + storeDisk + cmdline come from a single coherent
      # nixosSystem, eliminating cross-config mount-layout coupling.
      guest = nixpkgs.lib.nixosSystem {
        system = guestSystem;
        specialArgs = {
          inherit self;
          cradleAgent = self.packages.${guestSystem}.agent-static;
          cradlePtyBridge = self.packages.${guestSystem}.pty-bridge-static;
        };
        modules = [
          microvm.nixosModules.microvm
          ./host/base-module.nix
          ./host/guest/configuration.nix
        ];
      };

      perSystem = flake-utils.lib.eachSystem [ "x86_64-linux" "aarch64-linux" ] (
        system:
        let
          pkgs = nixpkgs.legacyPackages.${system};

          # Statically-linked firecracker (musl libc) so it execs cleanly
          # inside the jailer's chroot — the chroot has no /nix/store, so a
          # dynamically-linked firecracker would fail to find its ELF
          # interpreter and any libc.
          firecracker = pkgs.pkgsStatic.firecracker;

          host = pkgs.callPackage ./host { inherit firecracker; };
          agent = pkgs.callPackage ./agent { };
          # Statically-linked agent for embedding in the server-controlled
          # initrd. The initrd's bash stage-1 backgrounds this binary right
          # before switch_root; the running process needs no /nix/store
          # access from the user's rootfs once it's loaded into memory.
          agent-static = pkgs.pkgsStatic.callPackage ./agent { };
          # Statically-linked PTY bridge baked into the guest image. The CLI
          # runs it as the interactive step command (it allocates the PTY
          # the user's command runs under). Static + musl so it doesn't
          # depend on anything in the guest's /nix/store layout.
          pty-bridge = pkgs.callPackage ./pty-bridge { };
          pty-bridge-static = pkgs.pkgsStatic.callPackage ./pty-bridge { };
        in
        {
          packages = {
            inherit host agent agent-static pty-bridge pty-bridge-static;
            default = host;
          };

          devShells.default = pkgs.mkShell {
            packages = [
              pkgs.cargo
              pkgs.rustc
              pkgs.rust-analyzer
              firecracker
              pkgs.nix
            ];

            FIRECRACKER_BIN = "${firecracker}/bin/firecracker";
            JAILER_BIN = "${firecracker}/bin/jailer";
            SNAPSHOT_EDITOR_BIN = "${firecracker}/bin/snapshot-editor";

            shellHook = ''
              export CRADLE_GUEST_FLAKE="$PWD"
            '';
          };
        }
      );

      cradle = nixpkgs.lib.nixosSystem {
        system = "x86_64-linux";
        modules = [
          ./host/nixos/cradle.nix
          { _module.args.hostPackage = self.packages.x86_64-linux.host; }
        ];
      };
    in
    nixpkgs.lib.recursiveUpdate perSystem {
      nixosConfigurations = {
        cradle = cradle;
        guest = guest;
      };

      packages.${guestSystem} = {
        # All four artifacts from the same nixosSystem (`guest`). They MUST
        # come from one config — microvm.nix's kernelParams encode an
        # absolute `init=/nix/store/<this-system>/init` that's only valid
        # inside this system's closure. User-uploaded flakes produce their
        # own bundle via the per-request wrapper in host/src/user_flake.rs.
        default-kernel = guestPkgs.runCommand "vmlinux" { } ''
          cp ${guest.config.microvm.kernel.dev}/vmlinux $out
        '';
        default-initrd = guestPkgs.runCommand "initrd" { } ''
          cp ${guest.config.microvm.initrdPath} $out
        '';
        default-storeDisk = guest.config.microvm.storeDisk;
        default-cmdline = guestPkgs.writeText "cmdline" (
          nixpkgs.lib.concatStringsSep " " (
            guest.config.microvm.kernelParams ++ [ "console=ttyS0" ]
          )
        );

        cradle-vm =
          (cradle.extendModules {
            modules = [
              ({ lib, ... }: {
                virtualisation.vmVariant.virtualisation = {
                  memorySize = 4096;
                  cores = 4;
                  diskSize = 16384; # 16GB
                  graphics = false;
                  # Persist the /nix/store overlay to the qcow2 disk instead of
                  # tmpfs, so anything nix builds inside the cradle VM survives
                  # restart and we don't re-fetch nixpkgs / rebuild the kernel
                  # every run. The disk file lives next to the run script in CWD.
                  writableStoreUseTmpfs = false;
                  # macvtap-fd networking. The launcher (`packages.cradle-vm-run`)
                  # opens /dev/tap<ifindex of macvtap0> as fd 3 before exec'ing
                  # qemu. The MAC here MUST match macvtap0's MAC on the WSL2
                  # host (configured in WSL2's /etc/nixos/configuration.nix),
                  # otherwise the macvtap link-layer filter silently drops
                  # frames in both directions.
                  #
                  # mkForce is essential: nixos's qemu-vm.nix unconditionally
                  # appends a default `-net nic,...user.0...` SLIRP pair to
                  # networkingOptions, which would otherwise leave us with two
                  # NICs (and the static IP bound to the wrong one).
                  qemu.networkingOptions = lib.mkForce [
                    "-netdev tap,fd=3,id=net0"
                    "-device virtio-net-pci,netdev=net0,mac=52:54:00:12:34:42"
                  ];
                };
              })
            ];
          }).config.system.build.vm;

        # Launcher: opens /dev/tap<ifindex of macvtap0> as fd 3, then exec's
        # the cradle-vm runscript. QEMU inherits the fd and references it via
        # `-netdev tap,fd=3`. Run with `nix run .#cradle-vm-run`.
        cradle-vm-run = guestPkgs.writeShellApplication {
          name = "cradle-vm-run";
          runtimeInputs = with guestPkgs; [ coreutils ];
          text = ''
            if [ ! -e /sys/class/net/macvtap0/ifindex ]; then
              echo "macvtap0 not found. Is the cradle-vm-network systemd service running on the WSL2 host?" >&2
              exit 1
            fi
            idx=$(cat /sys/class/net/macvtap0/ifindex)
            dev=/dev/tap"$idx"
            if [ ! -w "$dev" ]; then
              echo "$dev is not writable by $(id -un). Check the chown line in the cradle-vm-network service." >&2
              exit 1
            fi
            exec 3<>"$dev"
            exec ${self.packages.${guestSystem}.cradle-vm}/bin/run-cradle-vm "$@"
          '';
        };
      };
    };
}
