//! Per-request wrapper-flake synthesis.
//!
//! `/frames/build` with a `flake` upload extracts the user's tarball, then
//! we synthesize a thin wrapper flake on disk that:
//!
//! - declares the user's extracted directory as a `path:` flake input;
//! - imports the user's `nixosModules.guest` together with our embedded
//!   [`BASE_MODULE`] (firecracker invariants, the agent baked into the
//!   initrd, etc.);
//! - threads the host-resolved `cradleAgent` path through `specialArgs` so
//!   the base module knows which binary to drop into the initrd;
//! - produces all four boot artifacts (kernel + initrd + storeDisk + cmdline)
//!   from one coherent nixosSystem.
//!
//! The wrapper-flake directory lives inside the per-request `FrameStore`
//! tempdir, so it's cleaned up automatically when the store drops.

use std::{
    io,
    path::{Path, PathBuf},
};

use tokio::sync::mpsc;

use crate::nix_build::run_nix_build;

/// The NixOS module embedded into every wrapper flake.
const BASE_MODULE: &str = include_str!("../base-module.nix");

/// Boot bundle for one frame: paths to the four pieces firecracker needs.
/// Always emitted as a unit because microvm.nix's `kernelParams` encode
/// `init=/nix/store/<this-system>/init` — kernel, initrd, storeDisk, and
/// cmdline must all come from the same nixosSystem.
#[derive(Debug, Clone)]
pub struct UserArtifacts {
    pub kernel: PathBuf,
    pub initrd: PathBuf,
    pub store_disk: PathBuf,
    pub cmdline: PathBuf,
}

/// Build the four-artifact bundle from a user-uploaded flake.
///
/// `user_flake_dir` is the extracted tarball root (must contain `flake.nix`
/// and expose `nixosModules.guest`). `out_dir` is a per-request scratch dir
/// that the wrapper flake will be materialized in. `agent_static` is the
/// already-built static agent — its path is substituted into the wrapper's
/// `specialArgs.cradleAgent` so the base module can reference it.
///
/// Streams nix progress lines on `progress` (same format as
/// [`crate::nix_build::build_server_artifacts`]).
pub async fn build_user_artifacts(
    user_flake_dir: &Path,
    out_dir: &Path,
    agent_static: &Path,
    pty_bridge_static: &Path,
    progress: Option<mpsc::Sender<String>>,
) -> io::Result<UserArtifacts> {
    let wrapper_dir = out_dir.join("wrapper");
    tokio::fs::create_dir_all(&wrapper_dir).await?;

    let user_path = user_flake_dir
        .canonicalize()?
        .to_string_lossy()
        .into_owned();
    let agent_path = agent_static.to_string_lossy().into_owned();
    let bridge_path = pty_bridge_static.to_string_lossy().into_owned();
    let flake_nix = wrapper_flake_source(&user_path, &agent_path, &bridge_path);
    tokio::fs::write(wrapper_dir.join("flake.nix"), flake_nix).await?;
    tokio::fs::write(wrapper_dir.join("base.nix"), BASE_MODULE).await?;

    let wrapper = wrapper_dir.to_string_lossy();
    let attrs = [
        format!("{wrapper}#packages.x86_64-linux.kernel"),
        format!("{wrapper}#packages.x86_64-linux.initrd"),
        format!("{wrapper}#packages.x86_64-linux.storeDisk"),
        format!("{wrapper}#packages.x86_64-linux.cmdline"),
    ];
    let paths = run_nix_build(&attrs, progress).await?;
    Ok(UserArtifacts {
        kernel: paths[0].clone(),
        initrd: paths[1].clone(),
        store_disk: paths[2].clone(),
        cmdline: paths[3].clone(),
    })
}

/// Wrapper `flake.nix` text. Kept as a Rust format string so the dynamic
/// fields (user path + agent path + pty-bridge path) stay obvious and
/// shell-escape-free.
fn wrapper_flake_source(user_path: &str, agent_path: &str, bridge_path: &str) -> String {
    format!(
        r#"{{
  description = "cradle user-flake wrapper (synthesized per request)";

  inputs.nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  inputs.microvm.url = "github:astro/microvm.nix";
  inputs.microvm.inputs.nixpkgs.follows = "nixpkgs";
  # Same workaround as the top-level flake: microvm.nix references a spectrum
  # input whose pinned rev no longer has a flake.nix.
  inputs.microvm.inputs.spectrum.follows = "nixpkgs";
  inputs.user.url = "path:{user_path}";

  outputs = {{ self, nixpkgs, microvm, user, ... }}:
    let
      sys = nixpkgs.lib.nixosSystem {{
        system = "x86_64-linux";
        # `cradleAgent` / `cradlePtyBridge` are absolute /nix/store paths to
        # the host's pre-built static binaries. base.nix uses cradleAgent to
        # populate the initrd, and installs cradlePtyBridge into the guest's
        # systemPackages for interactive steps.
        specialArgs = {{
          cradleAgent = {agent_path};
          cradlePtyBridge = {bridge_path};
        }};
        modules = [
          microvm.nixosModules.microvm
          ./base.nix
          user.nixosModules.guest
        ];
      }};
      pkgs = nixpkgs.legacyPackages.x86_64-linux;
    in {{
      # All four artifacts MUST come from this same nixosSystem — microvm.nix
      # bakes `init=/nix/store/<this-system>/init` into kernelParams.
      packages.x86_64-linux.kernel = pkgs.runCommand "vmlinux" {{ }} ''
        cp ${{sys.config.microvm.kernel.dev}}/vmlinux $out
      '';
      packages.x86_64-linux.initrd = pkgs.runCommand "initrd" {{ }} ''
        cp ${{sys.config.microvm.initrdPath}} $out
      '';
      packages.x86_64-linux.storeDisk = sys.config.microvm.storeDisk;
      packages.x86_64-linux.cmdline = pkgs.writeText "cmdline" (
        nixpkgs.lib.concatStringsSep " " (
          sys.config.microvm.kernelParams ++ [ "console=ttyS0" ]
        )
      );
    }};
}}
"#
    )
}
