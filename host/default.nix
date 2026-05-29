{
  lib,
  rustPlatform,
  firecracker,
  fetchurl,
}:

let
  # See the matching block in agent/default.nix for the rationale.
  uaFetchurl = fetchurl // {
    __functor = _self: args: fetchurl (args // {
      curlOptsList = (args.curlOptsList or []) ++ [
        "--user-agent" "cradle-build (https://github.com/braid)"
      ];
    });
  };
  uaImportCargoLock = rustPlatform.importCargoLock.override {
    fetchurl = uaFetchurl;
  };
  uaBuildRustPackage = rustPlatform.buildRustPackage.override {
    importCargoLock = uaImportCargoLock;
  };
in

uaBuildRustPackage {
  pname = "host";
  version = "0.1.0";

  src = lib.cleanSourceWith {
    src = ../.;
    filter =
      path: type:
      let
        base = baseNameOf (toString path);
      in
      base != "target" && base != "result" && lib.cleanSourceFilter path type;
  };

  cargoLock.lockFile = ../Cargo.lock;

  cargoBuildFlags = [ "-p" "host" ];
  cargoTestFlags = [ "-p" "host" ];

  FIRECRACKER_BIN = "${firecracker}/bin/firecracker";
  JAILER_BIN = "${firecracker}/bin/jailer";
  SNAPSHOT_EDITOR_BIN = "${firecracker}/bin/snapshot-editor";
  CRADLE_GUEST_FLAKE = ../.;

  meta = {
    description = "Firecracker VM orchestrator";
    mainProgram = "host";
    platforms = lib.platforms.linux;
  };
}
