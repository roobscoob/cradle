{
  lib,
  rustPlatform,
  fetchurl,
}:

let
  # See the matching block in agent/default.nix for the rationale (crates.io
  # rejects bare-curl-UA API fetches; nixpkgs' importCargoLock hits that
  # endpoint, so we thread a UA-setting fetchurl through it).
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
  pname = "pty-bridge";
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

  cargoBuildFlags = [ "-p" "pty-bridge" ];
  cargoTestFlags = [ "-p" "pty-bridge" ];

  meta = {
    description = "In-guest PTY bridge for braid cradle interactive sessions";
    mainProgram = "pty-bridge";
    platforms = lib.platforms.linux;
  };
}
