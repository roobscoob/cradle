{
  lib,
  rustPlatform,
  fetchurl,
}:

let
  # crates.io's data-access policy (https://crates.io/data-access) rejects
  # API-endpoint requests that arrive with bare `curl/<v>` as the User-Agent
  # — and nixpkgs's `importCargoLock` builds its download URLs against
  # exactly that endpoint with no UA set, so crate fetches return HTTP 403.
  # We wrap `fetchurl` to append a real User-Agent and thread it through
  # `importCargoLock`'s callPackage args (via `.override`), then thread the
  # resulting importCargoLock through `buildRustPackage`'s callPackage args
  # the same way. The wrapper is scoped to THIS build's crate fetches only;
  # nothing else in the package set sees it.
  #
  # The override is purely additive — we don't change the URL, only the
  # curl flags — so the fixed-output hashes in Cargo.lock still match.
  uaFetchurl = fetchurl // {
    __functor = _self: args: fetchurl (args // {
      curlOptsList = (args.curlOptsList or []) ++ [
        "--user-agent" "cradle-build (https://github.com/roobscoob/cradle)"
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
  pname = "agent";
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

  cargoBuildFlags = [ "-p" "agent" ];
  cargoTestFlags = [ "-p" "agent" ];

  meta = {
    description = "In-VM agent for braid cradle";
    mainProgram = "agent";
    platforms = lib.platforms.linux;
  };
}
