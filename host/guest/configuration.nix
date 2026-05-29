{ ... }:
{
  # Pull in the shared cradle base (cradle-ready marker service, firecracker
  # hypervisor pin, autologin/empty-root defaults). Both the default
  # storeDisk and per-request user-flake builds use the same base.
  imports = [ ../base-module.nix ];

  # Demo-specific overrides — kept minimal so this stays close to "what a
  # user would get if they uploaded a no-op flake".
  networking.hostName = "cradle-demo";
}
