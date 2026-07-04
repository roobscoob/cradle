{ pkgs, lib, hostPackage, ... }:
{
  system.stateVersion = "24.11";

  nix.settings.experimental-features = [ "nix-command" "flakes" ];

  boot.kernelModules = [ "kvm" "kvm-intel" "kvm-amd" ];

  networking.hostName = "cradle";
  # Static IP on the LAN via macvtap (configured at the QEMU level). The
  # macvtap0 interface on the WSL2 host bridges eth2 (192.168.0.0/16, gateway
  # 192.168.1.1), so this guest is a first-class citizen on that network.
  networking.useDHCP = false;
  networking.interfaces.eth0.ipv4.addresses = [{
    address = "192.168.0.42";
    prefixLength = 16;
  }];
  networking.defaultGateway = "192.168.1.1";
  networking.nameservers = [ "1.1.1.1" "8.8.8.8" ];

  # The host binds 0.0.0.0:8080 (host/src/main.rs BIND_ADDR) for remote CLI
  # clients on the LAN; NixOS's firewall is on by default, so the port must
  # be opened explicitly or only localhost can reach the API.
  networking.firewall.allowedTCPPorts = [ 8080 ];

  services.getty.autologinUser = "root";
  users.users.root.password = "";

  environment.systemPackages = [
    hostPackage
    pkgs.nix
    pkgs.git
  ];

  # programs.bash.loginShellInit = ''
  #   if [ "$(tty)" = "/dev/ttyS0" ]; then
  #     exec ${lib.getExe hostPackage}
  #   fi
  # '';
}
