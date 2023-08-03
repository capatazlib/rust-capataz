{ self, ... } @ inputs: pkgs:

let
  inherit (pkgs.lib) mkForce;
  rustToolchain = self.packages.${pkgs.system}.toolchain;
in

{
  tools = {
    rustfmt = mkForce rustToolchain;
    cargo-check = mkForce rustToolchain;
    clippy = mkForce rustToolchain;
  };

  hooks = {
    nixpkgs-fmt.enable = true;
    commitizen.enable = true;
    rustfmt.enable = true;
    cargo-check.enable = true;
    clippy.enable = true;
  };
}
