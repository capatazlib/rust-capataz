{pkgs ? import ./nix/packages.nix }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    figlet
    stdenv
    mozilla-rust
    rust-analyzer
  ];
}
