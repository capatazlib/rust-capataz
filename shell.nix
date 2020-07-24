{pkgs ? import ./nix/packages.nix }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    figlet
    stdenv
    cacert
    mozilla-rust
    rust-analyzer
  ];
}
