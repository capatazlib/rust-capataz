{pkgs ? import ./nix/packages.nix }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # general purpose deps
    cacert
    figlet
    stdenv

    # rust related deps
    mozilla-rust
    rust-analyzer
  ];
}
