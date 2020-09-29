let
  pinnedPkgs = import ./nix/packages.nix { nightly = false; };
in

{ pkgs ? pinnedPkgs }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # general purpose deps
    figlet
    cacert

    # rust related deps
    crate2nix
    grcov
    mozilla-rust
    rust-analyzer
  ];
}
