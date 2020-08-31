{pkgs ? import ./nix/packages.nix { nightly = false; }}:

pkgs.mkShell {
  buildInputs = with pkgs; [
    # general purpose deps
    cacert
    figlet
    stdenv

    # rust related deps
    crate2nix
    grcov
    mozilla-rust
    rust-analyzer
  ];
}
