{ pkgs ? import ./nix/packages.nix { nightly = true; } }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    grcov
    mozilla-rust
  ];
}
