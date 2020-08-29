{ pkgs ? import ./nix/packages.nix {} }:

pkgs.mkShell {
  buildInputs = with pkgs; [
    grcov
    mozilla-rust
  ];
}
