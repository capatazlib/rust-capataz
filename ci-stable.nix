{ pkgs ? import ./nix/packages.nix {} }:

let
  # Using nix instead of cargo to cache as much as possible of the
  # dependant crates in the nix store, accelarating the build times
  nix-cargo = pkgs.callPackage ./Cargo.nix {};
  rust-capataz = nix-cargo.rootCrate.build.override {
    runTests = true;
  };
in
  pkgs.mkShell {
    buildInputs = with pkgs; [
      mozilla-rust
      rust-capataz
    ];
  }
