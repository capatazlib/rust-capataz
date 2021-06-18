{

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";

    flake-utils.url = "github:numtide/flake-utils";
    flake-utils.inputs.nixpkgs.follows = "nixpkgs";

    rust-overlay-flk.url = "github:oxalica/rust-overlay";
    rust-overlay-flk.inputs.flake-utils.follows = "flake-utils";

    pre-commit-flk.url = "github:cachix/pre-commit-hooks.nix";
    pre-commit-flk.inputs.nixpkgs.follows = "nixpkgs";
    pre-commit-flk.inputs.flake-utils.follows = "flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils, rust-overlay-flk, pre-commit-flk }:
    flake-utils.lib.eachSystem [ "x86_64-linux" "x86_64-darwin" ]
      (system:
        let
          rust-overlay = import rust-overlay-flk;
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay self.overlay.${system} ];
          };
        in
        {
          overlay = final: prev: {
            # to ensure rust-bin (from rust-overlay) is present, we must
            # rely on final
            cap-rust = final.rust-bin.fromRustupToolchainFile ./rust-toolchain;

            # to ensure cap-rust is present, we must rely on final
            cap-pre-commit = final.callPackage ./infra/nix/pre-commit.nix {
              inherit system pre-commit-flk;
            };

            # tool for checking commits (pending to add to upstream)
            commitsar = prev.callPackage ./infra/nix/tools/commitsar.nix { };
          };

          devShell = pkgs.mkShell {
            buildInputs = builtins.attrValues {
              inherit (pkgs) figlet commitsar cap-rust;
            };

            shellHook = ''
              figlet -f slant rust-Capataz
            '' + pkgs.cap-pre-commit.shellHook;
          };
        }
      );

}
