let
  sources =
    import ./sources.nix;

  mozilla-overlay =
    import sources.nixpkgs-mozilla;

  project-overlay =
    self: super:
    let
        crate2nix = import (fetchTarball "https://github.com/kolloch/crate2nix/tarball/0.8.0") {};

        grcov = self.callPackage ./grcov {};

        rustTools = self.rustChannelOf {
          rustToolchain = ../rust-toolchain;
        };

        # We want to get the rust package with all these utilities
        mozilla-rust = rustTools.rust.override {
          extensions = [
            "rust-src"
            "rust-std"
            "rustfmt-preview"
            "rls-preview"
            "clippy-preview"
          ];
        };

        # rust-src is a link tree, so we need to get the src attribute from one of
        # it's paths
        mozRustSrc = (builtins.elemAt rustTools.rust-src.paths 0);

        # We need to modify the structure of the rust source package that comes
        # from the nixpkgs-mozilla to work with the one upstream nixpkgs uses.
        rustSrc = super.runCommandLocal "${mozRustSrc.name}-compat.tar.gz" {} ''
          # get contents on directory in place
          tar -xf ${mozRustSrc.src} --strip-components 1
          mkdir out

          # modify the directory structure to work with development/compilers/rust/rust-src.nix
          mv rust-src/lib/rustlib/src/rust/* out
          tar -czf rust-src.tar.gz out

          # vaya con dios
          mv rust-src.tar.gz $out
        '';
      in
        {
          rustTools = rustTools;
          rustPlatform = super.makeRustPlatform {
            cargo = mozilla-rust;
            rustc = (mozilla-rust // { src = rustSrc; });
          };
          mozilla-rust = mozilla-rust;
        };

  pinnedPkgs =
    import sources.nixpkgs {
      overlays = [
        mozilla-overlay
        project-overlay
      ];
    };

in
  pinnedPkgs
