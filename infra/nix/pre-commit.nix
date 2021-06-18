{ system, pre-commit-flk, commitsar, cap-rust }:

pre-commit-flk.lib.${system}.run {
  src = ./.;

  tools = {
    cargo = cap-rust;
    clippy = cap-rust;
    rustfmt = cap-rust;
  };

  hooks = {
    nix-linter.enable = true;
    nixpkgs-fmt.enable = true;

    cargo-check.enable = true;
    rustfmt.enable = true;
    clippy.enable = true;

    commitsar = {
      enable = true;
      name = "commitsar";
      entry = "${commitsar}/bin/commitsar";
      files = ".*";
      pass_filenames = false;
      raw = { stages = [ "push" ]; };
    };

    cargo-test = {
      enable = true;
      name = "cargo-test";
      entry = "${cap-rust}/bin/cargo test";
      files = "\\.rs$";
      pass_filenames = false;
    };
  };

}
