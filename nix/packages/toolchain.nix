{ self, fenix, ... } @ inputs: { system }:

fenix.packages.${system}.fromToolchainFile {
  file = "${self}/rust-toolchain.toml";
  sha256 = "sha256-e4mlaJehWBymYxJGgnbuCObVlqMlQSilZ8FljG9zPHY=";
}
