{ self, fenix, ... } @ inputs: { system }:

fenix.packages.${system}.fromToolchainFile {
  file = "${self}/rust-toolchain.toml";
  sha256 = "sha256-ks0nMEGGXKrHnfv4Fku+vhQ7gx76ruv6Ij4fKZR3l78=";
}
