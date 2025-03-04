{

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    fenix.url = "github:nix-community/fenix";
    devenv.url = "github:cachix/devenv";
    nixDir.url = "github:roman/nixDir/v2";
    nixDir.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { nixDir, ... } @ inputs:
    nixDir.lib.buildFlake {
      inherit inputs;
      root = ./.;
      systems = [ "x86_64-linux" "x86_64-darwin" "aarch64-darwin" ];
      generateAllPackage = true;
      injectPreCommit = true;
    };
}
