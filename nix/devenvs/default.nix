{ self, ... } @ inputs: {pkgs, config, ...}:

let
  selfPkgs = self.packages.${pkgs.system};
in

{
  packages = builtins.attrValues {
    inherit (pkgs) figlet lolcat gnumake;
    inherit (selfPkgs) toolchain;
  };

  enterShell = ''
    figlet -f slant rust-Capataz | lolcat
  '';

}
