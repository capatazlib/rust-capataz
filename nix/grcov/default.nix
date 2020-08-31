let
  pkgs = import ../packages.nix {};
  pname = "grcov";
  version = "0.5.15";
in
{ rustPlatform ? pkgs.rustPlatform,
  fetchFromGitHub ? pkgs.fetchFromGitHub,
  stdenv ? pkgs.stdenv }:

rustPlatform.buildRustPackage {
  pname = pname;
  version = version;

  src = fetchFromGitHub {
    owner = "mozilla";
    repo = pname;
    rev = "v${version}";
    sha256 = "18iidzdbdiqxk7rfpa4gq25rjx3838y5kgmpkblrhna1rwj33q2l";
  };

  cargoSha256 = "1fsdd05zllarlzslgc2pbq16w11gs981v7niy7cava558awgfh2g";
  verifyCargoDeps = true;

  meta = with stdenv.lib; {
    description = "Rust tool to collect and aggregate code coverage data for multiple source files";
    homepage = https://github.com/mozilla/grcov;
    license = licenses.mpl20;
    platforms = platforms.all;
  };
}
