{ buildGoModule, fetchFromGitHub }:

let version = "0.16.0";
in
buildGoModule {
  name = "commitsar";
  inherit version;

  src = fetchFromGitHub {
    owner = "aevea";
    repo = "commitsar";
    rev = "v${version}";
    sha256 = "sha256-ZCl5olalywM6CFXmqvFaNGrK2iSYOyIsX5D9n29ulwA=";
  };

  vendorSha256 = "sha256-BCqxdj4Ed+0tQ4XOMGp8nJDZbJ/SP0OVHmHUM4ijFuM=";
  doCheck = false;
}
