{
  description = "Tiny document full-text search engine";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = nixpkgs.legacyPackages.${system};
      aum = {pythonPkgs}:
        pythonPkgs.buildPythonPackage {
          name = "aum";
          pyproject = true;
          src = ./.;

          dependencies = with pythonPkgs; [
            meilisearch
            poetry-core
            tika-client
          ];

          nativeCheckInputs = with pythonPkgs; [
            pytestCheckHook
            pkgs.tika
          ];

          disabledTests = [
            "integration"
          ];
        };
    in {
      checks = {
        testPython311 = aum {pythonPkgs = pkgs.python311Packages;};
        testPython312 = aum {pythonPkgs = pkgs.python312Packages;};
        testPython313 = aum {pythonPkgs = pkgs.python312Packages;};
      };

      devShells.default = pkgs.mkShell {
        buildInputs = with pkgs; [
          black
          isort
          poetry
          tika
        ];
      };

      formatter = pkgs.alejandra;

      packages.default = aum {pythonPkgs = pkgs.python3Packages;};
    });
}
