{
  description = "aum - document search engine";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        python = pkgs.python312;
      in {
        devShells.default = pkgs.mkShell {
          packages = [
            python
            pkgs.uv
            pkgs.ruff
            pkgs.nodejs_22
            pkgs.jdk21_headless
          ];

          shellHook = ''
            export AUM_TIKA_SERVER_URL=http://localhost:9998
            echo "aum dev shell"
            echo "  uv sync              - install Python deps"
            echo "  cd frontend && npm i - install frontend deps"
          '';
        };
      });
}
