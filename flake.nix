{
  description = "Tiny document full-text search engine";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    pre-commit-hooks.url = "github:cachix/git-hooks.nix";
    pre-commit-hooks.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    pre-commit-hooks,
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
            starlette
            tika-client
            uvicorn
          ];

          propagatedBuildInputs = [pkgs.tika];

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
        integration = let
          guestSystem =
            if pkgs.stdenv.hostPlatform.isLinux
            then pkgs.stdenv.hostPlatform.system
            else let
              hostToGuest = {
                "x86_64-darwin" = "x86_64-linux";
                "aarch64-darwin" = "aarch64-linux";
              };
            in
              hostToGuest.${pkgs.stdenv.hostPlatform.system};
          integrationTest = pkgs.writeShellScript "aum-integration-test" ''
            cd ${self.packages.${guestSystem}.default.src}
            pytest -v
          '';
        in
          pkgs.nixosTest {
            name = "aum-integration-test";
            nodes = {
              machine = {
                services.meilisearch.enable = true;
                services.sonic-server.enable = true;

                environment.systemPackages = with nixpkgs.legacyPackages.${guestSystem}; [
                  (python3.withPackages (ps: [self.packages.${guestSystem}.default ps.pytest]))
                  tika
                ];
              };
            };
            testScript = ''
              with subtest("Wait for network"):
                  machine.systemctl("start network-online.target")
                  machine.wait_for_unit("network-online.target")

              with subtest("Wait for search engine backend"):
                  machine.wait_for_unit("meilisearch.service")
                  machine.wait_for_unit("sonic-server.service")

              with subtest("Run aum tests"):
                  machine.succeed("${integrationTest}")
            '';
          };
        pre-commit-check = pre-commit-hooks.lib.${system}.run {
          src = ./.;
          hooks = {
            alejandra.enable = true;
            black.enable = true;
            isort.enable = true;
            pylint.enable = true;
          };
        };
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
