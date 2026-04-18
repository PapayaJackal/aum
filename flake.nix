{
  description = "aum — document search engine (Rust + Svelte)";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    crane.url = "github:ipetkov/crane";
    rust-overlay = {
      url = "github:oxalica/rust-overlay";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs = { self, nixpkgs, flake-utils, crane, rust-overlay }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          overlays = [ rust-overlay.overlays.default ];
        };

        inherit (pkgs) lib;

        # Rust ≥1.85 is required by edition 2024.
        rustToolchain = pkgs.rust-bin.stable.latest.default;

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;

        # cleanCargoSource strips non-Rust files; add .sql so sqlx::migrate!()
        # can embed the migration files at compile time.
        src =
          let
            sqlFilter = path: _type: builtins.match ".*[.]sql$" path != null;
            sqlOrCargo = path: type: (sqlFilter path type) || (craneLib.filterCargoSources path type);
          in
          lib.cleanSourceWith {
            src = ./.;
            filter = sqlOrCargo;
          };

        commonNativeBuildInputs = [ pkgs.pkg-config ];
        # libsqlite3-sys links to system sqlite (no 'bundled' feature).
        # openssl-sys is pulled in transitively and needs the system library.
        commonBuildInputs = [ pkgs.sqlite pkgs.openssl ];

        # utoipa-swagger-ui's build.rs tries to download swagger-ui at compile
        # time.  Pre-fetch the zip and copy it into the writable build directory
        # in postUnpack.  We cannot just point SWAGGER_UI_DOWNLOAD_URL at the
        # Nix store path directly: Nix marks store files immutable, and Rust's
        # std::fs::copy uses copy_file_range which returns EPERM for immutable
        # source files.  Copying to the build tree first avoids this.
        # The path is then written into .cargo/config.toml (force = true) so the
        # build script picks it up regardless of any env var already set.
        swaggerUiZip = pkgs.fetchurl {
          url = "https://github.com/swagger-api/swagger-ui/archive/refs/tags/v5.17.14.zip";
          name = "swagger-ui-v5.17.14.zip";
          hash = "sha256-SBJE0IEgl7Efuu73n3HZQrFxYX+cn5UU5jrL4T5xzNw=";
        };

        # Shared postUnpack snippet used in every Rust build derivation.
        swaggerSetupHook = ''
          mkdir -p "$sourceRoot/.build-deps" "$sourceRoot/.cargo"
          cp ${swaggerUiZip} "$sourceRoot/.build-deps/v5.17.14.zip"
          chmod +w "$sourceRoot/.build-deps/v5.17.14.zip"
          printf '\n[env]\nSWAGGER_UI_DOWNLOAD_URL = { value = "file://%s/.build-deps/v5.17.14.zip", force = true }\n' \
            "$(realpath "$sourceRoot")" >> "$sourceRoot/.cargo/config.toml"
        '';

        commonCargoArgs = {
          inherit src;
          pname = "aum";
          strictDeps = true;
          nativeBuildInputs = commonNativeBuildInputs;
          buildInputs = commonBuildInputs;
        };

        # ── Frontend ──────────────────────────────────────────────────────────

        npmDeps = pkgs.fetchNpmDeps {
          name = "aum-frontend-npm-deps";
          src = ./frontend;
          # Run `nix build .#frontend` with this fake hash to discover the real one.
          hash = "sha256-5MT8+DyWzNPTguqMaMrH42ZRn+zGmhYOSiBZXIkhaz0=";
        };

        frontend = pkgs.stdenv.mkDerivation {
          name = "aum-frontend";
          src = ./frontend;

          nativeBuildInputs = [
            pkgs.nodejs_22
            pkgs.npmHooks.npmConfigHook
          ];

          inherit npmDeps;

          buildPhase = ''
            runHook preBuild
            npm run build
            runHook postBuild
          '';

          installPhase = ''
            runHook preInstall
            cp -r dist $out
            runHook postInstall
          '';
        };

        # ── Rust deps-only cache ──────────────────────────────────────────────
        #
        # aum-api/build.rs checks `if dist_dir.is_dir() { return; }` before
        # running npm.  We inject a dummy dist/ so the check passes during the
        # dep-only compilation pass, where network and Node are unavailable.
        cargoArtifacts = craneLib.buildDepsOnly (commonCargoArgs // {
          cargoExtraArgs = "--bin aum --features aum-cli/bundle-frontend";

          postUnpack = swaggerSetupHook + ''
            mkdir -p "$sourceRoot/frontend/dist"
            printf '<!DOCTYPE html><html></html>' > "$sourceRoot/frontend/dist/index.html"
          '';
        });

        # ── Final binary ──────────────────────────────────────────────────────
        #
        # Copy the pre-built frontend dist before cargo runs so rust-embed can
        # embed it.  build.rs sees dist/ exists and skips npm again.
        aum = craneLib.buildPackage (commonCargoArgs // {
          inherit cargoArtifacts;
          cargoExtraArgs = "--bin aum --features aum-cli/bundle-frontend";

          postUnpack = swaggerSetupHook;

          preBuild = ''
            mkdir -p frontend
            cp -r ${frontend} frontend/dist
          '';
        });

      in {
        packages = {
          inherit aum frontend;
          default = aum;
        };

        devShells.default = pkgs.mkShell {
          inputsFrom = [ aum ];
          nativeBuildInputs = [
            rustToolchain
            pkgs.rust-analyzer
            pkgs.nodejs_22
            pkgs.cargo-watch
          ];
          shellHook = ''
            echo "aum dev shell — run 'cargo build' or 'npm run dev' in frontend/"
          '';
        };
      }
    ) // {
      nixosModules.default = import ./nix/module.nix self;
    };
}
