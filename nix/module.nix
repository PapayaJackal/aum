# NixOS module for aum.
# Import via flake: nixosModules.default = import ./nix/module.nix self;
flake:

{ config, lib, pkgs, ... }:

let
  cfg = config.services.aum;

  tomlFormat = pkgs.formats.toml { };
  configFile = tomlFormat.generate "aum.toml" cfg.settings;

  # Wrapper placed in PATH so that `aum <subcommand>` from the CLI uses the
  # same data directory as the running service.
  cliWrapper = pkgs.writeShellScriptBin "aum" ''
    export AUM_DATA__DIR="${cfg.dataDir}"
    exec ${cfg.package}/bin/aum "$@"
  '';

in {
  options.services.aum = {

    enable = lib.mkEnableOption "aum document search engine";

    package = lib.mkOption {
      type = lib.types.package;
      default = flake.packages.${pkgs.stdenv.hostPlatform.system}.default;
      defaultText = lib.literalExpression "flake.packages.\${system}.default";
      description = "The aum package to use.";
    };

    dataDir = lib.mkOption {
      type = lib.types.str;
      default = "/var/lib/aum";
      description = ''
        Directory for aum's SQLite database, extracted content, and lock files.
        Created and owned by the <literal>aum</literal> service user.
      '';
    };

    settings = lib.mkOption {
      type = lib.types.submodule {
        # freeformType lets users set any valid aum.toml key without needing
        # an explicit option declaration for every field.
        freeformType = tomlFormat.type;

        options = {

          data.dir = lib.mkOption {
            type = lib.types.str;
            default = cfg.dataDir;
            description = "Root directory for aum data files.";
          };

          server.host = lib.mkOption {
            type = lib.types.str;
            default = "127.0.0.1";
            description = "IP address the HTTP server binds to.";
          };

          server.port = lib.mkOption {
            type = lib.types.port;
            default = 8000;
            description = "TCP port for the HTTP server.";
          };

          server.base_url = lib.mkOption {
            type = lib.types.str;
            default = "http://localhost:8000";
            description = "Public base URL used in self-referential links.";
          };

          log.level = lib.mkOption {
            type = lib.types.enum [ "DEBUG" "INFO" "WARNING" "ERROR" ];
            default = "INFO";
            description = "Minimum log severity level.";
          };

          log.format = lib.mkOption {
            type = lib.types.enum [ "console" "json" ];
            # json integrates better with journald / structured log aggregators
            default = "json";
            description = "Log output format.";
          };

          meilisearch.url = lib.mkOption {
            type = lib.types.str;
            default = "http://localhost:7700";
            description = "URL of the Meilisearch instance.";
          };

        };
      };

      default = { };
      description = ''
        Configuration written to <literal>/etc/aum/aum.toml</literal>.
        This attrset is serialized directly to TOML; any valid aum config key
        may be set here.

        Secrets (API keys, passwords) should NOT be placed here — the Nix store
        is world-readable.  Instead use:
        <programlisting>
        systemd.services.aum.serviceConfig.EnvironmentFile = "/run/secrets/aum";
        </programlisting>
        with a file containing <literal>AUM_MEILISEARCH__API_KEY=...</literal> etc.

        Example:
        <programlisting>
        services.aum.settings = {
          server.port = 8080;
          server.base_url = "https://search.example.com";
          meilisearch.url = "http://meili.internal:7700";
          auth.public_mode = false;
          embeddings.enabled = false;
        };
        </programlisting>
      '';
    };

  };

  config = lib.mkIf cfg.enable {

    # Generated TOML config at /etc/aum/aum.toml (read-only Nix store path).
    environment.etc."aum/aum.toml".source = configFile;

    # CLI wrapper inherits the configured data directory.
    environment.systemPackages = [ cliWrapper ];

    users.users.aum = {
      isSystemUser = true;
      group = "aum";
      description = "aum service user";
      home = cfg.dataDir;
      createHome = false; # systemd StateDirectory handles creation
    };

    users.groups.aum = { };

    systemd.services.aum = {
      description = "aum document search engine";
      after = [ "network.target" ];
      wantedBy = [ "multi-user.target" ];

      serviceConfig = {
        Type = "simple";
        ExecStart = "${cfg.package}/bin/aum serve";
        Restart = "on-failure";
        RestartSec = "5s";

        User = "aum";
        Group = "aum";

        # systemd creates /var/lib/aum owned by the service user before any
        # ExecStartPre runs, so the symlink below can always be written.
        StateDirectory = "aum";
        StateDirectoryMode = "0750";

        # aum loads "aum.toml" relative to its working directory.  Symlink the
        # generated config there so load_config() finds it.  The '+' prefix
        # runs this as root (needed to write into StateDirectory before the
        # privilege drop), and ln -sf is idempotent across restarts.
        ExecStartPre = [
          "+${pkgs.coreutils}/bin/ln -sf /etc/aum/aum.toml ${cfg.dataDir}/aum.toml"
        ];

        WorkingDirectory = cfg.dataDir;

        # Hardening
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        ReadWritePaths = [ cfg.dataDir ];
        PrivateTmp = true;
        PrivateDevices = true;
        RestrictAddressFamilies = [ "AF_INET" "AF_INET6" "AF_UNIX" ];
        CapabilityBoundingSet = "";
        AmbientCapabilities = "";
        SystemCallFilter = [ "@system-service" ];
        SystemCallErrorNumber = "EPERM";
        LimitNOFILE = 65536;
      };

      environment = {
        # Belt-and-suspenders: ensures the correct data dir even if the
        # working directory is somehow changed.
        AUM_DATA__DIR = cfg.dataDir;
      };
    };
  };
}
