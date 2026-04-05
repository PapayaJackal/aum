# aum

A document search engine with optional hybrid (keyword + vector) search. It
extracts text and metadata from documents using Apache Tika, indexes them in
Meilisearch, and serves a web UI for searching across your corpus.

This is a personal project used to iterate on ideas around document search
and retrieval. It is not production software. If you need a production-grade
document search platform, look at
[OpenAleph](https://github.com/openaleph/openaleph) or
[Datashare](https://datashare.icij.org/) instead.

## Features

- Ingest directories of documents (PDF, Office, email, archives) with
  automatic text extraction and recursive unpacking of nested files
- Full-text search powered by Meilisearch
- Optional hybrid search combining BM25 keyword scoring with vector
  similarity (via Ollama or any OpenAI-compatible embedding API), with
  an adjustable semantic ratio slider in the UI
- Rich document preview for images, PDFs (rendered with PDF.js), HTML
  files, and emails, with DOMPurify sanitization and CSP isolation
- Email thread reconstruction with a unified thread view
- Faceted filtering by file type, author, date, and email addresses
- Sort results by relevance, date, or file size
- Resizable split-pane search UI with fullscreen preview mode
- Vim-style keyboard navigation (`j`/`k` to move, `o` to open, `?`
  for the shortcut reference)
- Multi-index support with per-user access control
- OCR support via Tesseract (through Tika)
- Multi-instance Tika and embedder pools with per-instance concurrency
  limits and automatic health tracking
- Resume interrupted ingest and embedding jobs with crash detection
- User management with local passwords, OAuth/OIDC, and
  WebAuthn/passkey authentication
- User invitations for onboarding new users via link
- Public mode for anonymous read-only access to search
- CLI-first administration
- All application state stored in a single portable SQLite database

## Requirements

- Rust 1.85+ (to build from source)
- Meilisearch 1.x+
- Apache Tika 3.x
- Node.js 22+ (to build the frontend)
- Optional: Ollama or an OpenAI-compatible API for embeddings

## Getting started

Start the supporting services (Meilisearch and Tika) with Docker Compose:

```sh
docker compose up -d
```

Build the frontend:

```sh
cd frontend && npm ci && npm run build && cd ..
```

Build the binaries:

```sh
cargo build --release
```

Create an admin user:

```sh
./target/release/aum user create admin --admin --generate-password
```

Start the server:

```sh
./target/release/aum-api serve
```

The web UI will be available at `http://localhost:8000`.

Ingest a directory of documents:

```sh
./target/release/aum ingest <index> /path/to/documents
```

## NixOS

A Nix flake is provided for building aum and running it as a NixOS service.

### Building

```sh
nix build
./result/bin/aum --help
```

The first build requires discovering the npm dependency hash. Run
`nix build .#frontend` — it will fail and print the correct hash; paste it
into the `fetchNpmDeps.hash` field in `flake.nix`, then run `nix build` again.

### NixOS module

Add the flake as an input and enable the module:

```nix
# flake.nix
inputs.aum.url = "github:PapayaJackal/aum";

# nixosConfiguration
{ inputs, ... }: {
  imports = [ inputs.aum.nixosModules.default ];

  services.aum = {
    enable = true;
    settings = {
      server.base_url = "https://search.example.com";
      server.port = 8000;
      meilisearch.url = "http://localhost:7700";
      auth.public_mode = false;
    };
  };
}
```

The module creates an `aum` system user, writes the configuration to
`/etc/aum/aum.toml`, and runs `aum serve` as a systemd service with
`WorkingDirectory=/var/lib/aum`.

An `aum` wrapper is also added to `environment.systemPackages` so that CLI
commands (`aum ingest`, `aum user create`, etc.) automatically target the
same data directory as the service.

### Secrets

Do not put API keys or passwords in `services.aum.settings` — the Nix store
is world-readable. Pass secrets as environment variables instead:

```nix
systemd.services.aum.serviceConfig.EnvironmentFile = "/run/secrets/aum";
```

The secrets file should contain `AUM_*` assignments, for example:

```sh
AUM_MEILISEARCH__API_KEY=sk-...
AUM_EMBEDDINGS__API_KEY=sk-...
```

## Configuration

aum reads configuration from these sources, in order of priority:

1. Environment variables with the `AUM_` prefix
2. An `aum.toml` file in the working directory
3. A `.env` file in the working directory

Run `aum config` to print the resolved configuration.

Key settings:

- `AUM_MEILI_URL` -- Meilisearch URL (default: `http://localhost:7700`)
- `AUM_TIKA_SERVER_URL` -- Tika URL (default: `http://localhost:9998`)
- `AUM_JWT_SECRET` -- JWT signing secret (at least 32 bytes). If not
  set, a random one is generated on each restart and sessions will not
  persist.
- `AUM_DATA_DIR` -- Directory for the SQLite database and extracted files
  (default: `data`)
- `AUM_PUBLIC_MODE` -- Allow anonymous read-only search access (default:
  `false`)
- `AUM_PASSKEY_ENABLED` -- Enable WebAuthn/passkey registration and
  authentication (default: `false`)
- `AUM_PASSKEY_REQUIRED` -- Require all users to register a passkey
  (default: `false`)
- `AUM_WEBAUTHN_RP_ID` -- WebAuthn Relying Party ID, the domain users
  access the site from (default: `localhost`)
- `AUM_LOG_LEVEL` -- Log level (default: `INFO`)
- `AUM_LOG_FORMAT` -- `json` or `console` (default: `json`)
- `AUM_PORT` -- Server port (default: `8000`)

## CLI reference

All administration is done through the CLI. The web UI is only for
searching.

- `aum serve` -- Start the web server
- `aum ingest <index> <directory>` -- Ingest documents from a directory
- `aum resume [job_id]` -- Resume an interrupted ingest or embedding job
- `aum embed <index>` -- Generate embeddings for documents that lack them
- `aum init <index>` -- Create or initialize a search index
- `aum reset <index>` -- Delete and recreate an index
- `aum indices` -- List all indices with document counts
- `aum search <index> <query>` -- Search from the command line
- `aum jobs` -- List ingest and embedding jobs
- `aum job <id>` -- Show details for a specific job
- `aum retry <id>` -- Retry failed items from a job (`--only` to filter by error type)
- `aum user create <name>` -- Create a user (`--admin`, `--generate-password`)
- `aum user list` -- List all users
- `aum user delete <name>` -- Delete a user
- `aum user set-password <name>` -- Change a user's password
- `aum user set-admin <name>` -- Grant or revoke (`--revoke`) admin
- `aum user grant <name> <index>` -- Grant a user access to an index
- `aum user revoke <name> <index>` -- Revoke access to an index
- `aum user token <name>` -- Generate an API token
- `aum user invite <name>` -- Generate an invitation link (`--admin`, `--expires`)
- `aum user reset-mfa <name>` -- Remove all passkeys for a user
- `aum config` -- Print the resolved configuration

Run any command with `--help` for full usage details.

## Scaling extraction

By default, aum sends documents to a single Tika server. For large
corpora you can run multiple Tika instances and configure aum to
distribute extraction across them with per-instance concurrency limits:

```toml
# aum.toml
[[tika_instances]]
url = "http://tika1:9998"
concurrency = 4

[[tika_instances]]
url = "http://tika2:9998"
concurrency = 4
```

Instances are selected via round-robin. Unhealthy instances are
automatically taken out of rotation and retried after a cooldown.

## Hybrid search

Embedding documents for hybrid search requires either a running
[Ollama](https://ollama.com/) instance or an API key for an
OpenAI-compatible embedding service.

### Using Ollama

Run Ollama on the host (recommended for GPU acceleration):

```sh
ollama serve
```

Enable embeddings and generate them:

```sh
AUM_EMBEDDINGS_ENABLED=true aum embed <index>
```

The default model is `qwen3-embedding:0.6b` (256 dimensions). aum will
pull it automatically on first use. To use a different model:

```sh
aum embed <index> --backend ollama --model nomic-embed-text
```

### Using an OpenAI-compatible API

Set the API URL and key, then embed:

```sh
export AUM_EMBEDDINGS_BACKEND=openai
export AUM_EMBEDDINGS_API_URL=https://api.openai.com/v1/embeddings
export AUM_EMBEDDINGS_API_KEY=sk-...
export AUM_EMBEDDINGS_MODEL=text-embedding-3-small
AUM_EMBEDDINGS_ENABLED=true aum embed <index>
```

This also works with any OpenAI-compatible endpoint (vLLM, LiteLLM,
Together, etc).

Once documents have embeddings, set `AUM_EMBEDDINGS_ENABLED=true` when
running the server to enable the hybrid search option in the UI.

## Testing

Run the test suite:

```bash
cargo test
```
