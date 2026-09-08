# aum

A document search engine with optional hybrid (keyword + vector) search. It
extracts text and metadata from documents using Apache Tika, indexes them in
OpenSearch (or Meilisearch), and serves a web UI for searching across your corpus.

This is a personal project used to iterate on ideas around document search
and retrieval. It is not production software. If you need a production-grade
document search platform, look at
[OpenAleph](https://github.com/openaleph/openaleph) or
[Datashare](https://datashare.icij.org/) instead.

## Features

- Ingest directories of documents (PDF, Office, email, archives) with
  automatic text extraction and recursive unpacking of nested files
- Full-text search powered by OpenSearch or Meilisearch
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

## Getting started (Docker Compose)

Install Docker with Docker Compose, then clone this repository. Rust and Node
are only needed for building directly from source; Docker builds the bundled app.
The first image build can take several minutes. Allow memory for OpenSearch's
1 GiB heap plus Tika and the application.

```sh
mkdir -p documents
# Put the documents you want to search in ./documents.
docker compose build aum
docker compose run --rm --no-deps aum setup --admin admin --generate-password
docker compose up -d --wait
docker compose exec aum doctor
```

Save the generated password, then log in at `http://localhost:8000` as `admin`.
Setup creates a starter configuration and the first administrator. Running it
again preserves the configuration and existing administrator credentials.

Ingest your documents and try a search:

```sh
docker compose exec aum ingest documents /documents
docker compose exec aum search documents "your search phrase"
```

The documents mount is read-only. To use another directory, set
`AUM_DOCUMENTS_DIR=/absolute/path/to/documents` before running Compose. Keep that
value set for subsequent Compose commands. Application state and search data
persist in named volumes. `docker compose down` stops the stack without deleting
them; adding `--volumes` deletes the stored data.

The local stack exposes the UI, OpenSearch, and Tika only on localhost. It disables
OpenSearch authentication and is intended for local use.

If startup fails, run `docker compose logs opensearch tika aum`. If OpenSearch
reports a `vm.max_map_count` bootstrap error, increase that host kernel setting
as directed in its error message and restart the stack. `aum doctor` prints
individual PASS/FAIL checks and exits nonzero when a required check fails.

## Building from source

Requirements: Rust 1.91+, Node.js 22.12+, a C toolchain, pkg-config, OpenSSL and
SQLite development libraries, plus OpenSearch and Apache Tika. The Compose file
pins the supporting service versions used by the local stack.

```sh
docker compose up -d --wait opensearch tika
cd frontend && npm ci && npm run build && cd ..
cargo build --release --locked
./target/release/aum setup
./target/release/aum doctor
./target/release/aum serve
```

`aum setup` prompts for an administrator username and password. For unattended
setup, use `aum setup --admin admin --generate-password` and save the printed
password. Run source-build commands from the repository root so the configuration,
data directory, and frontend assets resolve consistently.

Then, in another terminal:

```sh
./target/release/aum ingest documents /path/to/documents
```

For a portable binary with the frontend embedded, build with
`cargo build --release --locked --features bundle-frontend`.
Meilisearch is an alternative backend: build with
`--features meilisearch` and set `AUM_SEARCH_BACKEND=meilisearch`.
Embeddings are optional; see Hybrid search below.

## NixOS

A Nix flake is provided for building aum and running it as a NixOS service.

### Building

```sh
nix build
./result/bin/aum --help
```

The frontend dependency hash is included in the flake; no source edits are
needed for a normal build.

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
      opensearch.url = "http://localhost:9200";
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
3. Compiled-in defaults

The application does not load `.env` files. Export `AUM_*` variables in your
shell or put settings in `aum.toml`. Docker Compose uses `.env` for its own
interpolation; it does not automatically pass those variables to aum.

Run `aum config` to print the resolved configuration.

Environment variables map onto the configuration sections, with a double
underscore separating the section from the setting: `AUM_<SECTION>__<KEY>`.
A single underscore is silently ignored, so `AUM_DATA_DIR` has no effect
while `AUM_DATA__DIR` does.

Key settings:

- `AUM_SEARCH_BACKEND` -- `opensearch` or `meilisearch` (default:
  `opensearch`). This one is top-level and takes a single underscore.
- `AUM_OPENSEARCH__URL` -- OpenSearch URL (default: `http://localhost:9200`)
- `AUM_MEILISEARCH__URL` -- Meilisearch URL (default: `http://localhost:7700`)
- `AUM_TIKA__SERVER_URL` -- Tika URL (default: `http://localhost:9998`)
- `AUM_DATA__DIR` -- Directory for the SQLite database and extracted files
  (default: `data`)
- `AUM_SERVER__PORT` -- Server port (default: `8000`)
- `AUM_SERVER__HOST` -- Address to bind (default: `0.0.0.0`)
- `AUM_SERVER__BASE_URL` -- Public base URL (default:
  `http://localhost:8000`)
- `AUM_AUTH__PUBLIC_MODE` -- Allow anonymous read-only search access
  (default: `false`)
- `AUM_AUTH__SESSION_EXPIRE_HOURS` -- Session lifetime in hours (default:
  `168`)
- `AUM_LOG__LEVEL` -- Log level (default: `INFO`)
- `AUM_LOG__FORMAT` -- `json` or `console` (default: `console`)

## CLI reference

All administration is done through the CLI. The web UI is only for
searching.

- `aum setup` -- Create starter configuration and the first administrator
- `aum doctor` -- Check data-directory writes, frontend assets, search, and Tika
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
- `aum config` -- Print the resolved configuration

Run any command with `--help` for full usage details.

## Scaling extraction

By default, aum sends documents to a single Tika server. For large
corpora you can run multiple Tika instances and configure aum to
distribute extraction across them with per-instance concurrency limits:

```toml
# aum.toml
[[tika.instances]]
url = "http://tika1:9998"
concurrency = 4

[[tika.instances]]
url = "http://tika2:9998"
concurrency = 4
```

Instances are selected via round-robin. Unhealthy instances are
automatically taken out of rotation and retried after a cooldown.

Aum sends OCR and plain-text handler settings through Tika 4's per-request
configuration endpoint. External Tika instances must enable
`server.allowPerRequestConfig`; the Compose service mounts the included
[`tika-config.json`](tika-config.json) with this setting enabled.

## Hybrid search

Embedding documents for hybrid search requires either a running
[Ollama](https://ollama.com/) instance or an API key for an
OpenAI-compatible embedding service.

On OpenSearch, hybrid search uses weighted reciprocal rank fusion (RRF):
`semantic_ratio` controls the vector weight and `1 - semantic_ratio` controls
the keyword weight. Zero runs keyword search without requesting embeddings;
one runs vector search only. Intermediate values use a per-request pipeline,
so concurrent users can choose different weights. Use the OpenSearch 3.6
version pinned in Docker Compose for these features.

Fusion and vector retrieval consider at least 1,000 candidates per shard,
independent of page size, and expand for pages beyond that window up to
`offset + limit = 10,000`. This trades additional retrieval work for a broader
candidate pool. Rankings can change when paging beyond the initial window.
API totals and facets currently describe keyword matches, including during
hybrid search; they are not a count of all semantic matches.

See [search quality assessment](docs/search-quality.md) and
[relevance evaluation datasets](evaluation/README.md) for testing and limitations.

### Using Ollama

Run Ollama on the host (recommended for GPU acceleration):

```sh
ollama serve
```

Enable embeddings and generate them:

```sh
AUM_EMBEDDINGS__ENABLED=true aum embed <index>
```

The default model is `qwen3-embedding:0.6b` (256 dimensions). aum will
pull it automatically on first use. To use a different model:

```sh
aum embed <index> --backend ollama --model nomic-embed-text
```

### Using an OpenAI-compatible API

Set the API URL and key, then embed:

```sh
export AUM_EMBEDDINGS__BACKEND=openai
export AUM_EMBEDDINGS__API_URL=https://api.openai.com/v1/embeddings
export AUM_EMBEDDINGS__API_KEY=sk-...
export AUM_EMBEDDINGS__MODEL=text-embedding-3-small
AUM_EMBEDDINGS__ENABLED=true aum embed <index>
```

This also works with any OpenAI-compatible endpoint (vLLM, LiteLLM,
Together, etc).

Once documents have embeddings, set `AUM_EMBEDDINGS__ENABLED=true` when
running the server to enable the hybrid search option in the UI.

## Testing

Run the test suite:

```bash
cargo test
```
