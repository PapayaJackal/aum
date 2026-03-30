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
- Prometheus metrics
- All application state stored in a single portable SQLite database

## Requirements

- Python 3.11+
- Meilisearch 1.x+
- Apache Tika 3.x
- Node.js 22+ (to build the frontend)
- [uv](https://docs.astral.sh/uv/)
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

Create an admin user:

```sh
uv run aum user create admin --admin --generate-password
```

Start the server:

```sh
uv run aum serve
```

The web UI will be available at `http://localhost:8000`.

Ingest a directory of documents:

```sh
uv run aum ingest /path/to/documents
```

## Configuration

aum reads configuration from these sources, in order of priority:

1. Environment variables with the `AUM_` prefix
2. An `aum.toml` file in the working directory
3. A `.env` file in the working directory

Run `uv run aum config` to print the resolved configuration.

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
- `AUM_METRICS_PORT` -- Prometheus metrics port (default: `9090`)

## CLI reference

All administration is done through the CLI. The web UI is only for
searching.

- `aum serve` -- Start the web server
- `aum ingest <directory>` -- Ingest documents from a directory
- `aum resume [job_id]` -- Resume an interrupted ingest or embedding job
- `aum embed` -- Generate embeddings for documents that lack them
- `aum init <index>` -- Create or initialize a search index
- `aum reset <index>` -- Delete and recreate an index
- `aum indices` -- List all indices with document counts
- `aum search <query>` -- Search from the command line
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

Run any command with `--help` for full usage details. All commands should be
invoked with `uv run aum`.

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
AUM_EMBEDDINGS_ENABLED=true uv run aum embed
```

The default model is `snowflake-arctic-embed2` (1024 dimensions). aum will
pull it automatically on first use. To use a different model:

```sh
uv run aum embed --backend ollama --model nomic-embed-text
```

### Using an OpenAI-compatible API

Set the API URL and key, then embed:

```sh
export AUM_EMBEDDINGS_BACKEND=openai
export AUM_EMBEDDINGS_API_URL=https://api.openai.com/v1/embeddings
export AUM_EMBEDDINGS_API_KEY=sk-...
export AUM_EMBEDDINGS_MODEL=text-embedding-3-small
AUM_EMBEDDINGS_ENABLED=true uv run aum embed
```

This also works with any OpenAI-compatible endpoint (vLLM, LiteLLM,
Together, etc).

Once documents have embeddings, set `AUM_EMBEDDINGS_ENABLED=true` when
running the server to enable the hybrid search option in the UI.

## Testing

Unit tests run locally without external services:

```bash
uv run python -m pytest
```

Integration tests exercise the full stack (Meilisearch, Tika, Ollama) inside containers. The frontend is built automatically as part of the Docker image:

```bash
docker compose -f docker-compose.test.yml up --build --abort-on-container-exit --exit-code-from test-runner
docker compose -f docker-compose.test.yml down -v
```

This ingests real test data, runs CLI/API/embedding/permission tests, and Playwright browser tests against the SPA.

## Prometheus metrics

aum exposes Prometheus metrics on a separate port (default 9090) at
`/metrics`. The metrics server starts automatically with `aum serve`.

### Ingest

- `aum_documents_ingested_total` (counter, labels: `backend`) -- Documents successfully ingested.
- `aum_documents_failed_total` (counter, labels: `error_type`) -- Documents that failed during ingestion.
- `aum_ingest_duration_seconds` (histogram, labels: `stage`) -- Time to process a single document, broken down by pipeline stage.
- `aum_ingest_jobs_active` (gauge) -- Number of currently running ingest jobs.

### Index management

- `aum_indexes_recreated_total` (counter, labels: `index`) -- Times an index was recreated due to a mapping change.

### Search

- `aum_search_requests_total` (counter, labels: `type`) -- Total search requests by search type (text, hybrid).
- `aum_search_latency_seconds` (histogram, labels: `type`, `backend`) -- Search request latency.

### Extraction

- `aum_extraction_duration_seconds` (histogram) -- Time to extract text from a document via Tika.
- `aum_extraction_errors_total` (counter, labels: `error_type`) -- Extraction failures by error type.

### Embedding

- `aum_embedding_duration_seconds` (histogram, labels: `backend`) -- Time to embed a batch of documents.
- `aum_embedding_requests_total` (counter, labels: `backend`) -- Total embedding API requests.
- `aum_embedding_docs_processed_total` (counter) -- Documents successfully embedded.
- `aum_embedding_docs_failed_total` (counter) -- Documents that failed embedding.
- `aum_embedding_jobs_active` (gauge) -- Number of currently running embedding jobs.

### Authentication

- `aum_auth_requests_total` (counter, labels: `method`) -- Authentication attempts by method (local, OAuth).
- `aum_auth_failures_total` (counter, labels: `reason`) -- Failed authentication attempts by reason.
- `aum_auth_rate_limited_total` (counter) -- Login attempts rejected by the rate limiter.

### Documents

- `aum_document_views_total` (counter) -- Document detail page views.
- `aum_document_downloads_total` (counter) -- Document file downloads.
- `aum_document_previews_total` (counter, labels: `content_type`) -- Document preview requests.
- `aum_thread_lookups_total` (counter) -- Email thread reconstruction lookups.

### Instance pool

- `aum_pool_requests_total` (counter, labels: `service`, `url`) -- Requests dispatched to pooled service instances.
- `aum_pool_errors_total` (counter, labels: `service`, `url`, `error`) -- Errors from pooled instances.
- `aum_pool_request_duration_seconds` (histogram, labels: `service`, `url`) -- Request latency per instance.
- `aum_pool_instance_healthy` (gauge, labels: `service`, `url`) -- Health status per instance (1 = healthy).
- `aum_pool_instance_in_flight` (gauge, labels: `service`, `url`) -- In-flight requests per instance.

### Build

- `aum_build_info` (info) -- Build and version metadata.
