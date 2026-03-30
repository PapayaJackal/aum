from __future__ import annotations

import os
from pathlib import Path

from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, TomlConfigSettingsSource


class OAuthProvider(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AUM_OAUTH_")

    name: str
    client_id: str
    client_secret: str
    server_metadata_url: str


class TikaInstance(BaseModel):
    """A Tika server endpoint with its own concurrency limit."""

    url: str
    concurrency: int = 0  # 0 = auto (share of ingest_max_workers)


class EmbedderInstance(BaseModel):
    """An embedding service endpoint with its own concurrency limit."""

    url: str
    concurrency: int = 1


class AumConfig(BaseSettings):
    model_config = SettingsConfigDict(
        env_prefix="AUM_",
        toml_file="aum.toml",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        **kwargs: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            kwargs["init_settings"],
            kwargs["env_settings"],
            TomlConfigSettingsSource(settings_cls),
            kwargs["dotenv_settings"],
        )

    # Search backend to use: "meilisearch" (default) or "elasticsearch".
    search_backend: str = "meilisearch"

    # Meilisearch URL.
    meili_url: str = "http://localhost:7700"
    # Meilisearch API key (leave empty for a key-less local instance).
    meili_api_key: str = ""
    # Default index name for ingestion and search (Meilisearch backend).
    meili_index: str = "aum"
    # Blend ratio for hybrid search (0.0 = pure keyword, 1.0 = pure semantic).
    # Higher values favour vector similarity over keyword overlap.
    meili_semantic_ratio: float = 0.5
    # Number of words shown in content snippets.  Meilisearch measures this in
    # words (not characters); 50 words ≈ 300 characters.
    meili_crop_length: int = 50

    # Elasticsearch URL.
    es_url: str = "http://localhost:9200"
    # Default index name for ingestion and search (Elasticsearch backend).
    es_index: str = "aum"
    # Use Reciprocal Rank Fusion for hybrid search scoring instead of
    # combined score. Requires an Elasticsearch license that supports RRF.
    es_rrf: bool = False
    # Maximum character offset Elasticsearch will scan for highlights.
    # Documents longer than this won't get snippet highlights.
    es_max_highlight_offset: int = 10_000_000

    # Multiple Tika server instances for distributed extraction.
    # Each entry specifies a URL and optional per-instance concurrency.
    # When empty, falls back to tika_server_url as a single instance.
    tika_instances: list[TikaInstance] = Field(default_factory=list)

    # Tika server URL for document text and metadata extraction.
    tika_server_url: str = "http://localhost:9998"
    # Request timeout in seconds for Tika HTTP calls. OCR on large documents
    # can take several minutes, so this defaults to 300 seconds.
    tika_request_timeout: int = 300
    # Enable OCR via Tesseract during extraction. Requires a Tika server
    # built with Tesseract support (the -full Docker image).
    ocr_enabled: bool = False
    # Tesseract language code(s). Use "+" to combine, e.g. "fra+eng".
    ocr_language: str = "eng"

    # Enable hybrid search with vector embeddings. When disabled, only
    # keyword (BM25) search is available.
    embeddings_enabled: bool = False
    # Embedding backend: "ollama" for a local/remote Ollama instance,
    # "openai" for any OpenAI-compatible embedding API.
    embeddings_backend: str = "ollama"
    # Embedding model name. Passed to the backend as-is.
    embeddings_model: str = "snowflake-arctic-embed2"
    # Vector dimension. Must match the model's output dimension.
    embeddings_dimension: int = 1024
    # Number of documents per embedding API call.
    embeddings_batch_size: int = 8
    # Maximum token context length for the embedding model. Text is chunked
    # so that each chunk fits within this limit.
    embeddings_context_length: int = 8192
    # Character overlap between consecutive text chunks.
    embeddings_chunk_overlap: int = 200
    # Prefix prepended to search queries before embedding. Many embedding
    # models expect a task prefix for asymmetric retrieval.
    embeddings_query_prefix: str = "query: "

    # Multiple embedding service instances for distributed embedding.
    # Each entry specifies a URL and optional per-instance concurrency.
    # When empty, falls back to ollama_url or embeddings_api_url.
    embedder_instances: list[EmbedderInstance] = Field(default_factory=list)

    # Ollama API URL.
    ollama_url: str = "http://localhost:11434"

    # OpenAI-compatible embedding API endpoint URL.
    embeddings_api_url: str = ""
    # Bearer token for the OpenAI-compatible embedding API.
    embeddings_api_key: str = ""

    # Directory for application data: the SQLite database, extracted
    # attachments, and file caches. The entire directory is portable.
    data_dir: str = "data"

    # Base URL users access the site from (e.g. "https://search.example.com").
    # Used for WebAuthn origin validation and invitation links.
    base_url: str = "http://localhost:8000"

    # Address to bind the web server to.
    host: str = "0.0.0.0"
    # Port for the web server and API.
    port: int = 8000
    # Port for the Prometheus metrics endpoint (/metrics).
    metrics_port: int = 9090
    # Enable the Swagger/OpenAPI docs UI at /api/docs.
    enable_docs: bool = False
    # Allowed CORS origins. Leave empty to disable CORS.
    cors_origins: list[str] = Field(default_factory=list)

    # Public mode: allow anonymous access to search, require admin for jobs API.
    public_mode: bool = False

    # JWT signing secret. If empty, a random secret is generated on each
    # restart and all sessions will be invalidated. Set this to a stable
    # value (at least 32 bytes) for persistent sessions.
    jwt_secret: str = ""
    # JWT signing algorithm.
    jwt_algorithm: str = "HS256"
    # Access token lifetime in minutes.
    access_token_expire_minutes: int = 720
    # Refresh token lifetime in days.
    refresh_token_expire_days: int = 7
    # Minimum password length for local user accounts.
    password_min_length: int = 8
    # OAuth/OIDC providers for federated login.
    oauth_providers: list[OAuthProvider] = Field(default_factory=list)

    # WebAuthn Relying Party ID — the domain users access the site from
    # (e.g. "search.example.com", or "localhost" for development).
    webauthn_rp_id: str = "localhost"
    # WebAuthn Relying Party name (human-readable).
    webauthn_rp_name: str = "aum"
    # Enable WebAuthn passkey authentication.
    passkey_enabled: bool = False
    # Require all users to register a passkey to log in.
    passkey_required: bool = False

    # Log level (DEBUG, INFO, WARNING, ERROR).
    log_level: str = "INFO"
    # Log output format: "json" for structured logging, "console" for
    # human-readable output.
    log_format: str = "console"

    # Number of documents per batch when bulk-indexing into Elasticsearch.
    ingest_batch_size: int = 50
    # Number of parallel workers for document extraction via Tika.
    ingest_max_workers: int = os.cpu_count() or 4
    # Maximum nesting depth for recursive extraction of archives, email
    # attachments, and other compound documents.
    ingest_max_extract_depth: int = 5
    # Maximum content length in characters per extracted document. Content
    # exceeding this is truncated and a ContentTruncated warning recorded
    # so the user can split or re-extract the file. 0 = no limit.
    ingest_max_content_length: int = 10 * 1024 * 1024

    @property
    def db_path(self) -> str:
        path = Path(self.data_dir) / "aum.db"
        path.parent.mkdir(parents=True, exist_ok=True)
        return str(path)

    @property
    def extract_dir(self) -> str:
        path = Path(self.data_dir) / "extracted"
        path.mkdir(parents=True, exist_ok=True)
        return str(path)

    @property
    def effective_tika_instances(self) -> list[TikaInstance]:
        """Resolved Tika instance list with concurrency defaults.

        Falls back to the single ``tika_server_url`` when no explicit
        instances are configured.  Instances with ``concurrency=0`` get
        an equal share of ``ingest_max_workers``.
        """
        instances = self.tika_instances or [TikaInstance(url=self.tika_server_url)]
        result: list[TikaInstance] = []
        n_auto = sum(1 for i in instances if i.concurrency == 0)
        auto_share = max(1, self.ingest_max_workers // max(n_auto, 1))
        for inst in instances:
            if inst.concurrency == 0:
                result.append(TikaInstance(url=inst.url, concurrency=auto_share))
            else:
                result.append(inst)
        return result

    @property
    def effective_embedder_instances(self) -> list[EmbedderInstance]:
        """Resolved embedder instance list.

        Falls back to the single ``ollama_url`` or ``embeddings_api_url``
        when no explicit instances are configured.
        """
        if self.embedder_instances:
            return self.embedder_instances
        url = self.ollama_url if self.embeddings_backend == "ollama" else self.embeddings_api_url
        return [EmbedderInstance(url=url, concurrency=1)]
