from __future__ import annotations

import os
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict, TomlConfigSettingsSource


class OAuthProvider(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="AUM_OAUTH_")

    name: str
    client_id: str
    client_secret: str
    server_metadata_url: str


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

    # Search backend
    search_backend: str = "elasticsearch"

    # Elasticsearch
    es_url: str = "http://localhost:9200"
    es_index: str = "aum"
    es_rrf: bool = False
    es_max_highlight_offset: int = 10_000_000

    # Tika
    tika_server_url: str = "http://localhost:9998"
    ocr_enabled: bool = False
    ocr_language: str = "eng"

    # Embeddings
    embeddings_enabled: bool = False
    embeddings_backend: str = "ollama"  # "ollama" or "openai"
    embeddings_model: str = "snowflake-arctic-embed2"
    embeddings_dimension: int = 1024
    embeddings_batch_size: int = 8
    embeddings_context_length: int = 8192
    embeddings_chunk_overlap: int = 200
    embeddings_query_prefix: str = "query: "

    # Ollama
    ollama_url: str = "http://localhost:11434"

    # OpenAI-compatible embedding API
    embeddings_api_url: str = ""
    embeddings_api_key: str = ""

    # Data directory — stores DB, extracted attachments, converted PDFs, caches, etc.
    data_dir: str = "data"

    # Server
    host: str = "0.0.0.0"
    port: int = 8000
    metrics_port: int = 9090
    enable_docs: bool = False

    # Auth
    jwt_secret: str = ""
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 720
    refresh_token_expire_days: int = 7
    password_min_length: int = 8
    oauth_providers: list[OAuthProvider] = Field(default_factory=list)

    # Logging
    log_level: str = "INFO"
    log_format: str = "json"

    # Ingest
    ingest_batch_size: int = 50
    ingest_max_workers: int = os.cpu_count() or 4
    ingest_max_extract_depth: int = 5

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
