from __future__ import annotations

import sqlite3
from functools import lru_cache
from typing import Annotated

import structlog
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from aum.auth.local import LocalAuth
from aum.auth.models import User
from aum.auth.oauth import OAuthManager
from aum.auth.permissions import PermissionManager
from aum.auth.tokens import TokenError, TokenManager
from aum.config import AumConfig
from aum.ingest.tracker import JobTracker
from aum.search.base import SearchBackend
from aum.search.elasticsearch import ElasticsearchBackend

log = structlog.get_logger()

_bearer = HTTPBearer(auto_error=False)


# --- Internal helpers (accept config, used by CLI and internally) ---


@lru_cache
def get_config() -> AumConfig:
    return AumConfig()


_db_cache: dict[str, sqlite3.Connection] = {}


def _get_db(config: AumConfig) -> sqlite3.Connection:
    if config.db_path not in _db_cache:
        conn = sqlite3.connect(config.db_path, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        _db_cache[config.db_path] = conn
    return _db_cache[config.db_path]


def make_embedder(config: AumConfig):
    """Create an embedder instance based on config."""
    from aum.embeddings.base import Embedder

    if config.embeddings_backend == "ollama":
        from aum.embeddings.ollama import OllamaEmbedder

        return OllamaEmbedder(
            model=config.embeddings_model,
            base_url=config.ollama_url,
            expected_dimension=config.embeddings_dimension,
            context_length=config.embeddings_context_length,
            query_prefix=config.embeddings_query_prefix,
        )
    elif config.embeddings_backend == "openai":
        from aum.embeddings.openai import OpenAIEmbedder

        if not config.embeddings_api_url:
            raise ValueError("embeddings_api_url must be set when using openai backend")
        return OpenAIEmbedder(
            model=config.embeddings_model,
            api_url=config.embeddings_api_url,
            api_key=config.embeddings_api_key,
            expected_dimension=config.embeddings_dimension,
            query_prefix=config.embeddings_query_prefix,
        )
    else:
        raise ValueError(f"Unsupported embeddings backend: {config.embeddings_backend!r}")


def make_search_backend(config: AumConfig, index: str | None = None) -> SearchBackend:
    if config.search_backend == "elasticsearch":
        return ElasticsearchBackend(url=config.es_url, index=index or config.es_index, rrf=config.es_rrf, max_highlight_offset=config.es_max_highlight_offset)
    raise ValueError(f"Unsupported search backend: {config.search_backend!r}")


def default_index_name(config: AumConfig) -> str:
    return config.es_index


def make_tracker(config: AumConfig) -> JobTracker:
    return JobTracker(db_path=config.db_path)


def make_local_auth(config: AumConfig) -> LocalAuth:
    return LocalAuth(_get_db(config))


def make_token_manager(config: AumConfig) -> TokenManager:
    return TokenManager(
        secret=config.jwt_secret,
        algorithm=config.jwt_algorithm,
        access_expire_minutes=config.access_token_expire_minutes,
        refresh_expire_days=config.refresh_token_expire_days,
    )


def make_permission_manager(config: AumConfig) -> PermissionManager:
    return PermissionManager(_get_db(config))


def make_oauth_manager(config: AumConfig) -> OAuthManager | None:
    if not config.oauth_providers:
        return None
    return OAuthManager(_get_db(config), config.oauth_providers)


# --- FastAPI dependency functions (no params, safe for injection) ---


def get_search_backend() -> SearchBackend:
    return make_search_backend(get_config())


def get_tracker() -> JobTracker:
    return make_tracker(get_config())


def get_local_auth() -> LocalAuth:
    return make_local_auth(get_config())


def get_token_manager() -> TokenManager:
    return make_token_manager(get_config())


def get_permission_manager() -> PermissionManager:
    return make_permission_manager(get_config())


def get_oauth_manager() -> OAuthManager | None:
    return make_oauth_manager(get_config())


async def get_current_user(
    credentials: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
) -> User:
    """Extract and verify the current user from the Authorization header."""
    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Not authenticated")

    token_mgr = get_token_manager()
    try:
        payload = token_mgr.verify_access_token(credentials.credentials)
    except TokenError as exc:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail=str(exc))

    user_id = int(payload["sub"])
    auth = get_local_auth()
    user = auth.get_user(user_id)
    if user is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="User not found")

    return user
