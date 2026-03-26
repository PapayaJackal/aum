from __future__ import annotations

import time

import httpx
import structlog

from aum.embeddings.base import l2_normalize, l2_normalize_batch
from aum.metrics import EMBEDDING_DURATION, EMBEDDING_REQUESTS

log = structlog.get_logger()


class OllamaEmbedder:
    """Compute embeddings using an Ollama server."""

    def __init__(
        self,
        model: str = "snowflake-arctic-embed2",
        base_url: str = "http://localhost:11434",
        expected_dimension: int = 1024,
        context_length: int = 8192,
        query_prefix: str = "query: ",
        timeout: float = 600.0,
    ) -> None:
        self._model = model
        self._base_url = base_url.rstrip("/")
        self._dimension = expected_dimension
        self._context_length = context_length
        self._query_prefix = query_prefix
        self._client = httpx.Client(timeout=timeout)
        log.info("ollama embedder configured", model=model, base_url=base_url, num_ctx=context_length)

    @property
    def dimension(self) -> int:
        return self._dimension

    def ensure_model(self) -> None:
        """Pull the model if it is not already available locally."""
        log.info("ensuring ollama model is available", model=self._model)
        resp = self._client.post(
            f"{self._base_url}/api/pull",
            json={"name": self._model, "stream": False},
            timeout=600.0,
        )
        resp.raise_for_status()
        log.info("ollama model ready", model=self._model)

    def embed_query(self, text: str) -> list[float]:
        """Embed a search query with the query prefix and L2 normalization."""
        return l2_normalize(self._embed_raw([self._query_prefix + text])[0])

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed document texts with L2 normalization (no prefix)."""
        return l2_normalize_batch(self._embed_raw(texts))

    def _embed_raw(self, texts: list[str]) -> list[list[float]]:
        start = time.monotonic()
        EMBEDDING_REQUESTS.labels(backend="ollama").inc()

        resp = self._client.post(
            f"{self._base_url}/api/embed",
            json={
                "model": self._model,
                "input": texts,
                "options": {"num_ctx": self._context_length},
            },
        )
        resp.raise_for_status()
        data = resp.json()
        embeddings: list[list[float]] = data["embeddings"]

        elapsed = time.monotonic() - start
        EMBEDDING_DURATION.labels(backend="ollama").observe(elapsed)
        log.debug("ollama embedded batch", count=len(texts), elapsed=round(elapsed, 3))

        # Verify dimension on first call
        if embeddings and len(embeddings[0]) != self._dimension:
            actual = len(embeddings[0])
            log.warning(
                "embedding dimension mismatch",
                expected=self._dimension,
                actual=actual,
            )
            self._dimension = actual

        return embeddings
