from __future__ import annotations

import time

import httpx
import structlog

from aum.embeddings.base import l2_normalize, l2_normalize_batch
from aum.metrics import EMBEDDING_DURATION, EMBEDDING_REQUESTS

log = structlog.get_logger()


class OpenAIEmbedder:
    """Compute embeddings using an OpenAI-compatible API.

    Works with OpenAI, Azure OpenAI, and any provider that implements
    the ``POST /v1/embeddings`` endpoint (e.g. vLLM, LiteLLM, Together).
    """

    def __init__(
        self,
        model: str,
        api_url: str,
        api_key: str = "",
        expected_dimension: int = 1024,
        query_prefix: str = "query: ",
        timeout: float = 120.0,
    ) -> None:
        self._model = model
        self._api_url = api_url.rstrip("/")
        self._dimension = expected_dimension
        self._query_prefix = query_prefix
        headers: dict[str, str] = {"Content-Type": "application/json"}
        if api_key:
            headers["Authorization"] = f"Bearer {api_key}"
        self._client = httpx.Client(timeout=timeout, headers=headers)
        log.info("openai embedder configured", model=model, api_url=api_url)

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed_query(self, text: str) -> list[float]:
        """Embed a search query with the query prefix and L2 normalization."""
        return l2_normalize(self._embed_raw([self._query_prefix + text])[0])

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed document texts with L2 normalization (no prefix)."""
        return l2_normalize_batch(self._embed_raw(texts))

    def _embed_raw(self, texts: list[str]) -> list[list[float]]:
        start = time.monotonic()
        EMBEDDING_REQUESTS.labels(backend="openai").inc()

        resp = self._client.post(
            f"{self._api_url}/v1/embeddings",
            json={"model": self._model, "input": texts},
        )
        resp.raise_for_status()
        data = resp.json()

        # Sort by index to guarantee order matches input
        sorted_data = sorted(data["data"], key=lambda x: x["index"])
        embeddings: list[list[float]] = [item["embedding"] for item in sorted_data]

        elapsed = time.monotonic() - start
        EMBEDDING_DURATION.labels(backend="openai").observe(elapsed)
        log.debug("openai embedded batch", count=len(texts), elapsed=round(elapsed, 3))

        if embeddings and len(embeddings[0]) != self._dimension:
            actual = len(embeddings[0])
            log.warning(
                "embedding dimension mismatch",
                expected=self._dimension,
                actual=actual,
            )
            self._dimension = actual

        return embeddings
