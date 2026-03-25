from __future__ import annotations

import time

import structlog

from aum.metrics import EMBEDDING_DURATION

log = structlog.get_logger()


class SentenceTransformerEmbedder:
    """Compute embeddings using a local sentence-transformers model."""

    def __init__(self, model_name: str, expected_dimension: int) -> None:
        from sentence_transformers import SentenceTransformer

        log.info("loading embedding model", model=model_name)
        self._model = SentenceTransformer(model_name)
        self._dimension = expected_dimension

        # Verify dimension matches
        test = self._model.encode(["test"])
        actual_dim = len(test[0])
        if actual_dim != expected_dimension:
            log.warning(
                "embedding dimension mismatch",
                expected=expected_dimension,
                actual=actual_dim,
            )
            self._dimension = actual_dim

        log.info("embedding model loaded", model=model_name, dimension=self._dimension)

    @property
    def dimension(self) -> int:
        return self._dimension

    def embed(self, text: str) -> list[float]:
        return self.embed_batch([text])[0]

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        start = time.monotonic()
        embeddings = self._model.encode(texts, show_progress_bar=False)
        elapsed = time.monotonic() - start
        EMBEDDING_DURATION.observe(elapsed)
        log.debug("embedded batch", count=len(texts), elapsed=round(elapsed, 3))
        return [vec.tolist() for vec in embeddings]
