from __future__ import annotations

import math
from typing import Protocol


def l2_normalize(vector: list[float]) -> list[float]:
    """L2-normalize a vector."""
    norm = math.sqrt(sum(x * x for x in vector))
    if norm > 0:
        return [x / norm for x in vector]
    return vector


def l2_normalize_batch(vectors: list[list[float]]) -> list[list[float]]:
    return [l2_normalize(v) for v in vectors]


class Embedder(Protocol):
    @property
    def dimension(self) -> int:
        """Return the embedding vector dimension."""
        ...

    def embed_query(self, text: str) -> list[float]:
        """Embed a search query (with query prefix + L2 normalization)."""
        ...

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed document texts (with L2 normalization, no query prefix)."""
        ...
