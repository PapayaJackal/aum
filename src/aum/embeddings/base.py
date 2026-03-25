from __future__ import annotations

from typing import Protocol


class Embedder(Protocol):
    @property
    def dimension(self) -> int:
        """Return the embedding vector dimension."""
        ...

    def embed(self, text: str) -> list[float]:
        """Embed a single text string."""
        ...

    def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts."""
        ...
