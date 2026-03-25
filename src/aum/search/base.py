from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from aum.models import Document


@dataclass
class SearchResult:
    doc_id: str
    source_path: str
    score: float
    snippet: str
    metadata: dict[str, str | list[str]] = field(default_factory=dict)


class SearchBackend(Protocol):
    def initialize(self, *, vector_dimension: int | None = None) -> None:
        """Create index/mappings. If vector_dimension is set, configure vector fields."""
        ...

    def index_document(self, doc_id: str, document: Document) -> None:
        """Index a single document."""
        ...

    def index_batch(self, documents: list[tuple[str, Document]]) -> list[tuple[str, str]]:
        """Index a batch of (doc_id, document) pairs. Returns list of (doc_id, error) for failures."""
        ...

    def search_text(self, query: str, *, limit: int = 20) -> list[SearchResult]:
        """Full-text keyword search."""
        ...

    def search_vector(self, vector: list[float], *, limit: int = 20) -> list[SearchResult]:
        """Vector similarity search (kNN)."""
        ...

    def search_hybrid(
        self, query: str, vector: list[float], *, limit: int = 20
    ) -> list[SearchResult]:
        """Combined keyword + vector search."""
        ...

    def delete_index(self) -> None:
        """Delete the entire index."""
        ...

    def document_count(self) -> int:
        """Return the number of documents in the index."""
        ...

    def get_document(self, doc_id: str) -> SearchResult | None:
        """Fetch a single document by its ID. Returns None if not found."""
        ...

    def list_indices(self) -> list[str]:
        """Return a list of available index names."""
        ...
