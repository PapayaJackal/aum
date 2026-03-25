from __future__ import annotations

import structlog
from elasticsearch import Elasticsearch, NotFoundError

from aum.models import Document
from aum.search.base import SearchResult

log = structlog.get_logger()


class ElasticsearchBackend:
    """Search backend using Elasticsearch with optional kNN vector search."""

    def __init__(self, url: str = "http://localhost:9200", index: str = "aum") -> None:
        self._client = Elasticsearch(url)
        self._index = index
        self._vector_dimension: int | None = None

    def initialize(self, *, vector_dimension: int | None = None) -> None:
        self._vector_dimension = vector_dimension

        if self._client.indices.exists(index=self._index):
            log.info("elasticsearch index already exists", index=self._index)
            return

        mappings: dict = {
            "properties": {
                "source_path": {"type": "keyword"},
                "content": {"type": "text", "analyzer": "standard"},
                "metadata": {"type": "object", "dynamic": False},
            }
        }

        if vector_dimension:
            mappings["properties"]["embedding"] = {
                "type": "dense_vector",
                "dims": vector_dimension,
                "index": True,
                "similarity": "cosine",
            }

        self._client.indices.create(
            index=self._index,
            body={"mappings": mappings},
        )
        log.info("created elasticsearch index", index=self._index, vector=vector_dimension is not None)

    def index_document(self, doc_id: str, document: Document) -> None:
        body: dict = {
            "source_path": str(document.source_path),
            "content": document.content,
            "metadata": document.metadata,
        }
        if document.embedding is not None:
            body["embedding"] = document.embedding

        self._client.index(index=self._index, id=doc_id, body=body)

    def index_batch(self, documents: list[tuple[str, Document]]) -> list[tuple[str, str]]:
        """Index a batch of documents. Returns a list of (doc_id, error_reason) for any failures."""
        if not documents:
            return []

        operations: list[dict] = []
        for doc_id, document in documents:
            operations.append({"index": {"_index": self._index, "_id": doc_id}})
            body: dict = {
                "source_path": str(document.source_path),
                "content": document.content,
                "metadata": document.metadata,
            }
            if document.embedding is not None:
                body["embedding"] = document.embedding
            operations.append(body)

        resp = self._client.bulk(operations=operations)
        if not resp.get("errors"):
            return []

        failures: list[tuple[str, str]] = []
        for item in resp["items"]:
            index_result = item.get("index", {})
            if "error" in index_result:
                error = index_result["error"]
                reason = f"{error.get('type', 'unknown')}: {error.get('reason', 'unknown')}"
                failures.append((index_result.get("_id", "unknown"), reason))

        log.warning("elasticsearch bulk indexing had errors", failed_count=len(failures))
        return failures

    def search_text(self, query: str, *, limit: int = 20) -> list[SearchResult]:
        resp = self._client.search(
            index=self._index,
            body={
                "query": {"match": {"content": query}},
                "size": limit,
                "highlight": {"fields": {"content": {"fragment_size": 200, "number_of_fragments": 1}}},
            },
        )
        return self._parse_hits(resp)

    def search_vector(self, vector: list[float], *, limit: int = 20) -> list[SearchResult]:
        resp = self._client.search(
            index=self._index,
            body={
                "knn": {
                    "field": "embedding",
                    "query_vector": vector,
                    "k": limit,
                    "num_candidates": limit * 5,
                },
                "size": limit,
            },
        )
        return self._parse_hits(resp)

    def search_hybrid(
        self, query: str, vector: list[float], *, limit: int = 20
    ) -> list[SearchResult]:
        resp = self._client.search(
            index=self._index,
            body={
                "query": {"match": {"content": query}},
                "knn": {
                    "field": "embedding",
                    "query_vector": vector,
                    "k": limit,
                    "num_candidates": limit * 5,
                },
                "size": limit,
            },
        )
        return self._parse_hits(resp)

    def delete_index(self) -> None:
        try:
            self._client.indices.delete(index=self._index)
            log.info("deleted elasticsearch index", index=self._index)
        except NotFoundError:
            log.info("elasticsearch index not found, nothing to delete", index=self._index)

    def document_count(self) -> int:
        try:
            resp = self._client.count(index=self._index)
            return resp["count"]
        except NotFoundError:
            return 0

    def get_document(self, doc_id: str) -> SearchResult | None:
        try:
            hit = self._client.get(index=self._index, id=doc_id)
            source = hit["_source"]
            return SearchResult(
                doc_id=hit["_id"],
                source_path=source.get("source_path", ""),
                score=1.0,
                snippet=source.get("content", ""),
                metadata=source.get("metadata", {}),
            )
        except NotFoundError:
            return None

    def list_indices(self) -> list[str]:
        try:
            indices = self._client.indices.get(index="*")
            return sorted(name for name in indices if not name.startswith("."))
        except Exception:
            return []

    def _parse_hits(self, resp: dict) -> list[SearchResult]:
        results: list[SearchResult] = []
        for hit in resp.get("hits", {}).get("hits", []):
            source = hit["_source"]
            highlight = hit.get("highlight", {}).get("content", [])
            snippet = highlight[0] if highlight else source.get("content", "")[:200]
            results.append(
                SearchResult(
                    doc_id=hit["_id"],
                    source_path=source.get("source_path", ""),
                    score=hit.get("_score", 0.0),
                    snippet=snippet,
                    metadata=source.get("metadata", {}),
                )
            )
        return results
