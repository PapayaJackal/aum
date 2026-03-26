from __future__ import annotations

import structlog
from elasticsearch import Elasticsearch, NotFoundError

from aum.metrics import INDEXES_RECREATED
from aum.models import Document
from aum.search.base import (
    DATE_FACETS,
    FACET_FIELDS,
    REVERSE_MIMETYPE_ALIASES,
    SearchResult,
    alias_mimetype,
    extract_email,
)

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Indexed metadata fields
#
# Only these curated fields are indexed in Elasticsearch.  The full Tika
# metadata blob is still stored (``enabled: false``) so nothing is lost, but
# we avoid exploding the dynamic-field count past ES's 1000-field limit.
# ---------------------------------------------------------------------------

_META_PROPERTIES: dict[str, dict] = {
    "content_type": {"type": "keyword"},
    "creator": {"type": "keyword"},
    "created": {
        "type": "date",
        "format": "strict_date_optional_time||epoch_millis",
        "ignore_malformed": True,
    },
    "modified": {
        "type": "date",
        "format": "strict_date_optional_time||epoch_millis",
        "ignore_malformed": True,
    },
    "email_addresses": {"type": "keyword"},
}

# For each canonical field, the Tika metadata keys to try (first match wins).
_META_SOURCE_KEYS: dict[str, list[str]] = {
    "content_type": ["Content-Type"],
    "creator": ["dc:creator", "xmp:dc:creator", "Author", "meta:author", "creator"],
    "created": ["dcterms:created", "Creation-Date", "meta:creation-date", "created", "date"],
    "modified": ["dcterms:modified", "Last-Modified", "meta:save-date", "modified"],
}

# Email header keys whose values are merged into a single ``email_addresses`` field.
_EMAIL_HEADER_KEYS = ["Message-From", "Message-To", "Message-CC"]


def _extract_indexed_meta(raw_metadata: dict[str, str | list[str]]) -> dict[str, str | list[str]]:
    """Pick the fields we care about out of the raw Tika metadata."""
    result: dict[str, str | list[str]] = {}
    for field_name, source_keys in _META_SOURCE_KEYS.items():
        for key in source_keys:
            if key in raw_metadata:
                result[field_name] = raw_metadata[key]
                break

    # Strip MIME type parameters (e.g. "; charset=UTF-8") so aggregations
    # group on the base type only.
    if "content_type" in result and isinstance(result["content_type"], str):
        result["content_type"] = result["content_type"].split(";")[0].strip()

    # Merge all email header values into a single list, extracting just the
    # email address (no display name) and normalising to lowercase.
    # Values that don't contain a valid address (e.g. "undisclosed-recipients:;")
    # are silently dropped.
    seen: set[str] = set()
    unique: list[str] = []
    for key in _EMAIL_HEADER_KEYS:
        val = raw_metadata.get(key)
        if val is None:
            continue
        raw_vals = val if isinstance(val, list) else [val]
        for rv in raw_vals:
            addr = extract_email(rv)
            if addr is not None and addr not in seen:
                seen.add(addr)
                unique.append(addr)
    if unique:
        result["email_addresses"] = unique

    return result


# ---------------------------------------------------------------------------
# Elasticsearch aggregations built from the shared facet definitions.
# ---------------------------------------------------------------------------

_FACET_AGGS: dict[str, dict] = {}
for _label, _es_field in FACET_FIELDS.items():
    if _label in DATE_FACETS:
        _FACET_AGGS[_label] = {
            "date_histogram": {
                "field": _es_field,
                "calendar_interval": "year",
                "format": "yyyy",
                "min_doc_count": 1,
            }
        }
    else:
        _FACET_AGGS[_label] = {"terms": {"field": _es_field, "size": 100}}


def _parse_facets(resp: dict) -> dict[str, list[str]]:
    result: dict[str, list[str]] = {}
    for label in FACET_FIELDS:
        buckets = resp.get("aggregations", {}).get(label, {}).get("buckets", [])
        if label in DATE_FACETS:
            values = [b["key_as_string"] for b in buckets if b.get("doc_count", 0) > 0]
        elif label == "File Type":
            values = sorted({alias_mimetype(b["key"]) for b in buckets if b.get("key")})
        else:
            values = sorted(b["key"] for b in buckets if b.get("key"))
        if values:
            result[label] = values
    return result


def _build_filter_clauses(filters: dict[str, list[str]]) -> list[dict]:
    """Convert facet display-name filters to Elasticsearch filter clauses."""
    clauses: list[dict] = []
    for label, values in filters.items():
        if not values:
            continue
        es_field = FACET_FIELDS.get(label)
        if es_field is None:
            continue
        if label in DATE_FACETS:
            # values = [min_year, max_year]
            range_filter: dict = {"format": "yyyy"}
            if len(values) >= 1:
                range_filter["gte"] = values[0]
            if len(values) >= 2:
                range_filter["lte"] = values[-1]
            clauses.append({"range": {es_field: range_filter}})
        elif label == "File Type":
            # Reverse-map human aliases back to raw MIME types
            raw_types: list[str] = []
            for alias in values:
                if alias in REVERSE_MIMETYPE_ALIASES:
                    raw_types.extend(REVERSE_MIMETYPE_ALIASES[alias])
                else:
                    raw_types.append(alias)
            clauses.append({"terms": {es_field: raw_types}})
        else:
            clauses.append({"terms": {es_field: values}})
    return clauses


class ElasticsearchBackend:
    """Search backend using Elasticsearch with optional kNN vector search."""

    def __init__(self, url: str = "http://localhost:9200", index: str = "aum") -> None:
        self._client = Elasticsearch(url)
        self._index = index
        self._vector_dimension: int | None = None

    def _build_mappings(self, vector_dimension: int | None) -> dict:
        mappings: dict = {
            "properties": {
                "source_path": {"type": "keyword"},
                "display_path": {"type": "keyword"},
                "extracted_from": {"type": "keyword"},
                "content": {"type": "text", "analyzer": "standard"},
                "metadata": {"type": "object", "enabled": False},
                "meta": {
                    "type": "object",
                    "dynamic": False,
                    "properties": _META_PROPERTIES,
                },
            }
        }

        if vector_dimension:
            mappings["properties"]["embedding"] = {
                "type": "dense_vector",
                "dims": vector_dimension,
                "index": True,
                "similarity": "cosine",
            }

        return mappings

    def _meta_mapping_matches(self) -> bool:
        """Check whether the existing index has the expected meta field mappings."""
        try:
            resp = self._client.indices.get_mapping(index=self._index)
            existing = resp[self._index]["mappings"]
            existing_meta = existing.get("properties", {}).get("meta", {}).get("properties", {})
            for field, expected_props in _META_PROPERTIES.items():
                actual = existing_meta.get(field, {})
                if actual.get("type") != expected_props.get("type"):
                    log.warning(
                        "meta field type mismatch",
                        field=field,
                        expected=expected_props.get("type"),
                        actual=actual.get("type"),
                    )
                    return False
            return True
        except Exception:
            log.warning("failed to check index mapping", index=self._index, exc_info=True)
            return False

    def initialize(self, *, vector_dimension: int | None = None) -> None:
        self._vector_dimension = vector_dimension

        if self._client.indices.exists(index=self._index):
            if self._meta_mapping_matches():
                log.info("elasticsearch index already exists with correct mapping", index=self._index)
                return
            log.warning(
                "elasticsearch index has stale mapping, recreating",
                index=self._index,
            )
            self._client.indices.delete(index=self._index)
            INDEXES_RECREATED.labels(index=self._index).inc()

        mappings = self._build_mappings(vector_dimension)

        self._client.indices.create(
            index=self._index,
            body={"mappings": mappings},
        )
        log.info("created elasticsearch index", index=self._index, vector=vector_dimension is not None)

    def index_document(self, doc_id: str, document: Document) -> None:
        body: dict = {
            "source_path": str(document.source_path),
            "display_path": document.metadata.get("_aum_display_path", ""),
            "extracted_from": document.metadata.get("_aum_extracted_from", ""),
            "content": document.content,
            "metadata": document.metadata,
            "meta": _extract_indexed_meta(document.metadata),
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
                "display_path": document.metadata.get("_aum_display_path", ""),
                "extracted_from": document.metadata.get("_aum_extracted_from", ""),
                "content": document.content,
                "metadata": document.metadata,
                "meta": _extract_indexed_meta(document.metadata),
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

    def search_text(self, query: str, *, limit: int = 20, offset: int = 0, include_facets: bool = False, filters: dict[str, list[str]] | None = None) -> tuple[list[SearchResult], int, dict[str, list[str]] | None]:
        match_clause: dict = {"match": {"content": {"query": query, "operator": "and"}}}
        filter_clauses = _build_filter_clauses(filters) if filters else []
        if filter_clauses:
            query_body: dict = {"bool": {"must": [match_clause], "filter": filter_clauses}}
        else:
            query_body = match_clause
        body: dict = {
            "query": query_body,
            "size": limit,
            "from": offset,
            "highlight": {
                "pre_tags": ["<mark>"],
                "post_tags": ["</mark>"],
                "max_analyzed_offset": 999_999,
                "fields": {"content": {"fragment_size": 200, "number_of_fragments": 1}},
            },
        }
        if include_facets:
            body["aggs"] = _FACET_AGGS
        resp = self._client.search(index=self._index, body=body)
        results, total = self._parse_hits(resp)
        facets = _parse_facets(resp) if include_facets else None
        return results, total, facets

    def search_vector(self, vector: list[float], *, limit: int = 20, offset: int = 0, include_facets: bool = False, filters: dict[str, list[str]] | None = None) -> tuple[list[SearchResult], int, dict[str, list[str]] | None]:
        filter_clauses = _build_filter_clauses(filters) if filters else []
        knn_body: dict = {
            "field": "embedding",
            "query_vector": vector,
            "k": limit,
            "num_candidates": limit * 5,
        }
        if filter_clauses:
            knn_body["filter"] = {"bool": {"filter": filter_clauses}}
        body: dict = {
            "knn": knn_body,
            "size": limit,
            "from": offset,
        }
        if include_facets:
            body["aggs"] = _FACET_AGGS
        resp = self._client.search(index=self._index, body=body)
        results, total = self._parse_hits(resp)
        facets = _parse_facets(resp) if include_facets else None
        return results, total, facets

    def search_hybrid(
        self, query: str, vector: list[float], *, limit: int = 20, offset: int = 0, include_facets: bool = False, filters: dict[str, list[str]] | None = None
    ) -> tuple[list[SearchResult], int, dict[str, list[str]] | None]:
        match_clause: dict = {"match": {"content": {"query": query, "operator": "and"}}}
        filter_clauses = _build_filter_clauses(filters) if filters else []
        if filter_clauses:
            query_body: dict = {"bool": {"must": [match_clause], "filter": filter_clauses}}
        else:
            query_body = match_clause
        knn_body: dict = {
            "field": "embedding",
            "query_vector": vector,
            "k": limit,
            "num_candidates": limit * 5,
        }
        if filter_clauses:
            knn_body["filter"] = {"bool": {"filter": filter_clauses}}
        body: dict = {
            "query": query_body,
            "knn": knn_body,
            "size": limit,
            "from": offset,
        }
        if include_facets:
            body["aggs"] = _FACET_AGGS
        resp = self._client.search(index=self._index, body=body)
        results, total = self._parse_hits(resp)
        facets = _parse_facets(resp) if include_facets else None
        return results, total, facets

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
            metadata = dict(source.get("metadata", {}))
            meta = source.get("meta", {})
            for label, es_field in FACET_FIELDS.items():
                meta_key = es_field.split(".", 1)[1]
                val = meta.get(meta_key)
                if val is None:
                    continue
                if label == "File Type" and isinstance(val, str):
                    val = alias_mimetype(val)
                elif label in DATE_FACETS and isinstance(val, str) and len(val) >= 4:
                    val = val[:4]
                metadata[label] = val
            return SearchResult(
                doc_id=hit["_id"],
                source_path=source.get("source_path", ""),
                display_path=source.get("display_path", ""),
                score=1.0,
                snippet=source.get("content", ""),
                metadata=metadata,
                extracted_from=source.get("extracted_from", ""),
            )
        except NotFoundError:
            return None

    def find_attachments(self, display_path: str) -> list[SearchResult]:
        """Find documents that were extracted from the given display_path."""
        body: dict = {
            "query": {"term": {"extracted_from": display_path}},
            "size": 200,
        }
        try:
            resp = self._client.search(index=self._index, body=body)
        except NotFoundError:
            return []
        results, _ = self._parse_hits(resp)
        return results

    def find_by_display_path(self, display_path: str) -> SearchResult | None:
        """Find a single document by exact display_path."""
        body: dict = {
            "query": {"term": {"display_path": display_path}},
            "size": 1,
        }
        try:
            resp = self._client.search(index=self._index, body=body)
        except NotFoundError:
            return None
        results, _ = self._parse_hits(resp)
        return results[0] if results else None

    def list_indices(self) -> list[str]:
        try:
            indices = self._client.indices.get(index="*")
            return sorted(name for name in indices if not name.startswith("."))
        except Exception:
            return []

    def _parse_hits(self, resp: dict) -> tuple[list[SearchResult], int]:
        hits = resp.get("hits", {})
        total_obj = hits.get("total", {})
        total = total_obj.get("value", 0) if isinstance(total_obj, dict) else int(total_obj)
        results: list[SearchResult] = []
        for hit in hits.get("hits", []):
            source = hit["_source"]
            metadata = dict(source.get("metadata", {}))
            # Inject the indexed meta fields under their facet-friendly label
            # names so client-side facet filtering can match on them.
            meta = source.get("meta", {})
            for label, es_field in FACET_FIELDS.items():
                meta_key = es_field.split(".", 1)[1]
                val = meta.get(meta_key)
                if val is None:
                    continue
                if label == "File Type" and isinstance(val, str):
                    val = alias_mimetype(val)
                elif label in DATE_FACETS and isinstance(val, str) and len(val) >= 4:
                    val = val[:4]
                metadata[label] = val
            highlight = hit.get("highlight", {}).get("content", [])
            snippet = highlight[0] if highlight else source.get("content", "")[:200]
            results.append(
                SearchResult(
                    doc_id=hit["_id"],
                    source_path=source.get("source_path", ""),
                    display_path=source.get("display_path", ""),
                    score=hit.get("_score", 0.0),
                    snippet=snippet,
                    metadata=metadata,
                    extracted_from=source.get("extracted_from", ""),
                )
            )
        return results, total
