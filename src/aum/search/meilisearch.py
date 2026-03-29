from __future__ import annotations

import json
import time

import meilisearch
import meilisearch.errors
import structlog

from aum.metrics import DOCS_TRUNCATED, SEARCH_LATENCY
from aum.models import Document
from aum.search.base import (
    DATE_FACETS,
    REVERSE_MIMETYPE_ALIASES,
    BatchResult,
    SearchResult,
    alias_mimetype,
    extract_email,
)

log = structlog.get_logger()

# ---------------------------------------------------------------------------
# Field name mappings – Meilisearch uses flat field names (no dots allowed)
# ---------------------------------------------------------------------------

# Maps our display label → Meilisearch filterable field name
_MEILI_FACET_FIELDS: dict[str, str] = {
    "Created": "meta_created_year",
    "File Type": "meta_content_type",
    "Creator": "meta_creator",
    "Email Addresses": "meta_email_addresses",
}

_FILTERABLE_ATTRS: list[str] = [
    "id",
    "meta_content_type",
    "meta_creator",
    "meta_created_year",
    "meta_email_addresses",
    "has_embeddings",
    "extracted_from",
    "display_path",
]

# Only these fields are searched; all others are stored but not ranked on.
_SEARCHABLE_ATTRS: list[str] = ["display_path", "content"]

# ---------------------------------------------------------------------------
# Indexed metadata extraction – mirrors elasticsearch.py exactly
# ---------------------------------------------------------------------------

_META_SOURCE_KEYS: dict[str, list[str]] = {
    "content_type": ["Content-Type"],
    "creator": ["dc:creator", "xmp:dc:creator", "Author", "meta:author", "creator"],
    "created": ["dcterms:created", "Creation-Date", "meta:creation-date", "created", "date"],
    "modified": ["dcterms:modified", "Last-Modified", "meta:save-date", "modified"],
}

_EMAIL_HEADER_KEYS: list[str] = ["Message-From", "Message-To", "Message-CC"]

# Generous budget for large batch operations (50+ documents).
_TASK_TIMEOUT_MS: int = 60_000
# Larger budget for embedding updates: each doc may have many chunk vectors.
_EMBED_TASK_TIMEOUT_MS: int = 300_000

# Meilisearch default payload limit is 100 MB.  Stay safely under it.
_MAX_PAYLOAD_BYTES: int = 95 * 1024 * 1024


def _extract_indexed_meta(raw_metadata: dict[str, str | list[str]]) -> dict[str, str | list[str]]:
    """Pick the curated fields out of raw Tika metadata.

    Mirrors the same function in elasticsearch.py so both backends index
    identical data from the same source documents.
    """
    result: dict[str, str | list[str]] = {}
    for field_name, source_keys in _META_SOURCE_KEYS.items():
        for key in source_keys:
            if key in raw_metadata:
                result[field_name] = raw_metadata[key]
                break

    # Strip MIME type parameters (e.g. "; charset=UTF-8") so facets group on base type only.
    if "content_type" in result and isinstance(result["content_type"], str):
        result["content_type"] = result["content_type"].split(";")[0].strip()

    # Merge all email header values into a deduplicated lowercase address list.
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


def _build_flat_meta(meta: dict[str, str | list[str]]) -> dict[str, object]:
    """Convert the extracted meta dict to Meilisearch flat field names."""
    flat: dict[str, object] = {}
    if "content_type" in meta:
        flat["meta_content_type"] = meta["content_type"]
    if "creator" in meta:
        flat["meta_creator"] = meta["creator"]
    if "email_addresses" in meta:
        flat["meta_email_addresses"] = meta["email_addresses"]
    # Store only the year for date faceting / filtering (matches ES yearly histogram).
    # Must be an integer: Meilisearch range operators (>=, <=) only work on numeric fields.
    if "created" in meta and isinstance(meta["created"], str) and len(meta["created"]) >= 4:
        try:
            flat["meta_created_year"] = int(meta["created"][:4])
        except ValueError:
            pass
    return flat


# ---------------------------------------------------------------------------
# Filter / facet helpers
# ---------------------------------------------------------------------------


def _escape_filter_value(value: str) -> str:
    """Escape a string for use inside a Meilisearch quoted filter value."""
    return value.replace("\\", "\\\\").replace('"', '\\"')


def _build_filter_string(filters: dict[str, list[str]]) -> str:
    """Convert facet display-name filters to a Meilisearch filter expression."""
    clauses: list[str] = []
    for label, values in filters.items():
        if not values:
            continue
        meili_field = _MEILI_FACET_FIELDS.get(label)
        if meili_field is None:
            continue
        if label in DATE_FACETS:
            # values = [min_year, max_year] — no quotes: meta_created_year is numeric.
            parts: list[str] = []
            if len(values) >= 1:
                parts.append(f"{meili_field} >= {values[0]}")
            if len(values) >= 2:
                parts.append(f"{meili_field} <= {values[-1]}")
            if parts:
                clauses.append(" AND ".join(parts))
        elif label == "File Type":
            # Reverse-map human aliases back to raw MIME types
            raw_types: list[str] = []
            for alias in values:
                if alias in REVERSE_MIMETYPE_ALIASES:
                    raw_types.extend(REVERSE_MIMETYPE_ALIASES[alias])
                else:
                    raw_types.append(alias)
            if raw_types:
                vals_str = ", ".join(f'"{_escape_filter_value(v)}"' for v in raw_types)
                clauses.append(f"{meili_field} IN [{vals_str}]")
        else:
            vals_str = ", ".join(f'"{_escape_filter_value(v)}"' for v in values)
            clauses.append(f"{meili_field} IN [{vals_str}]")
    return " AND ".join(clauses)


def _parse_facet_distribution(distribution: dict[str, dict[str, int]]) -> dict[str, list[str]]:
    """Convert Meilisearch ``facetDistribution`` to our ``{label: [values]}`` format."""
    result: dict[str, list[str]] = {}
    for label, meili_field in _MEILI_FACET_FIELDS.items():
        buckets = distribution.get(meili_field, {})
        if not buckets:
            continue
        if label in DATE_FACETS:
            values = sorted(k for k, v in buckets.items() if v > 0)
        elif label == "File Type":
            values = sorted({alias_mimetype(k) for k, v in buckets.items() if k and v > 0})
        else:
            values = sorted(k for k, v in buckets.items() if k and v > 0)
        if values:
            result[label] = values
    return result


# ---------------------------------------------------------------------------
# Document building / hit parsing
# ---------------------------------------------------------------------------


def _estimate_doc_size(body: dict) -> int:
    """Cheaply estimate the JSON payload size of a document body.

    The *content* and *metadata_json* fields dominate the payload for ingested
    documents; for embedding updates, the *_vectors* field dominates.  We use
    ``json.dumps`` on the large string fields (not the whole dict) to account
    for JSON escaping of quotes, backslashes, and control characters.
    """
    size = 512  # conservative overhead for keys, small fields, JSON syntax
    for key in ("content", "metadata_json"):
        val = body.get(key)
        if val:
            # json.dumps on a plain string is fast and accounts for escaping.
            size += len(json.dumps(val))
    # Embedding vectors: each float serialises to ~18 chars on average.
    vectors = body.get("_vectors")
    if isinstance(vectors, dict):
        custom = vectors.get("custom")
        if isinstance(custom, dict):
            embeddings = custom.get("embeddings")
            if isinstance(embeddings, list):
                for vec in embeddings:
                    if isinstance(vec, list):
                        size += len(vec) * 18
    return size


def _truncate_oversized(item: dict, max_bytes: int) -> tuple[dict, tuple[str, int, int] | None]:
    """Truncate the *content* field of a document body so it fits under *max_bytes*.

    Returns ``(item, truncation_info)`` where *truncation_info* is
    ``(doc_id, original_chars, truncated_chars)`` if truncation occurred, or
    ``None`` if the document was left unchanged.
    """
    content = item.get("content")
    if not content:
        return item, None
    # Compute how much space everything *except* content occupies.
    content_json_size = len(json.dumps(content))
    overhead = _estimate_doc_size(item) - content_json_size
    budget = max_bytes - overhead
    if budget >= content_json_size:
        return item, None
    if budget <= 0:
        return item, None
    # Use the escaping ratio to estimate how many raw chars fit in the budget.
    # This avoids expensive repeated json.dumps on large strings.
    ratio = content_json_size / len(content)
    allowed_chars = int(budget / ratio)
    # Clamp and do one verification pass — trim further if still over.
    allowed_chars = min(allowed_chars, len(content))
    while allowed_chars > 0 and len(json.dumps(content[:allowed_chars])) > budget:
        allowed_chars = int(allowed_chars * 0.95)
    doc_id = item.get("id", "unknown")
    log.warning(
        "truncating oversized document content to fit payload limit",
        doc_id=doc_id,
        original_chars=len(content),
        truncated_chars=allowed_chars,
    )
    DOCS_TRUNCATED.inc()
    return {**item, "content": content[:allowed_chars]}, (doc_id, len(content), allowed_chars)


def _split_by_payload_size(
    items: list[dict], max_bytes: int = _MAX_PAYLOAD_BYTES
) -> tuple[list[list[dict]], list[tuple[str, int, int]]]:
    """Split a list of document dicts into sub-batches that stay under *max_bytes*.

    Uses :func:`_estimate_doc_size` for a cheap size estimate instead of
    serialising every document.  If a single document would still exceed the
    limit on its own, its *content* field is truncated to fit.

    Returns ``(sub_batches, truncations)`` where *truncations* is a list of
    ``(doc_id, original_chars, truncated_chars)`` for any truncated documents.
    """
    batches: list[list[dict]] = []
    truncations: list[tuple[str, int, int]] = []
    current: list[dict] = []
    current_size = 0

    for item in items:
        item_size = _estimate_doc_size(item)
        if item_size > max_bytes:
            item, trunc_info = _truncate_oversized(item, max_bytes)
            if trunc_info:
                truncations.append(trunc_info)
            item_size = _estimate_doc_size(item)
        if current and current_size + item_size > max_bytes:
            batches.append(current)
            current = []
            current_size = 0
        current.append(item)
        current_size += item_size

    if current:
        batches.append(current)
    return batches, truncations


def _build_doc_body(doc_id: str, document: Document) -> dict:
    """Format a Document as a Meilisearch document dict."""
    meta = _extract_indexed_meta(document.metadata)
    flat_meta = _build_flat_meta(meta)
    return {
        "id": doc_id,
        "source_path": str(document.source_path),
        "display_path": document.metadata.get("_aum_display_path", ""),
        "extracted_from": document.metadata.get("_aum_extracted_from", ""),
        "content": document.content,
        # Stored as a JSON string so Meilisearch doesn't index arbitrary Tika keys.
        "metadata_json": json.dumps(dict(document.metadata)),
        "has_embeddings": False,
        # Explicit opt-out so Meilisearch doesn't error when the embedder is added
        # to an existing index via update_settings – see update_embeddings() for format.
        "_vectors": {"custom": None},
        **flat_meta,
    }


def _parse_hit(hit: dict, *, index_name: str = "") -> SearchResult:
    """Convert a Meilisearch hit dict into a SearchResult."""
    formatted = hit.get("_formatted", {})

    # Highlighted + cropped content snippet, or first 200 chars as fallback.
    content_hl = formatted.get("content", "")
    snippet = content_hl if content_hl else hit.get("content", "")[:200]

    # Highlighted display path (only present for text / hybrid searches).
    display_path_highlighted = formatted.get("display_path", "")

    # Reconstruct raw metadata from the stored JSON string.
    meta_json = hit.get("metadata_json", "{}")
    try:
        raw_metadata: dict = json.loads(meta_json) if isinstance(meta_json, str) else dict(meta_json)
    except (json.JSONDecodeError, TypeError):
        raw_metadata = {}

    # Inject indexed meta under facet-friendly label names so client-side
    # facet filtering can match on them – mirrors the ES backend's _parse_hits.
    if "meta_content_type" in hit:
        val = hit["meta_content_type"]
        raw_metadata["File Type"] = alias_mimetype(val) if isinstance(val, str) else val
    if "meta_creator" in hit:
        raw_metadata["Creator"] = hit["meta_creator"]
    if "meta_created_year" in hit:
        raw_metadata["Created"] = str(hit["meta_created_year"])
    if "meta_email_addresses" in hit:
        raw_metadata["Email Addresses"] = hit["meta_email_addresses"]

    return SearchResult(
        doc_id=hit["id"],
        source_path=hit.get("source_path", ""),
        display_path=hit.get("display_path", ""),
        score=hit.get("_rankingScore", 0.0),
        snippet=snippet,
        metadata=raw_metadata,
        extracted_from=hit.get("extracted_from", ""),
        display_path_highlighted=display_path_highlighted,
        index=index_name,
    )


# ---------------------------------------------------------------------------
# Backend class
# ---------------------------------------------------------------------------


class MeilisearchBackend:
    """Search backend using Meilisearch with optional vector/hybrid search."""

    def __init__(
        self,
        url: str = "http://localhost:7700",
        api_key: str = "",
        index: str = "aum",
        *,
        semantic_ratio: float = 0.75,
        crop_length: int = 50,
    ) -> None:
        self._client = meilisearch.Client(url, api_key or None)
        self._index_name = index
        # Support comma-separated multi-index (same contract as ElasticsearchBackend)
        self._indices = [i.strip() for i in index.split(",") if i.strip()]
        self._vector_dimension: int | None = None
        self._semantic_ratio = semantic_ratio
        self._crop_length = crop_length

    def _idx(self, name: str | None = None) -> meilisearch.index.Index:
        return self._client.index(name or self._indices[0])

    def _wait(self, task_uid: int) -> None:
        self._client.wait_for_task(task_uid, timeout_in_ms=_TASK_TIMEOUT_MS)

    def _desired_settings(self, vector_dimension: int | None) -> dict:
        settings: dict = {
            "searchableAttributes": _SEARCHABLE_ATTRS,
            "filterableAttributes": _FILTERABLE_ATTRS,
            "displayedAttributes": ["*"],
            "rankingRules": ["words", "typo", "proximity", "attribute", "sort", "exactness"],
        }
        if vector_dimension:
            settings["embedders"] = {
                "custom": {
                    "source": "userProvided",
                    "dimensions": vector_dimension,
                }
            }
        return settings

    def initialize(self, *, vector_dimension: int | None = None) -> None:
        """Ensure the index exists and has the required settings.

        Unlike the Elasticsearch backend, Meilisearch settings (filterable
        attributes, embedders) can be updated in-place without recreating the
        index or losing any documents.  We never delete an existing index here —
        only create one if it doesn't exist yet.
        """
        self._vector_dimension = vector_dimension
        index_name = self._indices[0]

        try:
            self._client.get_index(index_name)
            log.info("meilisearch index exists, updating settings", index=index_name)
        except meilisearch.errors.MeilisearchApiError as exc:
            if exc.code != "index_not_found":
                raise
            task = self._client.create_index(index_name, {"primaryKey": "id"})
            self._wait(task.task_uid)
            log.info("created meilisearch index", index=index_name)

        task = self._idx(index_name).update_settings(self._desired_settings(vector_dimension))
        self._wait(task.task_uid)
        log.info("meilisearch index settings applied", index=index_name, vector=vector_dimension is not None)

    def index_document(self, doc_id: str, document: Document) -> None:
        body = _build_doc_body(doc_id, document)
        task = self._idx().add_documents([body], primary_key="id")
        self._wait(task.task_uid)

    def index_batch(self, documents: list[tuple[str, Document]]) -> BatchResult:
        """Index a batch of documents."""
        if not documents:
            return BatchResult()
        bodies = [_build_doc_body(doc_id, doc) for doc_id, doc in documents]
        # Map body back to doc_id so we can report per-sub-batch failures.
        id_by_index = {i: doc_id for i, (doc_id, _) in enumerate(documents)}

        sub_batches, truncations = _split_by_payload_size(bodies)
        if len(sub_batches) > 1:
            log.info(
                "splitting indexing batch into sub-batches to stay under payload limit",
                original=len(bodies),
                sub_batches=len(sub_batches),
            )

        failures: list[tuple[str, str]] = []
        offset = 0
        for sub in sub_batches:
            try:
                task = self._idx().add_documents(sub, primary_key="id")
                result = self._client.wait_for_task(task.task_uid, timeout_in_ms=_TASK_TIMEOUT_MS)
                if result.status == "failed":
                    error = result.error or {}
                    reason = f"{error.get('code', 'unknown')}: {error.get('message', 'unknown')}"
                    log.warning("meilisearch sub-batch indexing failed", reason=reason, count=len(sub))
                    failures.extend((id_by_index[offset + j], reason) for j in range(len(sub)))
            except Exception:
                log.exception("meilisearch sub-batch indexing exception", n_docs=len(sub))
                failures.extend((id_by_index[offset + j], "exception during indexing") for j in range(len(sub)))
            offset += len(sub)
        return BatchResult(failures=failures, truncated=truncations)

    # ---------------------------------------------------------------------------
    # Search
    # ---------------------------------------------------------------------------

    def _common_search_params(
        self,
        *,
        limit: int,
        offset: int,
        include_facets: bool,
        filters: dict[str, list[str]] | None,
        highlight: bool,
    ) -> dict:
        params: dict = {
            "limit": limit,
            "offset": offset,
            "showRankingScore": True,
            # Always crop content so every search type (text, vector, hybrid) returns
            # a usable snippet rather than full document text.
            "attributesToCrop": ["content"],
            "cropLength": self._crop_length,
            "cropMarker": "...",
        }
        if highlight:
            params.update(
                {
                    "attributesToHighlight": ["content", "display_path"],
                    "highlightPreTag": "<mark>",
                    "highlightPostTag": "</mark>",
                }
            )
        filter_str = _build_filter_string(filters) if filters else ""
        if filter_str:
            params["filter"] = filter_str
        if include_facets:
            params["facets"] = list(_MEILI_FACET_FIELDS.values())
        return params

    def _execute_search(
        self,
        query: str,
        params: dict,
    ) -> tuple[list[SearchResult], int, dict[str, dict[str, int]]]:
        """Run the search across all configured indices and merge the results."""
        all_results: list[SearchResult] = []
        total = 0
        merged_facets: dict[str, dict[str, int]] = {}

        for idx_name in self._indices:
            resp = self._idx(idx_name).search(query, params)
            hits = resp.get("hits", [])
            # Use totalHits (paginated) or estimatedTotalHits (offset/limit).
            total += resp.get("totalHits", resp.get("estimatedTotalHits", len(hits)))
            for k, v in resp.get("facetDistribution", {}).items():
                for val, count in v.items():
                    merged_facets.setdefault(k, {})[val] = merged_facets.get(k, {}).get(val, 0) + count
            for hit in hits:
                all_results.append(_parse_hit(hit, index_name=idx_name))

        if len(self._indices) > 1:
            all_results.sort(key=lambda r: r.score, reverse=True)

        return all_results, total, merged_facets

    def search_text(
        self,
        query: str,
        *,
        limit: int = 20,
        offset: int = 0,
        include_facets: bool = False,
        filters: dict[str, list[str]] | None = None,
    ) -> tuple[list[SearchResult], int, dict[str, list[str]] | None]:
        params = self._common_search_params(
            limit=limit,
            offset=offset,
            include_facets=include_facets,
            filters=filters,
            highlight=True,
        )
        t0 = time.monotonic()
        results, total, merged_facets = self._execute_search(query, params)
        SEARCH_LATENCY.labels(type="text", backend="meilisearch").observe(time.monotonic() - t0)
        facets = _parse_facet_distribution(merged_facets) if include_facets else None
        return results, total, facets

    def search_vector(
        self,
        vector: list[float],
        *,
        limit: int = 20,
        offset: int = 0,
        include_facets: bool = False,
        filters: dict[str, list[str]] | None = None,
    ) -> tuple[list[SearchResult], int, dict[str, list[str]] | None]:
        params = self._common_search_params(
            limit=limit,
            offset=offset,
            include_facets=include_facets,
            filters=filters,
            highlight=False,
        )
        params["vector"] = vector
        params["hybrid"] = {"semanticRatio": 1.0, "embedder": "custom"}
        t0 = time.monotonic()
        results, total, merged_facets = self._execute_search("", params)
        SEARCH_LATENCY.labels(type="vector", backend="meilisearch").observe(time.monotonic() - t0)
        facets = _parse_facet_distribution(merged_facets) if include_facets else None
        return results, total, facets

    def search_hybrid(
        self,
        query: str,
        vector: list[float],
        *,
        limit: int = 20,
        offset: int = 0,
        include_facets: bool = False,
        filters: dict[str, list[str]] | None = None,
        semantic_ratio: float | None = None,
    ) -> tuple[list[SearchResult], int, dict[str, list[str]] | None]:
        params = self._common_search_params(
            limit=limit,
            offset=offset,
            include_facets=include_facets,
            filters=filters,
            highlight=True,
        )
        params["vector"] = vector
        ratio = semantic_ratio if semantic_ratio is not None else self._semantic_ratio
        params["hybrid"] = {"semanticRatio": ratio, "embedder": "custom"}
        t0 = time.monotonic()
        results, total, merged_facets = self._execute_search(query, params)
        SEARCH_LATENCY.labels(type="hybrid", backend="meilisearch").observe(time.monotonic() - t0)
        facets = _parse_facet_distribution(merged_facets) if include_facets else None
        return results, total, facets

    # ---------------------------------------------------------------------------
    # Document operations
    # ---------------------------------------------------------------------------

    def delete_index(self) -> None:
        index_name = self._indices[0]
        try:
            task = self._client.delete_index(index_name)
            self._wait(task.task_uid)
            log.info("deleted meilisearch index", index=index_name)
        except meilisearch.errors.MeilisearchApiError as exc:
            if exc.code == "index_not_found":
                log.info("meilisearch index not found, nothing to delete", index=index_name)
            else:
                raise

    def document_count(self) -> int:
        try:
            return self._idx().get_stats().number_of_documents
        except meilisearch.errors.MeilisearchApiError as exc:
            if exc.code == "index_not_found":
                return 0
            raise

    def get_document(self, doc_id: str) -> SearchResult | None:
        try:
            doc = self._idx().get_document(doc_id)
            hit = dict(doc)
            result = _parse_hit(hit, index_name=self._indices[0])
            # Return full content (not cropped) for the document detail view.
            result.snippet = hit.get("content", "")
            return result
        except meilisearch.errors.MeilisearchApiError as exc:
            if exc.code == "document_not_found":
                return None
            raise

    def find_attachments(self, display_path: str) -> list[SearchResult]:
        """Find documents that were extracted from the given display_path."""
        params: dict = {
            "filter": f'extracted_from = "{_escape_filter_value(display_path)}"',
            "limit": 200,
            "showRankingScore": True,
        }
        results: list[SearchResult] = []
        for idx_name in self._indices:
            resp = self._idx(idx_name).search("", params)
            for hit in resp.get("hits", []):
                results.append(_parse_hit(hit, index_name=idx_name))
        return results

    def find_by_display_path(self, display_path: str) -> SearchResult | None:
        """Find a single document by exact display_path."""
        params: dict = {
            "filter": f'display_path = "{_escape_filter_value(display_path)}"',
            "limit": 1,
            "showRankingScore": True,
        }
        for idx_name in self._indices:
            resp = self._idx(idx_name).search("", params)
            hits = resp.get("hits", [])
            if hits:
                return _parse_hit(hits[0], index_name=idx_name)
        return None

    def list_indices(self) -> list[str]:
        try:
            resp = self._client.get_indexes({"limit": 1000})
            return sorted(idx.uid for idx in resp.get("results", []) if not idx.uid.startswith("."))
        except Exception:
            log.exception("failed to list meilisearch indices")
            return []

    # ---------------------------------------------------------------------------
    # Embeddings
    # ---------------------------------------------------------------------------

    def count_unembedded(self) -> int:
        """Return the number of documents without chunk embeddings."""
        try:
            resp = self._idx().search(
                "",
                {
                    "filter": "has_embeddings = false",
                    "limit": 1,
                    "attributesToRetrieve": [],
                },
            )
            return resp.get("estimatedTotalHits", 0)
        except meilisearch.errors.MeilisearchApiError as exc:
            if exc.code == "index_not_found":
                return 0
            raise

    def scroll_unembedded(self, batch_size: int = 64):
        """Yield batches of (doc_id, content) for documents without chunk embeddings.

        Always queries from offset 0 because update_embeddings() marks each processed
        batch as has_embeddings=true before the next batch is fetched, so the result
        set naturally shrinks — offset-based pagination would skip documents otherwise.
        """
        while True:
            try:
                resp = self._idx().search(
                    "",
                    {
                        "filter": "has_embeddings = false",
                        "attributesToRetrieve": ["id", "content"],
                        "limit": batch_size,
                        "offset": 0,
                    },
                )
            except meilisearch.errors.MeilisearchApiError as exc:
                if exc.code == "index_not_found":
                    return
                raise
            hits = resp.get("hits", [])
            if not hits:
                break
            yield [(hit["id"], hit.get("content", "") or "") for hit in hits]
            if len(hits) < batch_size:
                break

    def scroll_document_ids(self, doc_ids: list[str], batch_size: int = 64):
        """Yield batches of (doc_id, content) for specific document IDs.

        Uses a single filter query per batch (``id IN [...]``) rather than one
        ``get_document`` call per ID, which would be O(N) round-trips.
        """
        for i in range(0, len(doc_ids), batch_size):
            chunk = doc_ids[i : i + batch_size]
            ids_str = ", ".join(f'"{doc_id}"' for doc_id in chunk)
            try:
                resp = self._idx().search(
                    "",
                    {
                        "filter": f"id IN [{ids_str}]",
                        "attributesToRetrieve": ["id", "content"],
                        "limit": len(chunk),
                    },
                )
            except meilisearch.errors.MeilisearchApiError as exc:
                if exc.code == "index_not_found":
                    return
                log.exception("failed to batch fetch documents for re-embed", n_ids=len(chunk))
                continue
            hits = resp.get("hits", [])
            found_ids = {hit["id"] for hit in hits}
            for doc_id in chunk:
                if doc_id not in found_ids:
                    log.warning("document not found for retry", doc_id=doc_id)
            batch = [(hit["id"], hit.get("content", "") or "") for hit in hits]
            if batch:
                yield batch

    def update_embeddings(self, updates: list[tuple[str, list[list[float]]]]) -> int:
        """Bulk-update chunk embeddings on existing documents. Returns failure count."""
        if not updates:
            return 0
        docs_to_update = [
            {
                "id": doc_id,
                "has_embeddings": True,
                "_vectors": {
                    "custom": {
                        "embeddings": chunk_vectors,
                        "regenerate": False,
                    }
                },
            }
            for doc_id, chunk_vectors in updates
        ]

        sub_batches, _ = _split_by_payload_size(docs_to_update)
        if len(sub_batches) > 1:
            log.info(
                "splitting embedding update into sub-batches to stay under payload limit",
                original=len(docs_to_update),
                sub_batches=len(sub_batches),
            )

        total_failed = 0
        for sub in sub_batches:
            try:
                task = self._idx().update_documents(sub)
                result = self._client.wait_for_task(task.task_uid, timeout_in_ms=_EMBED_TASK_TIMEOUT_MS)
                if result.status == "failed":
                    error = result.error or {}
                    log.warning(
                        "meilisearch embedding sub-batch update failed",
                        error=error,
                        count=len(sub),
                    )
                    total_failed += len(sub)
            except Exception:
                log.exception("meilisearch embedding sub-batch update exception", n_docs=len(sub))
                total_failed += len(sub)
        return total_failed

    def get_existing_doc_ids(self, doc_ids: list[str]) -> set[str]:
        """Return the subset of *doc_ids* that already exist in the index."""
        if not doc_ids:
            return set()
        ids_str = ", ".join(f'"{doc_id}"' for doc_id in doc_ids)
        try:
            resp = self._idx().search(
                "",
                {
                    "filter": f"id IN [{ids_str}]",
                    "attributesToRetrieve": ["id"],
                    "limit": len(doc_ids),
                },
            )
        except meilisearch.errors.MeilisearchApiError as exc:
            if exc.code == "index_not_found":
                return set()
            raise
        return {hit["id"] for hit in resp.get("hits", [])}
