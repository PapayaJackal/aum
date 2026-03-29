from __future__ import annotations

from dataclasses import dataclass, field
from email.utils import getaddresses
from typing import Protocol

from aum.models import Document


def extract_email(raw: str) -> str | None:
    """Extract just the email address from an RFC 2822 string like 'Name <email>'.

    Uses ``getaddresses`` rather than ``parseaddr`` because the latter chokes
    on commas inside display names (e.g. ``"Last, First <addr>"``).

    Returns the lowercased email, or *None* if no valid address can be found
    (e.g. ``"undisclosed-recipients:;"``).
    """
    for _, addr in getaddresses([raw]):
        if "@" in addr:
            return addr.strip().lower()
    # Fallback: extract from angle brackets (handles cases like
    # "user@example.com <user@example.com>" where getaddresses chokes).
    if "<" in raw and ">" in raw:
        inner = raw[raw.index("<") + 1 : raw.index(">")]
        if "@" in inner:
            return inner.strip().lower()
    # Bare address without angle brackets.
    stripped = raw.strip()
    if "@" in stripped:
        return stripped.lower()
    return None


# ---------------------------------------------------------------------------
# MIME-type aliases – shown instead of raw content-type strings in facets
# and search results.
# ---------------------------------------------------------------------------

MIMETYPE_ALIASES: dict[str, str] = {
    "application/pdf": "PDF",
    "application/msword": "Word",
    "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "Word",
    "application/vnd.ms-excel": "Excel",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "Excel",
    "application/vnd.ms-powerpoint": "PowerPoint",
    "application/vnd.openxmlformats-officedocument.presentationml.presentation": "PowerPoint",
    "application/rtf": "RTF",
    "application/vnd.oasis.opendocument.text": "OpenDocument Text",
    "application/vnd.oasis.opendocument.spreadsheet": "OpenDocument Spreadsheet",
    "text/plain": "Plain Text",
    "text/html": "HTML",
    "text/csv": "CSV",
    "text/xml": "XML",
    "application/xml": "XML",
    "application/json": "JSON",
    "application/zip": "ZIP",
    "application/x-tar": "TAR",
    "application/gzip": "GZIP",
    "message/rfc822": "Email",
    "image/jpeg": "JPEG Image",
    "image/png": "PNG Image",
    "image/tiff": "TIFF Image",
    "image/gif": "GIF Image",
}


def alias_mimetype(raw: str) -> str:
    """Return a human-friendly label for a MIME type, stripping parameters."""
    base = raw.split(";")[0].strip()
    return MIMETYPE_ALIASES.get(base, base)


# Reverse mapping: alias → list of raw MIME types that produce that alias.
REVERSE_MIMETYPE_ALIASES: dict[str, list[str]] = {}
for _raw, _alias in MIMETYPE_ALIASES.items():
    REVERSE_MIMETYPE_ALIASES.setdefault(_alias, []).append(_raw)


# ---------------------------------------------------------------------------
# Human-readable aliases for common Tika metadata keys.
# ---------------------------------------------------------------------------

METADATA_KEY_ALIASES: dict[str, str] = {
    "Content-Type": "Content Type",
    "dc:creator": "Author",
    "meta:author": "Author",
    "Author": "Author",
    "creator": "Creator",
    "dcterms:created": "Created",
    "Creation-Date": "Created",
    "meta:creation-date": "Created",
    "dcterms:modified": "Modified",
    "Last-Modified": "Modified",
    "meta:save-date": "Modified",
    "Content-Length": "File Size",
    "dc:title": "Title",
    "dc:subject": "Subject",
    "dc:description": "Description",
    "Message-From": "From",
    "Message-To": "To",
    "Message-CC": "CC",
    "subject": "Subject",
    "pdf:PDFVersion": "PDF Version",
    "xmpTPg:NPages": "Page Count",
    "meta:page-count": "Page Count",
    "meta:word-count": "Word Count",
    "meta:character-count": "Character Count",
    "Application-Name": "Application",
    "producer": "Producer",
    "pdf:docinfo:producer": "Producer",
}

# Tika keys that are noisy/internal and should be hidden from display.
HIDDEN_METADATA_KEYS: set[str] = {
    "X-Parsed-By",
    "X-TIKA:Parsed-By",
    "X-TIKA:Parsed-By-Full-Set",
    "X-TIKA:content_handler",
    "X-TIKA:embedded_depth",
    "X-TIKA:embedded_resource_path",
    "X-TIKA:parse_time_millis",
    "resourceName",
    "pdf:unmappedUnicodeCharsPerPage",
    "pdf:charsPerPage",
    "pdf:docinfo:custom:PageCount",
    "access_permission:can_print_degraded",
    "access_permission:can_modify",
    "access_permission:extract_content",
    "access_permission:can_print",
    "access_permission:extract_for_accessibility",
    "access_permission:fill_in_form",
    "access_permission:assemble_document",
    "access_permission:can_print_faithful",
    "pdf:encrypted",
    "pdf:hasXFA",
    "pdf:hasMarkedContent",
    "pdf:totalUnmappedUnicodeChars",
    "pdf:hasXMP",
    "pdf:hasCollection",
    "pdf:containsDamagedFont",
}

# ---------------------------------------------------------------------------
# Facet definitions – labels → indexed meta field paths.
# ---------------------------------------------------------------------------

FACET_FIELDS: dict[str, str] = {
    "Created": "meta.created",
    "File Type": "meta.content_type",
    "Creator": "meta.creator",
    "Email Addresses": "meta.email_addresses",
}

# Facets that use date-histogram aggregation rather than terms.
DATE_FACETS: set[str] = {"Created"}


@dataclass
class SearchResult:
    doc_id: str
    source_path: str
    display_path: str
    score: float
    snippet: str
    metadata: dict[str, str | list[str]] = field(default_factory=dict)
    extracted_from: str = ""
    display_path_highlighted: str = ""
    index: str = ""


@dataclass
class BatchResult:
    """Result of a batch indexing operation."""

    failures: list[tuple[str, str]] = field(default_factory=list)
    """(doc_id, error_reason) for documents that failed to index."""

    truncated: list[tuple[str, int, int]] = field(default_factory=list)
    """(doc_id, original_chars, truncated_chars) for documents whose content was truncated."""


class SearchBackend(Protocol):
    def initialize(self, *, vector_dimension: int | None = None) -> None:
        """Create index/mappings. If vector_dimension is set, configure vector fields."""
        ...

    def index_document(self, doc_id: str, document: Document) -> None:
        """Index a single document."""
        ...

    def index_batch(self, documents: list[tuple[str, Document]]) -> BatchResult:
        """Index a batch of (doc_id, document) pairs."""
        ...

    def search_text(
        self,
        query: str,
        *,
        limit: int = 20,
        offset: int = 0,
        include_facets: bool = False,
        filters: dict[str, list[str]] | None = None,
    ) -> tuple[list[SearchResult], int, dict[str, list[str]] | None]:
        """Full-text keyword search. Returns (results, total_count, facets). facets is None unless include_facets=True."""
        ...

    def search_vector(
        self,
        vector: list[float],
        *,
        limit: int = 20,
        offset: int = 0,
        include_facets: bool = False,
        filters: dict[str, list[str]] | None = None,
    ) -> tuple[list[SearchResult], int, dict[str, list[str]] | None]:
        """Vector similarity search (kNN). Returns (results, total_count, facets). facets is None unless include_facets=True."""
        ...

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
        """Combined keyword + vector search. Returns (results, total_count, facets). facets is None unless include_facets=True."""
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

    def find_attachments(self, display_path: str) -> list[SearchResult]:
        """Find documents extracted from the given display_path."""
        ...

    def find_by_display_path(self, display_path: str) -> SearchResult | None:
        """Find a single document by exact display_path."""
        ...

    def list_indices(self) -> list[str]:
        """Return a list of available index names."""
        ...

    def count_unembedded(self) -> int:
        """Return the number of documents without embeddings."""
        ...

    def scroll_unembedded(self, batch_size: int = 64) -> ...:
        """Yield batches of (doc_id, content) for documents without embeddings."""
        ...

    def scroll_document_ids(self, doc_ids: list[str], batch_size: int = 64) -> ...:
        """Yield batches of (doc_id, content) for specific document IDs."""
        ...

    def update_embeddings(self, updates: list[tuple[str, list[list[float]]]]) -> int:
        """Bulk-update chunk embedding vectors. Returns number of failures."""
        ...

    def get_existing_doc_ids(self, doc_ids: list[str]) -> set[str]:
        """Return the subset of *doc_ids* that already exist in the index."""
        ...
