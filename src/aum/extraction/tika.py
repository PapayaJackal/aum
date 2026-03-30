from __future__ import annotations

import hashlib
import io
import time
import zipfile
from pathlib import Path

import httpx
import structlog

from aum.extraction.base import ExtractionDepthError, ExtractionError, RecordErrorFn
from aum.metrics import DOCS_TRUNCATED, EXTRACTION_DURATION, EXTRACTION_ERRORS
from aum.models import Document

log = structlog.get_logger()

# Metadata key where Tika stores extracted text in /rmeta responses.
TIKA_CONTENT_KEY = "X-TIKA:content"

# Metadata key for the hierarchical path of an embedded resource.
_EMBEDDED_RESOURCE_PATH_KEY = "X-TIKA:embedded_resource_path"

# Tika metadata keys that are internal and should not be forwarded.
_TIKA_INTERNAL_KEYS = frozenset(
    {
        TIKA_CONTENT_KEY,
        _EMBEDDED_RESOURCE_PATH_KEY,
        "X-TIKA:content_handler",
        "X-TIKA:content_handler_type",
        "X-TIKA:parse_time_millis",
        "X-TIKA:Parsed-By",
        "X-TIKA:Parsed-By-Full-Set",
    }
)


def _condense_whitespace(text: str) -> str:
    """Collapse consecutive blank lines down to at most one.

    A line is considered blank if it contains only whitespace (including
    non-breaking spaces and other Unicode whitespace that Tika may emit).
    """
    lines = text.split("\n")
    result: list[str] = []
    blank_run = 0
    for line in lines:
        if line.strip() == "":
            blank_run += 1
            if blank_run <= 1:
                result.append("")
        else:
            blank_run = 0
            result.append(line)
    return "\n".join(result)


def _normalize_metadata(raw: dict) -> dict[str, str | list[str]]:
    metadata: dict[str, str | list[str]] = {}
    for key, value in raw.items():
        if key in _TIKA_INTERNAL_KEYS:
            continue
        if isinstance(value, list):
            metadata[key] = [str(v) for v in value]
        else:
            metadata[key] = str(value)
    return metadata


def _container_dir(extract_dir: Path, index_name: str, file_path: Path) -> Path:
    """Return a stable subdirectory for attachments extracted from a container file.

    Shards by the first two pairs of hex digits to avoid too many entries in a
    single directory, and namespaces under the index so extractions from
    different indices don't collide.
    """
    file_hash = hashlib.sha256(str(file_path.resolve()).encode()).hexdigest()[:16]
    return extract_dir / index_name / file_hash[:2] / file_hash[2:4] / file_hash


def _parse_unpack_zip(data: bytes) -> dict[str, bytes]:
    """Parse the zip returned by /unpack/all into {filename: bytes}.

    Skips per-file .metadata.json sidecars and the original document
    (which we already have on disk).
    """
    attachments: dict[str, bytes] = {}
    if not data:
        return attachments

    with zipfile.ZipFile(io.BytesIO(data)) as zf:
        for name in zf.namelist():
            # Skip metadata sidecars produced when includeMetadataInZip=true
            if name.endswith(".metadata.json"):
                continue
            attachments[name] = zf.read(name)

    return attachments


def _find_container_paths(parts: list[dict]) -> set[str]:
    """Identify which embedded_resource_paths are containers (have children).

    A path is a container if any deeper path starts with it.  For example
    if the parts contain ``/archive.zip/doc.txt``, then ``/archive.zip``
    is a container.
    """
    all_paths: list[str] = []
    for part in parts[1:]:
        erp = part.get(_EMBEDDED_RESOURCE_PATH_KEY, "")
        if erp:
            all_paths.append(erp)

    containers: set[str] = set()
    for path in all_paths:
        # Walk up the path to mark every ancestor as a container.
        # e.g. "/a.zip/b.tar/c.txt" → "/a.zip/b.tar" and "/a.zip" are containers.
        segments = [s for s in path.split("/") if s]
        for depth in range(1, len(segments)):
            containers.add("/" + "/".join(segments[:depth]))
    return containers


class TikaExtractor:
    """Extract text and metadata from documents using Apache Tika.

    Uses Tika's HTTP API directly via httpx:
    - ``/rmeta/text`` for recursive text + metadata extraction (one JSON
      entry per document, including all embedded docs at every depth).
    - ``/unpack/all`` to retrieve raw embedded files for download, called
      recursively on containers so nested attachments are also available.
    """

    def __init__(
        self,
        server_url: str = "http://localhost:9998",
        ocr_enabled: bool = False,
        ocr_language: str = "eng",
        extract_dir: str = "data/extracted",
        index_name: str = "default",
        max_depth: int = 5,
        request_timeout: int = 300,
        max_content_length: int = 0,
    ) -> None:
        self._server_url = server_url.rstrip("/")
        self._ocr_enabled = ocr_enabled
        self._ocr_language = ocr_language
        self._extract_dir = Path(extract_dir)
        self._index_name = index_name
        self._max_depth = max_depth
        self._max_content_length = max_content_length
        self._request_timeout = request_timeout
        self._client = httpx.Client(timeout=request_timeout)

    def close(self) -> None:
        """Close the underlying HTTP client to free connections."""
        self._client.close()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(
        self,
        file_path: Path,
        record_error: RecordErrorFn | None = None,
    ) -> list[Document]:
        """Extract text, metadata, and embedded documents from *file_path*.

        Returns one Document per content part (container + each embedded doc).
        """
        log.debug("extracting document", file_path=str(file_path))
        start = time.monotonic()

        try:
            parts = self._rmeta(file_path)
        except ExtractionError:
            EXTRACTION_DURATION.observe(time.monotonic() - start)
            raise

        if not parts:
            # Tika returned an empty list — treat as a parseable file with no
            # content so we still index its metadata and count it properly.
            parts = [{}]

        has_embedded = len(parts) > 1

        # Recursively unpack embedded files so they are available for download.
        # Uses the /rmeta tree to know which attachments are themselves
        # containers that need further unpacking.
        attachment_map: dict[str, Path] = {}  # embedded_resource_path → local path
        if has_embedded:
            container_paths = _find_container_paths(parts)
            try:
                attachment_map = self._unpack_recursive(
                    file_path,
                    container_paths,
                    depth=0,
                    current_erp="",
                )
            except ExtractionDepthError:
                raise
            except ExtractionError as exc:
                # Unpack failed entirely (e.g. corrupted/truncated archive).
                # Drop all embedded parts — indexing thousands of empty
                # documents just pollutes the index and error log.  Record
                # a single error so the user can investigate the file.
                log.warning(
                    "unpack failed, dropping embedded documents",
                    file_path=str(file_path),
                    embedded_count=len(parts) - 1,
                    error=str(exc),
                )
                EXTRACTION_ERRORS.labels(error_type="UnpackError").inc()
                if record_error is not None:
                    record_error(
                        file_path,
                        "UnpackError",
                        f"failed to unpack {len(parts) - 1} embedded documents: {exc}",
                    )
                parts = parts[:1]  # keep only the container

        EXTRACTION_DURATION.observe(time.monotonic() - start)

        documents: list[Document] = []
        empty_extractions = 0
        _size_cache: dict[Path, int] = {}

        for i, part in enumerate(parts):
            raw = (part.get(TIKA_CONTENT_KEY) or "").strip()
            # Truncate before whitespace condensing so we don't waste CPU
            # processing megabytes of content that will be discarded.
            original_length = 0
            if self._max_content_length and len(raw) > self._max_content_length:
                original_length = len(raw)
                raw = raw[: self._max_content_length]
            content = _condense_whitespace(raw)
            metadata = _normalize_metadata(part)

            if i == 0:
                source = file_path
            else:
                # Embedded document — resolve source path and display metadata.
                erp = part.get(_EMBEDDED_RESOURCE_PATH_KEY, "")
                resource_name = part.get("resourceName") or erp.rsplit("/", 1)[-1] or f"embedded-{i}"

                source = attachment_map.get(erp, file_path)

                if erp:
                    display = file_path / erp.lstrip("/")
                else:
                    display = file_path / resource_name
                metadata["_aum_display_path"] = str(display)

                if erp and "/" in erp.strip("/"):
                    parent_erp = erp.rsplit("/", 1)[0]
                    metadata["_aum_extracted_from"] = str(file_path / parent_erp.lstrip("/"))
                else:
                    metadata["_aum_extracted_from"] = str(file_path)

            if original_length:
                DOCS_TRUNCATED.inc()
                log.warning(
                    "content truncated",
                    file_path=str(source),
                    original_chars=original_length,
                    truncated_chars=self._max_content_length,
                )
                if record_error is not None:
                    record_error(
                        source,
                        "ContentTruncated",
                        f"content truncated from {original_length} to {self._max_content_length} chars"
                        " (exceeded ingest_max_content_length limit)",
                    )

            documents.append(Document(source_path=source, content=content, metadata=metadata))

            if not content:
                if source not in _size_cache:
                    try:
                        _size_cache[source] = source.stat().st_size
                    except OSError:
                        _size_cache[source] = 0
                if _size_cache[source] > 0:
                    empty_extractions += 1

        if empty_extractions:
            EXTRACTION_ERRORS.labels(error_type="EmptyExtraction").inc(empty_extractions)
            log.warning(
                "empty extractions",
                file_path=str(file_path),
                count=empty_extractions,
            )
            if record_error is not None:
                record_error(
                    file_path,
                    "EmptyExtraction",
                    f"Tika produced no text for {empty_extractions} document(s) from {file_path}",
                )

        log.info(
            "extracted document",
            file_path=str(file_path),
            parts=len(documents),
            embedded=has_embedded,
        )
        return documents

    def supports(self, mime_type: str) -> bool:
        return True

    # ------------------------------------------------------------------
    # HTTP helpers
    # ------------------------------------------------------------------

    def _tika_headers(self) -> dict[str, str]:
        headers: dict[str, str] = {}
        if not self._ocr_enabled:
            headers["X-Tika-OCRskipOcr"] = "true"
        else:
            headers["X-Tika-OCRLanguage"] = self._ocr_language
        return headers

    def _rmeta(self, file_path: Path) -> list[dict]:
        """Call ``PUT /rmeta/text`` and return the JSON metadata list.

        The first element is the container; subsequent elements are embedded
        documents (recursively, at all depths).  Each element's
        ``X-TIKA:content`` key holds the extracted plain text.
        """
        headers = {
            **self._tika_headers(),
            "Accept": "application/json",
        }

        try:
            with open(file_path, "rb") as f:
                resp = self._client.put(
                    f"{self._server_url}/rmeta/text",
                    content=f,
                    headers=headers,
                )
        except httpx.HTTPError as exc:
            EXTRACTION_ERRORS.labels(error_type="RmetaConnectionError").inc()
            raise ExtractionError(f"Tika /rmeta request failed for {file_path}: {exc}") from exc

        if resp.status_code != 200:
            EXTRACTION_ERRORS.labels(error_type=f"TikaHTTP{resp.status_code}").inc()
            raise ExtractionError(f"Tika /rmeta returned HTTP {resp.status_code} for {file_path}: {resp.text[:200]}")

        try:
            return resp.json()
        except Exception as exc:
            EXTRACTION_ERRORS.labels(error_type="RmetaParseError").inc()
            raise ExtractionError(f"Failed to parse Tika /rmeta JSON for {file_path}: {exc}") from exc

    def _unpack_raw(self, file_path: Path) -> dict[str, bytes]:
        """Call ``PUT /unpack/all`` and return the raw attachment bytes.

        Returns ``{filename: bytes}`` for immediate children only.
        Returns an empty dict on 204 (no embedded files).
        Raises ExtractionError on failure.
        """
        headers = {
            **self._tika_headers(),
            "Accept": "application/zip",
        }

        try:
            with open(file_path, "rb") as f:
                resp = self._client.put(
                    f"{self._server_url}/unpack/all",
                    content=f,
                    headers=headers,
                )
        except httpx.HTTPError as exc:
            EXTRACTION_ERRORS.labels(error_type="UnpackConnectionError").inc()
            raise ExtractionError(f"Tika /unpack request failed for {file_path}: {exc}") from exc

        if resp.status_code == 204:
            return {}

        if resp.status_code != 200:
            EXTRACTION_ERRORS.labels(error_type="UnpackError").inc()
            raise ExtractionError(f"Tika /unpack returned HTTP {resp.status_code} for {file_path}: {resp.text[:200]}")

        return _parse_unpack_zip(resp.content)

    def _unpack_recursive(
        self,
        file_path: Path,
        container_paths: set[str],
        depth: int,
        current_erp: str,
    ) -> dict[str, Path]:
        """Recursively unpack embedded files and save them to disk.

        *container_paths* is the set of ``embedded_resource_path`` values
        (from the /rmeta response) that are known to contain further
        embedded documents and therefore need recursive unpacking.

        Returns a flat mapping of ``{embedded_resource_path: local_path}``
        covering all depths.
        """
        if depth > self._max_depth:
            raise ExtractionDepthError(f"extraction depth limit ({self._max_depth}) exceeded at {file_path}")

        raw = self._unpack_raw(file_path)
        if not raw:
            return {}

        dest_dir = _container_dir(self._extract_dir, self._index_name, file_path)
        dest_dir.mkdir(parents=True, exist_ok=True)

        result: dict[str, Path] = {}
        for name, data in raw.items():
            safe_name = Path(name).name or name.replace("/", "_")
            if not safe_name or safe_name.startswith(".") or "\x00" in safe_name:
                continue
            att_path = dest_dir / safe_name
            att_path.write_bytes(data)

            child_erp = f"{current_erp}/{name}"
            result[child_erp] = att_path
            log.debug(
                "saved attachment",
                container=str(file_path),
                attachment=str(att_path),
                erp=child_erp,
                size=len(data),
            )

            # If this child is itself a container, recursively unpack it.
            if child_erp in container_paths:
                try:
                    nested = self._unpack_recursive(
                        att_path,
                        container_paths,
                        depth + 1,
                        child_erp,
                    )
                    result.update(nested)
                except ExtractionDepthError:
                    raise
                except ExtractionError as exc:
                    log.warning(
                        "recursive unpack failed for attachment",
                        attachment=str(att_path),
                        container=str(file_path),
                        erp=child_erp,
                        error=str(exc),
                    )

        return result
