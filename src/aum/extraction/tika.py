from __future__ import annotations

import hashlib
import time
from pathlib import Path

import structlog
from tika import parser as tika_parser
from tika import unpack as tika_unpack
from tika.tika import parse1 as tika_parse1

from aum.extraction.base import ExtractionDepthError, ExtractionError
from aum.metrics import EXTRACTION_DURATION, EXTRACTION_ERRORS
from aum.models import Document

log = structlog.get_logger()


def _normalize_metadata(raw: dict) -> dict[str, str | list[str]]:
    metadata: dict[str, str | list[str]] = {}
    for key, value in raw.items():
        if isinstance(value, list):
            metadata[key] = [str(v) for v in value]
        else:
            metadata[key] = str(value)
    return metadata


def _container_dir(extract_dir: Path, file_path: Path) -> Path:
    """Return a stable subdirectory for attachments extracted from a container file."""
    file_hash = hashlib.sha256(str(file_path.resolve()).encode()).hexdigest()[:16]
    return extract_dir / file_hash


class TikaExtractor:
    """Extract text and metadata from documents using Apache Tika.

    Handles embedded/attached documents (e.g. email attachments, files inside
    archives) by unpacking them to disk and recursively extracting each one.
    """

    def __init__(
        self,
        server_url: str = "http://localhost:9998",
        ocr_enabled: bool = True,
        ocr_language: str = "eng",
        extract_dir: str = "data/extracted",
        max_depth: int = 5,
    ) -> None:
        self._server_url = server_url
        self._ocr_enabled = ocr_enabled
        self._ocr_language = ocr_language
        self._extract_dir = Path(extract_dir)
        self._max_depth = max_depth

    def extract(self, file_path: Path) -> list[Document]:
        return self._extract_recursive(file_path, depth=0)

    def _extract_recursive(self, file_path: Path, depth: int) -> list[Document]:
        if depth > self._max_depth:
            raise ExtractionDepthError(
                f"extraction depth limit ({self._max_depth}) exceeded at {file_path}"
            )

        log.debug("extracting document", file_path=str(file_path), depth=depth)
        start = time.monotonic()

        # Primary path: one unpack call gives us the container's own text (__TEXT__),
        # its metadata (__METADATA__), and the raw embedded files — all in one shot.
        # The __TEXT__ content naturally excludes embedded document text.
        unpack_parsed: dict = {}
        attachment_paths: list[Path] = []
        try:
            unpack_parsed, attachment_paths = self._unpack(file_path)
        except ExtractionError as exc:
            log.warning("unpack failed, will fall back to direct extraction",
                        file_path=str(file_path), error=str(exc))

        content = (unpack_parsed.get("content") or "").strip()
        raw_metadata = unpack_parsed.get("metadata") or {}

        # Fallback: if unpack produced no text (failed or file has no attachments
        # and Tika skipped __TEXT__), call the parser directly with skip-embedded.
        if not content:
            try:
                fallback = self._extract_container(file_path)
                if fallback is not None:
                    content = fallback.content
                    raw_metadata = fallback.metadata  # type: ignore[assignment]
            except ExtractionError as exc:
                if not attachment_paths:
                    # Both unpack and direct extraction failed and there's nothing
                    # to index — propagate so the pipeline records a job failure.
                    EXTRACTION_DURATION.observe(time.monotonic() - start)
                    raise
                log.warning("container extraction failed",
                            file_path=str(file_path), error=str(exc))

        EXTRACTION_DURATION.observe(time.monotonic() - start)

        documents: list[Document] = []
        if content:
            documents.append(Document(
                source_path=file_path,
                content=content,
                metadata=_normalize_metadata(raw_metadata) if isinstance(raw_metadata, dict) else raw_metadata,
            ))

        # Recursively extract each attachment.
        # ExtractionDepthError is not caught here — it propagates to the pipeline
        # so it's recorded as a job failure rather than silently swallowed.
        for att_path in attachment_paths:
            try:
                documents.extend(self._extract_recursive(att_path, depth=depth + 1))
            except ExtractionDepthError:
                raise
            except ExtractionError as exc:
                log.warning(
                    "failed to extract attachment",
                    attachment=str(att_path),
                    container=str(file_path),
                    depth=depth,
                    error=str(exc),
                )
                EXTRACTION_ERRORS.labels(error_type="AttachmentError").inc()

        if not documents:
            # File parsed but produced no text — keep a placeholder so it's
            # tracked as processed rather than silently dropped
            documents.append(Document(source_path=file_path, content="", metadata={}))

        log.info(
            "extracted document",
            file_path=str(file_path),
            depth=depth,
            parts=len(documents),
        )

        return documents

    def _unpack(self, file_path: Path) -> tuple[dict, list[Path]]:
        """Call Tika's /unpack/all endpoint.

        Returns (parsed_dict, attachment_paths) where parsed_dict contains
        'content' (__TEXT__ — container only, no embedded docs) and 'metadata'.
        Raises ExtractionError if the unpack call fails.
        """
        try:
            raw = tika_parse1(
                "unpack", str(file_path), self._server_url,
                responseMimeType="application/x-tar",
                services={"meta": "/meta", "text": "/tika", "all": "/rmeta/xml", "unpack": "/unpack/all"},
                rawResponse=True,
            )
            status, response_bytes = raw
            parsed = tika_unpack._parse(raw)
        except Exception as exc:
            EXTRACTION_ERRORS.labels(error_type="UnpackError").inc()
            raise ExtractionError(
                f"Tika unpack failed for {file_path} (HTTP {status if 'status' in dir() else '?'}): {exc}"
            ) from exc

        if status != 200 and not parsed:
            reason = response_bytes.decode("utf-8", errors="replace").strip()[:200] if response_bytes else "empty response"
            EXTRACTION_ERRORS.labels(error_type="UnpackError").inc()
            raise ExtractionError(f"Tika unpack failed for {file_path} (HTTP {status}): {reason}")

        attachments: dict[str, bytes] = parsed.get("attachments") or {}
        attachment_paths: list[Path] = []

        if attachments:
            dest_dir = _container_dir(self._extract_dir, file_path)
            dest_dir.mkdir(parents=True, exist_ok=True)

            for name, data in attachments.items():
                safe_name = Path(name).name or name.replace("/", "_")
                if not safe_name:
                    continue
                att_path = dest_dir / safe_name
                att_path.write_bytes(data)
                attachment_paths.append(att_path)
                log.debug(
                    "saved attachment",
                    container=str(file_path),
                    attachment=str(att_path),
                    size=len(data),
                )

        return parsed, attachment_paths

    def _extract_container(self, file_path: Path) -> Document | None:
        """Fallback: extract text and metadata via the parser with skip-embedded.

        Used when unpack produces no text content.
        """
        headers = self._tika_headers(skip_embedded=True)

        try:
            parts = tika_parser.from_file(
                str(file_path),
                serverEndpoint=self._server_url,
                headers=headers,
                service="all",
            )
        except Exception as exc:
            EXTRACTION_ERRORS.labels(error_type=type(exc).__name__).inc()
            raise ExtractionError(f"Tika failed to parse {file_path}: {exc}") from exc

        if not parts:
            EXTRACTION_ERRORS.labels(error_type="NullResponse").inc()
            raise ExtractionError(f"Tika returned null for {file_path}")

        if isinstance(parts, dict):
            parts = [parts]

        container = parts[0]
        status = container.get("status")
        if status is not None and status != 200:
            EXTRACTION_ERRORS.labels(error_type=f"TikaHTTP{status}").inc()
            raise ExtractionError(f"Tika returned HTTP {status} for {file_path}")

        content = (container.get("content") or "").strip()
        if not content:
            return None

        raw_metadata = container.get("metadata") or {}
        return Document(
            source_path=file_path,
            content=content,
            metadata=_normalize_metadata(raw_metadata),
        )

    def _tika_headers(self, skip_embedded: bool = False) -> dict[str, str]:
        headers: dict[str, str] = {}
        if not self._ocr_enabled:
            headers["X-Tika-OCRskipOcr"] = "true"
        else:
            headers["X-Tika-OCRLanguage"] = self._ocr_language
        if skip_embedded:
            headers["X-Tika-Skip-Embedded"] = "true"
        return headers

    def supports(self, mime_type: str) -> bool:
        # Tika is the catch-all extractor — it handles virtually everything.
        # More specialized extractors can claim specific types at higher priority.
        return True
