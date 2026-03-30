"""Tests for text extraction helpers."""

import io
import zipfile
from pathlib import Path
from unittest.mock import patch

import httpx
import pytest

from aum.extraction.base import ExtractionDepthError, ExtractionError
from aum.extraction.tika import (
    TIKA_CONTENT_KEY,
    TikaExtractor,
    _condense_whitespace,
    _container_dir,
    _find_container_paths,
    _parse_unpack_zip,
    _safe_archive_path,
)

_ERP_KEY = "X-TIKA:embedded_resource_path"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class TestCondenseWhitespace:
    """Tests for _condense_whitespace()."""

    def test_plain_excess_newlines(self):
        assert _condense_whitespace("a\n\n\n\nb") == "a\n\nb"

    def test_two_newlines_preserved(self):
        assert _condense_whitespace("a\n\nb") == "a\n\nb"

    def test_single_newline_preserved(self):
        assert _condense_whitespace("a\nb") == "a\nb"

    def test_blank_lines_with_spaces(self):
        assert _condense_whitespace("a\n   \n   \n   \nb") == "a\n\nb"

    def test_blank_lines_with_tabs(self):
        assert _condense_whitespace("a\n\t\n\t\n\tb") == "a\n\n\tb"

    def test_blank_lines_with_mixed_whitespace(self):
        assert _condense_whitespace("a\n \t \n\t \n \t\nb") == "a\n\nb"

    def test_multiple_groups(self):
        text = "a\n\n\n\nb\n\n\n\nc"
        assert _condense_whitespace(text) == "a\n\nb\n\nc"

    def test_no_newlines(self):
        assert _condense_whitespace("hello world") == "hello world"

    def test_empty_string(self):
        assert _condense_whitespace("") == ""

    def test_exactly_three_blank_lines_condensed(self):
        assert _condense_whitespace("a\n\n\nb") == "a\n\nb"

    def test_whitespace_before_newlines(self):
        """Lines like 'text \\n \\n \\n' where spaces precede the newline."""
        assert _condense_whitespace("a \n \n \n \nb") == "a \n\nb"

    def test_trailing_spaces_on_blank_lines(self):
        """Mimics Tika output with ' \\n' blank lines (space before newline)."""
        text = "Betzy\n \n \n \nPolicy Advisor"
        assert _condense_whitespace(text) == "Betzy\n\nPolicy Advisor"

    def test_non_breaking_spaces(self):
        """Tika often emits non-breaking spaces (\\xa0) on blank lines."""
        text = "Hello\n\xa0\n\xa0\n\xa0\nWorld"
        assert _condense_whitespace(text) == "Hello\n\nWorld"

    def test_mixed_unicode_whitespace(self):
        """Blank lines with a mix of regular and non-breaking spaces."""
        text = "A\n \xa0 \n\t\xa0\n \nB"
        assert _condense_whitespace(text) == "A\n\nB"


class TestContainerDir:
    """Tests for the sharded extraction directory path."""

    def test_path_structure(self, tmp_path: Path) -> None:
        file_path = tmp_path / "doc.pdf"
        result = _container_dir(Path("/data/extracted"), "myindex", file_path)
        parts = result.parts
        # structure: /data/extracted / myindex / XX / XX / XXXXXXXXXXXXXXXX
        assert parts[-5] == "extracted"
        assert parts[-4] == "myindex"
        assert len(parts[-3]) == 2  # first shard
        assert len(parts[-2]) == 2  # second shard
        assert len(parts[-1]) == 16  # full hash prefix
        assert parts[-1].startswith(parts[-3])
        assert parts[-1][2:4] == parts[-2]

    def test_different_indices_dont_collide(self, tmp_path: Path) -> None:
        file_path = tmp_path / "doc.pdf"
        a = _container_dir(Path("/ex"), "index-a", file_path)
        b = _container_dir(Path("/ex"), "index-b", file_path)
        assert a != b
        assert a.parts[-4] == "index-a"
        assert b.parts[-4] == "index-b"

    def test_stable_across_calls(self, tmp_path: Path) -> None:
        file_path = tmp_path / "doc.pdf"
        assert _container_dir(Path("/ex"), "idx", file_path) == _container_dir(Path("/ex"), "idx", file_path)


# ---------------------------------------------------------------------------
# _safe_archive_path
# ---------------------------------------------------------------------------


class TestSafeArchivePath:
    def test_simple_filename(self) -> None:
        assert _safe_archive_path("file.txt") == Path("file.txt")

    def test_preserves_directory_structure(self) -> None:
        assert _safe_archive_path("dir1/dir2/file.txt") == Path("dir1/dir2/file.txt")

    def test_rejects_null_bytes(self) -> None:
        assert _safe_archive_path("file\x00.txt") is None

    def test_rejects_empty_string(self) -> None:
        assert _safe_archive_path("") is None

    def test_rejects_absolute_path(self) -> None:
        assert _safe_archive_path("/etc/passwd") is None

    def test_rejects_dotdot_components(self) -> None:
        assert _safe_archive_path("../../etc/passwd") is None
        assert _safe_archive_path("../..") is None
        assert _safe_archive_path("dir/../etc/passwd") is None

    def test_rejects_hidden_leaf_file(self) -> None:
        assert _safe_archive_path(".hidden") is None
        assert _safe_archive_path("dir/.hidden") is None

    def test_allows_hidden_intermediate_directory(self) -> None:
        assert _safe_archive_path(".hidden/file.txt") == Path(".hidden/file.txt")

    def test_rejects_only_dots(self) -> None:
        assert _safe_archive_path("..") is None
        assert _safe_archive_path(".") is None

    def test_normalizes_redundant_separators(self) -> None:
        assert _safe_archive_path("dir//file.txt") == Path("dir/file.txt")

    def test_windows_style_separators(self) -> None:
        # PurePosixPath treats backslash as part of the name, not a separator.
        # The result is a single component with a backslash in it.
        result = _safe_archive_path("dir\\file.txt")
        assert result is not None
        assert result.name == "dir\\file.txt"


# ---------------------------------------------------------------------------
# _parse_unpack_zip
# ---------------------------------------------------------------------------


def _make_unpack_zip(attachments: dict[str, bytes] | None = None) -> bytes:
    """Build a fake /unpack/all zip response."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in (attachments or {}).items():
            zf.writestr(name, data)
    return buf.getvalue()


class TestParseUnpackZip:
    """Tests for _parse_unpack_zip()."""

    def test_empty_data(self) -> None:
        assert _parse_unpack_zip(b"") == {}

    def test_extracts_attachments(self) -> None:
        data = _make_unpack_zip({"report.pdf": b"pdf-bytes", "image.png": b"png-bytes"})
        result = _parse_unpack_zip(data)
        assert result["report.pdf"] == b"pdf-bytes"
        assert result["image.png"] == b"png-bytes"
        assert len(result) == 2

    def test_skips_metadata_sidecars(self) -> None:
        data = _make_unpack_zip(
            {
                "report.pdf": b"pdf-bytes",
                "report.pdf.metadata.json": b'{"Author": "Test"}',
            }
        )
        result = _parse_unpack_zip(data)
        assert "report.pdf" in result
        assert "report.pdf.metadata.json" not in result

    def test_long_filename(self) -> None:
        long_name = "a" * 200 + ".pdf"
        data = _make_unpack_zip({long_name: b"data"})
        result = _parse_unpack_zip(data)
        assert result[long_name] == b"data"


# ---------------------------------------------------------------------------
# _find_container_paths
# ---------------------------------------------------------------------------


class TestFindContainerPaths:
    def test_flat_attachments(self) -> None:
        parts = [
            {},
            {_ERP_KEY: "/report.pdf"},
            {_ERP_KEY: "/image.png"},
        ]
        assert _find_container_paths(parts) == set()

    def test_nested_attachments(self) -> None:
        parts = [
            {},
            {_ERP_KEY: "/archive.zip"},
            {_ERP_KEY: "/archive.zip/doc.txt"},
        ]
        assert _find_container_paths(parts) == {"/archive.zip"}

    def test_deeply_nested(self) -> None:
        parts = [
            {},
            {_ERP_KEY: "/a.zip"},
            {_ERP_KEY: "/a.zip/b.tar"},
            {_ERP_KEY: "/a.zip/b.tar/c.txt"},
        ]
        assert _find_container_paths(parts) == {"/a.zip", "/a.zip/b.tar"}


# ---------------------------------------------------------------------------
# TikaExtractor header tests
# ---------------------------------------------------------------------------


class TestTikaHeaders:
    def test_ocr_enabled_sets_language(self):
        ext = TikaExtractor(ocr_enabled=True, ocr_language="deu")
        headers = ext._tika_headers()
        assert headers["X-Tika-OCRLanguage"] == "deu"
        assert "X-Tika-OCRskipOcr" not in headers

    def test_ocr_disabled_sets_skip(self):
        ext = TikaExtractor(ocr_enabled=False)
        headers = ext._tika_headers()
        assert headers["X-Tika-OCRskipOcr"] == "true"
        assert "X-Tika-OCRLanguage" not in headers


# ---------------------------------------------------------------------------
# Fake httpx responses
# ---------------------------------------------------------------------------


def _rmeta_response(parts: list[dict]) -> httpx.Response:
    return httpx.Response(200, json=parts)


def _unpack_response(attachments: dict[str, bytes] | None = None, status: int = 200) -> httpx.Response:
    if status == 204:
        return httpx.Response(204, content=b"")
    return httpx.Response(status, content=_make_unpack_zip(attachments))


# ---------------------------------------------------------------------------
# Full extraction tests
# ---------------------------------------------------------------------------


class TestTikaExtractor:
    def _make_extractor(self, tmp_path: Path) -> TikaExtractor:
        return TikaExtractor(
            server_url="http://localhost:9998",
            extract_dir=str(tmp_path / "extracted"),
        )

    # -- Simple documents --------------------------------------------------

    def test_simple_document_no_embedded(self, tmp_path: Path) -> None:
        """A plain document makes one /rmeta call only — no /unpack."""
        source = tmp_path / "doc.pdf"
        source.write_bytes(b"some content")

        extractor = self._make_extractor(tmp_path)
        resp = _rmeta_response(
            [
                {
                    TIKA_CONTENT_KEY: "Hello world",
                    "dc:title": "My Doc",
                    "Content-Type": "application/pdf",
                }
            ]
        )

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.return_value = resp
            docs = extractor.extract(source)

        assert len(docs) == 1
        assert docs[0].content == "Hello world"
        assert docs[0].metadata["dc:title"] == "My Doc"
        assert TIKA_CONTENT_KEY not in docs[0].metadata
        assert mock_client.put.call_count == 1

    # -- Embedded documents ------------------------------------------------

    def test_document_with_embedded_calls_unpack(self, tmp_path: Path) -> None:
        """A document with embedded files calls /rmeta then /unpack."""
        source = tmp_path / "email.eml"
        source.write_bytes(b"email content")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Email body", "Content-Type": "message/rfc822"},
                {
                    TIKA_CONTENT_KEY: "Attachment text",
                    "resourceName": "report.pdf",
                    _ERP_KEY: "/report.pdf",
                    "Content-Type": "application/pdf",
                },
            ]
        )
        unpack = _unpack_response({"report.pdf": b"pdf-bytes"})

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack]
            docs = extractor.extract(source)

        assert len(docs) == 2
        assert docs[0].content == "Email body"
        assert docs[1].content == "Attachment text"
        assert docs[1].metadata["_aum_extracted_from"] == str(source)
        assert mock_client.put.call_count == 2

    def test_embedded_doc_source_is_unpacked_file(self, tmp_path: Path) -> None:
        """Embedded docs should have source_path pointing to the unpacked file."""
        source = tmp_path / "archive.zip"
        source.write_bytes(b"zip content")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Archive", "Content-Type": "application/zip"},
                {TIKA_CONTENT_KEY: "Inner doc", "resourceName": "inner.txt", _ERP_KEY: "/inner.txt"},
            ]
        )
        unpack = _unpack_response({"inner.txt": b"inner content"})

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack]
            docs = extractor.extract(source)

        assert docs[1].source_path.name == "inner.txt"
        assert docs[1].source_path.read_bytes() == b"inner content"

    # -- Nested (recursive) unpack -----------------------------------------

    def test_nested_attachments_unpacked_recursively(self, tmp_path: Path) -> None:
        """email.eml → archive.zip → doc.txt: all three levels are indexed
        and doc.txt has a downloadable unpacked file."""
        source = tmp_path / "email.eml"
        source.write_bytes(b"email")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Email body"},
                {TIKA_CONTENT_KEY: "Archive listing", "resourceName": "archive.zip", _ERP_KEY: "/archive.zip"},
                {TIKA_CONTENT_KEY: "Deep doc", "resourceName": "doc.txt", _ERP_KEY: "/archive.zip/doc.txt"},
            ]
        )
        # First /unpack on the email → immediate child: archive.zip
        unpack_email = _unpack_response({"archive.zip": b"fake-zip"})
        # Second /unpack on archive.zip → its child: doc.txt
        unpack_archive = _unpack_response({"doc.txt": b"deep content"})

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack_email, unpack_archive]
            docs = extractor.extract(source)

        assert len(docs) == 3
        assert docs[0].content == "Email body"
        assert docs[1].content == "Archive listing"
        assert docs[2].content == "Deep doc"

        # Nested doc has correct display path and extracted_from
        assert docs[2].metadata["_aum_display_path"] == str(source / "archive.zip/doc.txt")
        assert docs[2].metadata["_aum_extracted_from"] == str(source / "archive.zip")

        # Has a real unpacked file
        assert docs[2].source_path.name == "doc.txt"
        assert docs[2].source_path.read_bytes() == b"deep content"

    def test_depth_limit_raises(self, tmp_path: Path) -> None:
        """Exceeding max_depth raises ExtractionDepthError."""
        source = tmp_path / "deep.zip"
        source.write_bytes(b"zip")

        extractor = TikaExtractor(
            server_url="http://localhost:9998",
            extract_dir=str(tmp_path / "extracted"),
            max_depth=1,
        )
        # Nesting: /a.zip/b.zip/c.txt → depth 2 exceeds max_depth=1
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Root"},
                {_ERP_KEY: "/a.zip", "resourceName": "a.zip"},
                {_ERP_KEY: "/a.zip/b.zip", "resourceName": "b.zip"},
                {_ERP_KEY: "/a.zip/b.zip/c.txt", "resourceName": "c.txt", TIKA_CONTENT_KEY: "Deep"},
            ]
        )
        unpack_root = _unpack_response({"a.zip": b"fake"})
        unpack_a = _unpack_response({"b.zip": b"fake2"})
        # b.zip unpack would be depth=2 which exceeds max_depth=1

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack_root, unpack_a, _unpack_response({"c.txt": b"x"})]
            with pytest.raises(ExtractionDepthError):
                extractor.extract(source)

    # -- Unpack failure fallback -------------------------------------------

    def test_unpack_failure_drops_embedded_docs(self, tmp_path: Path) -> None:
        """If /unpack fails (e.g. corrupted archive), embedded parts are
        dropped and a single UnpackError is recorded instead of thousands
        of EmptyExtraction errors."""
        source = tmp_path / "email.eml"
        source.write_bytes(b"email")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Body"},
                {TIKA_CONTENT_KEY: "Attachment", "resourceName": "att.pdf", _ERP_KEY: "/att.pdf"},
            ]
        )
        unpack_fail = httpx.Response(500, text="Internal Server Error")
        errors: list[tuple] = []

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack_fail]
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        # Only the container document is returned
        assert len(docs) == 1
        assert docs[0].content == "Body"
        # A single UnpackError is recorded
        assert len(errors) == 1
        assert errors[0][1] == "UnpackError"
        assert "1 embedded" in errors[0][2]

    def test_unpack_204_no_content(self, tmp_path: Path) -> None:
        """If /unpack returns 204, no attachments are saved."""
        source = tmp_path / "email.eml"
        source.write_bytes(b"email")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Body"},
                {TIKA_CONTENT_KEY: "Inline image", "resourceName": "img.png", _ERP_KEY: "/img.png"},
            ]
        )
        unpack = httpx.Response(204, content=b"")

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack]
            docs = extractor.extract(source)

        assert len(docs) == 2
        assert docs[1].source_path == source  # falls back

    # -- Embedded docs with empty content ------------------------------------

    def test_embedded_doc_with_no_content_still_indexed(self, tmp_path: Path) -> None:
        """Binary attachments (images etc.) with no text are still indexed
        so they appear in the parent's attachment list and are searchable
        by metadata.  They should also be counted as empty."""
        source = tmp_path / "email.eml"
        source.write_bytes(b"email")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Email body"},
                {"resourceName": "photo.jpg", _ERP_KEY: "/photo.jpg", "Content-Type": "image/jpeg"},  # no text
            ]
        )
        unpack = _unpack_response({"photo.jpg": b"jpeg-bytes"})
        errors: list[tuple] = []

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack]
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        assert len(docs) == 2
        assert docs[0].content == "Email body"
        assert docs[1].content == ""
        assert docs[1].metadata["Content-Type"] == "image/jpeg"
        assert docs[1].metadata["_aum_extracted_from"] == str(source)
        # A single aggregated EmptyExtraction error for the container
        assert len(errors) == 1
        assert errors[0][0] == source
        assert errors[0][1] == "EmptyExtraction"
        assert "1 document(s)" in errors[0][2]

    # -- Container metadata with empty content and embedded docs -----------

    def test_container_with_empty_content_and_embedded_docs(self, tmp_path: Path) -> None:
        """Container with no text but embedded docs: container still indexed."""
        source = tmp_path / "email.eml"
        source.write_bytes(b"email content")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {"Content-Type": "message/rfc822"},  # no text
                {TIKA_CONTENT_KEY: "Attachment text", "resourceName": "report.pdf", _ERP_KEY: "/report.pdf"},
            ]
        )
        unpack = _unpack_response({"report.pdf": b"pdf-bytes"})

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack]
            docs = extractor.extract(source)

        assert len(docs) == 2
        assert docs[0].content == ""
        assert docs[0].metadata["Content-Type"] == "message/rfc822"
        assert docs[1].content == "Attachment text"

    # -- Empty file handling -----------------------------------------------

    def test_empty_extraction_records_error(self, tmp_path: Path) -> None:
        """A non-zero file with no text records EmptyExtraction and is still indexed."""
        source = tmp_path / "doc.pdf"
        source.write_bytes(b"some binary content")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response([{"Content-Type": "application/pdf"}])
        errors: list[tuple] = []

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.return_value = rmeta
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        assert len(docs) == 1
        assert docs[0].content == ""
        assert docs[0].metadata["Content-Type"] == "application/pdf"
        assert len(errors) == 1
        assert errors[0][0] == source
        assert errors[0][1] == "EmptyExtraction"
        assert "1 document(s)" in errors[0][2]

    def test_zero_length_file_no_error(self, tmp_path: Path) -> None:
        """A zero-byte file gets a placeholder but no failure recorded."""
        source = tmp_path / "empty.pdf"
        source.write_bytes(b"")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response([{"Content-Type": "application/pdf"}])
        errors: list[tuple] = []

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.return_value = rmeta
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        assert len(docs) == 1
        assert docs[0].content == ""
        assert errors == []

    def test_empty_rmeta_response_treated_as_empty_extraction(self, tmp_path: Path) -> None:
        """Tika returning [] should produce a placeholder, not raise."""
        source = tmp_path / "weird.bin"
        source.write_bytes(b"something")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response([])
        errors: list[tuple] = []

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.return_value = rmeta
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        assert len(docs) == 1
        assert docs[0].content == ""
        assert len(errors) == 1
        assert errors[0][0] == source
        assert errors[0][1] == "EmptyExtraction"
        assert "1 document(s)" in errors[0][2]

    # -- Error handling ----------------------------------------------------

    def test_rmeta_http_error_raises(self, tmp_path: Path) -> None:
        source = tmp_path / "doc.pdf"
        source.write_bytes(b"data")

        extractor = self._make_extractor(tmp_path)

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.return_value = httpx.Response(422, text="Parse error")
            with pytest.raises(ExtractionError, match="HTTP 422"):
                extractor.extract(source)

    def test_rmeta_connection_error_raises(self, tmp_path: Path) -> None:
        source = tmp_path / "doc.pdf"
        source.write_bytes(b"data")

        extractor = self._make_extractor(tmp_path)

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = httpx.ConnectError("connection refused")
            with pytest.raises(ExtractionError, match="request failed"):
                extractor.extract(source)

    def test_tika_internal_keys_stripped(self, tmp_path: Path) -> None:
        """X-TIKA: internal keys should not appear in document metadata."""
        source = tmp_path / "doc.pdf"
        source.write_bytes(b"data")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {
                    TIKA_CONTENT_KEY: "text",
                    "X-TIKA:Parsed-By": "org.apache.tika.parser.DefaultParser",
                    "X-TIKA:parse_time_millis": "42",
                    _ERP_KEY: "/something",
                    "dc:title": "Kept",
                }
            ]
        )

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.return_value = rmeta
            docs = extractor.extract(source)

        assert "dc:title" in docs[0].metadata
        assert TIKA_CONTENT_KEY not in docs[0].metadata
        assert "X-TIKA:Parsed-By" not in docs[0].metadata
        assert "X-TIKA:parse_time_millis" not in docs[0].metadata
        assert _ERP_KEY not in docs[0].metadata

    # -- Content length truncation -----------------------------------------

    def test_content_truncated_when_exceeding_limit(self, tmp_path: Path) -> None:
        source = tmp_path / "huge.txt"
        source.write_bytes(b"x" * 100)

        extractor = TikaExtractor(
            server_url="http://localhost:9998",
            extract_dir=str(tmp_path / "extracted"),
            max_content_length=50,
        )
        rmeta = _rmeta_response([{TIKA_CONTENT_KEY: "a" * 200}])
        errors: list[tuple] = []

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.return_value = rmeta
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        assert len(docs) == 1
        assert len(docs[0].content) == 50
        assert len(errors) == 1
        assert errors[0][1] == "ContentTruncated"
        assert "200" in errors[0][2]

    def test_content_under_limit_not_truncated(self, tmp_path: Path) -> None:
        source = tmp_path / "small.txt"
        source.write_bytes(b"x" * 10)

        extractor = TikaExtractor(
            server_url="http://localhost:9998",
            extract_dir=str(tmp_path / "extracted"),
            max_content_length=500,
        )
        rmeta = _rmeta_response([{TIKA_CONTENT_KEY: "hello world"}])
        errors: list[tuple] = []

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.return_value = rmeta
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        assert docs[0].content == "hello world"
        assert errors == []

    def test_zero_max_content_length_disables_truncation(self, tmp_path: Path) -> None:
        source = tmp_path / "big.txt"
        source.write_bytes(b"x" * 100)

        extractor = TikaExtractor(
            server_url="http://localhost:9998",
            extract_dir=str(tmp_path / "extracted"),
            max_content_length=0,
        )
        long_content = "b" * 10_000
        rmeta = _rmeta_response([{TIKA_CONTENT_KEY: long_content}])

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.return_value = rmeta
            docs = extractor.extract(source)

        assert len(docs[0].content) == 10_000

    # -- Aggregated empty extraction errors --------------------------------

    def test_many_empty_parts_produce_single_error(self, tmp_path: Path) -> None:
        """A container with many empty embedded docs should record a single
        aggregated EmptyExtraction error, not one per part."""
        source = tmp_path / "archive.zip"
        source.write_bytes(b"PK fake zip")

        extractor = self._make_extractor(tmp_path)
        parts = [{TIKA_CONTENT_KEY: "container text"}]
        attachments = {}
        for i in range(50):
            name = f"empty_{i}.bin"
            parts.append({"resourceName": name, _ERP_KEY: f"/{name}"})
            attachments[name] = b"\xff" * 10  # non-zero so EmptyExtraction triggers

        rmeta = _rmeta_response(parts)
        unpack = _unpack_response(attachments)
        errors: list[tuple] = []

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack]
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        assert len(docs) == 51  # container + 50 embedded
        # Only one aggregated EmptyExtraction error for the container
        empty_errors = [e for e in errors if e[1] == "EmptyExtraction"]
        assert len(empty_errors) == 1
        assert empty_errors[0][0] == source
        assert "50 document(s)" in empty_errors[0][2]

    # -- Path preservation in archives --------------------------------------

    def test_same_filename_different_dirs_no_collision(self, tmp_path: Path) -> None:
        """Files with the same name under different archive directories must
        not overwrite each other."""
        source = tmp_path / "archive.zip"
        source.write_bytes(b"zip content")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Archive", "Content-Type": "application/zip"},
                {TIKA_CONTENT_KEY: "Config 1", _ERP_KEY: "/dir1/config.txt"},
                {TIKA_CONTENT_KEY: "Config 2", _ERP_KEY: "/dir2/config.txt"},
            ]
        )
        unpack = _unpack_response(
            {
                "dir1/config.txt": b"config-one",
                "dir2/config.txt": b"config-two",
            }
        )

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack]
            docs = extractor.extract(source)

        assert len(docs) == 3
        # Both files exist on disk with distinct content
        assert docs[1].source_path.read_bytes() == b"config-one"
        assert docs[2].source_path.read_bytes() == b"config-two"
        assert docs[1].source_path != docs[2].source_path

    def test_path_traversal_in_archive_skipped(self, tmp_path: Path) -> None:
        """Archive entries with path traversal components are silently skipped."""
        source = tmp_path / "evil.zip"
        source.write_bytes(b"zip")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Archive"},
                {TIKA_CONTENT_KEY: "Legit", _ERP_KEY: "/legit.txt"},
                {TIKA_CONTENT_KEY: "Evil", _ERP_KEY: "/../../../etc/passwd"},
            ]
        )
        unpack = _unpack_response(
            {
                "legit.txt": b"ok",
                "../../../etc/passwd": b"evil",
            }
        )

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack]
            docs = extractor.extract(source)

        assert len(docs) == 3
        # Legit file extracted normally
        assert docs[1].source_path.read_bytes() == b"ok"
        # Traversal entry rejected — its source_path falls back to the container
        assert docs[2].source_path == source

    def test_subdirectory_structure_preserved_on_disk(self, tmp_path: Path) -> None:
        """Extracted files preserve their archive-internal directory structure."""
        source = tmp_path / "archive.zip"
        source.write_bytes(b"zip content")

        extractor = self._make_extractor(tmp_path)
        rmeta = _rmeta_response(
            [
                {TIKA_CONTENT_KEY: "Archive"},
                {TIKA_CONTENT_KEY: "Deep file", _ERP_KEY: "/a/b/c/deep.txt"},
            ]
        )
        unpack = _unpack_response({"a/b/c/deep.txt": b"deep content"})

        with patch.object(extractor, "_client") as mock_client:
            mock_client.put.side_effect = [rmeta, unpack]
            docs = extractor.extract(source)

        assert len(docs) == 2
        assert docs[1].source_path.read_bytes() == b"deep content"
        # The file should be nested under subdirectories, not flattened
        assert "a" in docs[1].source_path.parts
        assert "b" in docs[1].source_path.parts
        assert "c" in docs[1].source_path.parts
