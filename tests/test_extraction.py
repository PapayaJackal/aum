"""Tests for text extraction helpers."""

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from aum.extraction.base import ExtractionError
from aum.extraction.tika import TikaExtractor, _condense_whitespace, _container_dir


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
        assert len(parts[-3]) == 2   # first shard
        assert len(parts[-2]) == 2   # second shard
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


class TestTikaHeaders:
    """Tests for OCR header propagation."""

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

    def test_skip_embedded_header(self):
        ext = TikaExtractor(ocr_enabled=True, ocr_language="eng")
        headers = ext._tika_headers(skip_embedded=True)
        assert headers["X-Tika-Skip-Embedded"] == "true"
        assert headers["X-Tika-OCRLanguage"] == "eng"

    def test_unpack_passes_headers(self, tmp_path: Path):
        """The _unpack method must pass OCR headers to tika_parse1."""
        source = tmp_path / "doc.pdf"
        source.write_bytes(b"test")

        ext = TikaExtractor(ocr_enabled=True, ocr_language="fra", extract_dir=str(tmp_path / "ex"))

        with patch("aum.extraction.tika.tika_parse1") as mock_parse1:
            mock_parse1.return_value = (200, b"")
            with patch("aum.extraction.tika.tika_unpack._parse", return_value={"content": "text", "metadata": {}}):
                ext._unpack(source)

        mock_parse1.assert_called_once()
        call_kwargs = mock_parse1.call_args
        passed_headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
        # tika_parse1 is called positionally, check kwargs
        if passed_headers is None:
            # All args might be positional — headers are passed as a kwarg
            passed_headers = call_kwargs.kwargs["headers"]
        assert passed_headers["X-Tika-OCRLanguage"] == "fra"


class TestTikaExtractorEmptyContent:
    """Tests for the empty-content detection in TikaExtractor."""

    def _make_extractor(self, tmp_path: Path) -> TikaExtractor:
        return TikaExtractor(
            server_url="http://localhost:9998",
            extract_dir=str(tmp_path / "extracted"),
        )

    def test_nonempty_file_no_text_returns_placeholder_and_records_error(self, tmp_path: Path) -> None:
        """A non-zero-size file that Tika returns no content for should still be
        indexed (with empty content for metadata) and record a failure."""
        source = tmp_path / "doc.pdf"
        source.write_bytes(b"some binary content")

        extractor = self._make_extractor(tmp_path)
        errors: list[tuple] = []

        with (
            patch.object(extractor, "_unpack", return_value=({"content": "", "metadata": {}}, [])),
            patch.object(extractor, "_extract_container", return_value=None),
        ):
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        assert len(docs) == 1
        assert docs[0].content == ""
        assert docs[0].source_path == source
        assert len(errors) == 1
        assert errors[0][1] == "EmptyExtraction"

    def test_zero_length_file_no_text_returns_placeholder_no_error(self, tmp_path: Path) -> None:
        """A zero-byte file with no extracted text gets a placeholder but no failure recorded."""
        source = tmp_path / "empty.pdf"
        source.write_bytes(b"")

        extractor = self._make_extractor(tmp_path)
        errors: list[tuple] = []

        with (
            patch.object(extractor, "_unpack", return_value=({"content": "", "metadata": {}}, [])),
            patch.object(extractor, "_extract_container", return_value=None),
        ):
            docs = extractor.extract(source, record_error=lambda p, et, msg: errors.append((p, et, msg)))

        assert len(docs) == 1
        assert docs[0].content == ""
        assert errors == []

    def test_file_with_text_is_not_affected(self, tmp_path: Path) -> None:
        """Files that produce content are unaffected by the empty-content check."""
        source = tmp_path / "doc.pdf"
        source.write_bytes(b"some binary content")

        extractor = self._make_extractor(tmp_path)

        with (
            patch.object(extractor, "_unpack", return_value=({"content": "Hello world", "metadata": {}}, [])),
            patch.object(extractor, "_extract_container", return_value=None),
        ):
            docs = extractor.extract(source)
            assert len(docs) == 1
            assert docs[0].content == "Hello world"
