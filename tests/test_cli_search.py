"""Tests for the CLI search command."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from click.testing import CliRunner

from aum.cli import main
from aum.search.base import SearchResult


def _make_result(
    *,
    doc_id: str = "doc1",
    source_path: str = "/data/docs/report.pdf",
    display_path: str = "docs/report.pdf",
    score: float = 5.123,
    snippet: str = "some matching text",
    metadata: dict | None = None,
    extracted_from: str = "",
) -> SearchResult:
    return SearchResult(
        doc_id=doc_id,
        source_path=source_path,
        display_path=display_path,
        score=score,
        snippet=snippet,
        metadata=metadata or {},
        extracted_from=extracted_from,
    )


SAMPLE_RESULTS = [
    _make_result(
        doc_id="doc1",
        display_path="docs/report.pdf",
        score=5.123,
        snippet="Russia <mark>is</mark> mentioned here",
        metadata={"File Type": "PDF", "Creator": "Alice", "Created": "2023"},
    ),
    _make_result(
        doc_id="doc2",
        display_path="docs/memo.docx",
        score=3.456,
        snippet="Another <mark>Russia</mark> reference",
        metadata={"File Type": "Word", "Creator": "Bob"},
    ),
]

SAMPLE_FACETS = {
    "File Type": ["PDF", "Word"],
    "Creator": ["Alice", "Bob"],
    "Created": ["2022", "2023", "2024"],
}


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def mock_backend():
    backend = MagicMock()
    backend.search_text.return_value = (SAMPLE_RESULTS, 2, None)
    backend.search_vector.return_value = (SAMPLE_RESULTS, 2, None)
    backend.search_hybrid.return_value = (SAMPLE_RESULTS, 2, None)
    return backend


@pytest.fixture
def _patch_cli(monkeypatch, tmp_path, mock_backend):
    """Patch config loading and search backend for all CLI search tests."""
    monkeypatch.setenv("AUM_DATA_DIR", str(tmp_path))
    monkeypatch.setattr("aum.api.deps.make_search_backend", lambda *a, **kw: mock_backend)


# --- Basic search ---


class TestSearchBasic:
    @pytest.fixture(autouse=True)
    def setup(self, _patch_cli):
        pass

    def test_text_search_returns_results(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia"])
        assert result.exit_code == 0
        assert "docs/report.pdf" in result.output
        assert "docs/memo.docx" in result.output
        mock_backend.search_text.assert_called_once()

    def test_unpacks_tuple_correctly(self, runner, mock_backend):
        """The bug that originally broke CLI search: not unpacking the 3-tuple."""
        mock_backend.search_text.return_value = (
            [_make_result(score=1.0, snippet="test")],
            1,
            None,
        )
        result = runner.invoke(main, ["search", "test"])
        assert result.exit_code == 0
        assert "[1.000]" in result.output

    def test_no_results(self, runner, mock_backend):
        mock_backend.search_text.return_value = ([], 0, None)
        result = runner.invoke(main, ["search", "nonexistent"])
        assert result.exit_code == 0
        assert "No results found" in result.output

    def test_result_count_display(self, runner):
        result = runner.invoke(main, ["search", "russia"])
        assert result.exit_code == 0
        assert "Showing 1-2 of 2 results" in result.output

    def test_score_displayed(self, runner):
        result = runner.invoke(main, ["search", "russia"])
        assert "[5.123]" in result.output
        assert "[3.456]" in result.output

    def test_display_path_used(self, runner):
        result = runner.invoke(main, ["search", "russia"])
        assert "docs/report.pdf" in result.output
        # Should not show the full source_path
        assert "/data/docs/report.pdf" not in result.output

    def test_falls_back_to_source_path(self, runner, mock_backend):
        mock_backend.search_text.return_value = (
            [_make_result(display_path="", source_path="/data/fallback.txt")],
            1,
            None,
        )
        result = runner.invoke(main, ["search", "test"])
        assert "/data/fallback.txt" in result.output


# --- HTML stripping ---


class TestSnippetDisplay:
    @pytest.fixture(autouse=True)
    def setup(self, _patch_cli):
        pass

    def test_strips_mark_tags(self, runner):
        result = runner.invoke(main, ["search", "russia"])
        assert "<mark>" not in result.output
        assert "</mark>" not in result.output
        assert "Russia is mentioned here" in result.output

    def test_truncates_long_snippets(self, runner, mock_backend):
        long_snippet = "x" * 300
        mock_backend.search_text.return_value = (
            [_make_result(snippet=long_snippet)],
            1,
            None,
        )
        result = runner.invoke(main, ["search", "test"])
        assert "..." in result.output
        # Should not contain the full 300 chars
        assert "x" * 201 not in result.output


# --- Metadata display ---


class TestMetadataDisplay:
    @pytest.fixture(autouse=True)
    def setup(self, _patch_cli):
        pass

    def test_shows_metadata_inline(self, runner):
        result = runner.invoke(main, ["search", "russia"])
        assert "File Type: PDF" in result.output
        assert "Creator: Alice" in result.output
        assert "Created: 2023" in result.output

    def test_no_metadata_no_bracket_line(self, runner, mock_backend):
        mock_backend.search_text.return_value = (
            [_make_result(metadata={})],
            1,
            None,
        )
        result = runner.invoke(main, ["search", "test"])
        # No metadata bracket line
        assert "[File Type" not in result.output

    def test_list_metadata_joined(self, runner, mock_backend):
        mock_backend.search_text.return_value = (
            [_make_result(metadata={"Creator": ["Alice", "Bob"]})],
            1,
            None,
        )
        result = runner.invoke(main, ["search", "test"])
        assert "Creator: Alice, Bob" in result.output


# --- Pagination ---


class TestPagination:
    @pytest.fixture(autouse=True)
    def setup(self, _patch_cli):
        pass

    def test_offset_option(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--offset", "10"])
        assert result.exit_code == 0
        assert "Showing 11-12 of 2 results" in result.output
        mock_backend.search_text.assert_called_once_with(
            "russia", limit=20, offset=10, include_facets=False, filters=None, sort=None
        )

    def test_limit_option(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--limit", "5"])
        assert result.exit_code == 0
        mock_backend.search_text.assert_called_once_with(
            "russia", limit=5, offset=0, include_facets=False, filters=None, sort=None
        )

    def test_result_numbering_with_offset(self, runner, mock_backend):
        mock_backend.search_text.return_value = (
            [_make_result(score=1.0)],
            50,
            None,
        )
        result = runner.invoke(main, ["search", "test", "--offset", "20"])
        assert "21." in result.output
        assert "Showing 21-21 of 50 results" in result.output


# --- Facets ---


class TestFacets:
    @pytest.fixture(autouse=True)
    def setup(self, _patch_cli):
        pass

    def test_facets_hidden_by_default(self, runner, mock_backend):
        mock_backend.search_text.return_value = (SAMPLE_RESULTS, 2, SAMPLE_FACETS)
        result = runner.invoke(main, ["search", "russia"])
        assert "Available Facets" not in result.output
        # include_facets should be False when --show-facets not given
        mock_backend.search_text.assert_called_once_with(
            "russia", limit=20, offset=0, include_facets=False, filters=None, sort=None
        )

    def test_show_facets_flag(self, runner, mock_backend):
        mock_backend.search_text.return_value = (SAMPLE_RESULTS, 2, SAMPLE_FACETS)
        result = runner.invoke(main, ["search", "russia", "--show-facets"])
        assert result.exit_code == 0
        assert "--- Available Facets ---" in result.output
        assert "File Type:" in result.output
        assert "- PDF" in result.output
        assert "- Word" in result.output
        assert "Creator:" in result.output
        assert "- Alice" in result.output
        assert "Created:" in result.output
        assert "- 2023" in result.output
        mock_backend.search_text.assert_called_once_with(
            "russia", limit=20, offset=0, include_facets=True, filters=None, sort=None
        )

    def test_show_facets_no_facets_returned(self, runner, mock_backend):
        mock_backend.search_text.return_value = (SAMPLE_RESULTS, 2, None)
        result = runner.invoke(main, ["search", "russia", "--show-facets"])
        assert result.exit_code == 0
        assert "Available Facets" not in result.output


# --- Filters ---


class TestFilters:
    @pytest.fixture(autouse=True)
    def setup(self, _patch_cli):
        pass

    def test_file_type_filter(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--file-type", "PDF"])
        assert result.exit_code == 0
        mock_backend.search_text.assert_called_once_with(
            "russia",
            limit=20,
            offset=0,
            include_facets=False,
            filters={"File Type": ["PDF"]},
            sort=None,
        )

    def test_multiple_file_type_filters(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--file-type", "PDF", "--file-type", "Word"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["filters"]["File Type"] == ["PDF", "Word"]

    def test_creator_filter(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--creator", "Alice"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["filters"]["Creator"] == ["Alice"]

    def test_email_filter(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--email", "alice@example.com"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["filters"]["Email Addresses"] == ["alice@example.com"]

    def test_created_from_filter(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--created-from", "2020"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["filters"]["Created"] == ["2020", "2099"]

    def test_created_to_filter(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--created-to", "2023"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["filters"]["Created"] == ["1900", "2023"]

    def test_created_range_filter(self, runner, mock_backend):
        result = runner.invoke(
            main,
            ["search", "russia", "--created-from", "2020", "--created-to", "2023"],
        )
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["filters"]["Created"] == ["2020", "2023"]

    def test_combined_filters(self, runner, mock_backend):
        result = runner.invoke(
            main,
            [
                "search",
                "russia",
                "--file-type",
                "PDF",
                "--creator",
                "Alice",
                "--created-from",
                "2020",
            ],
        )
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        filters = call_kwargs.kwargs["filters"]
        assert filters["File Type"] == ["PDF"]
        assert filters["Creator"] == ["Alice"]
        assert filters["Created"] == ["2020", "2099"]

    def test_no_filters_passes_none(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["filters"] is None


# --- Search type ---


class TestSearchType:
    @pytest.fixture(autouse=True)
    def setup(self, _patch_cli):
        pass

    def test_default_is_text(self, runner, mock_backend):
        runner.invoke(main, ["search", "russia"])
        mock_backend.search_text.assert_called_once()
        mock_backend.search_vector.assert_not_called()
        mock_backend.search_hybrid.assert_not_called()

    def test_invalid_search_type(self, runner):
        result = runner.invoke(main, ["search", "russia", "--type", "invalid"])
        assert result.exit_code != 0

    def test_index_option(self, runner, mock_backend):
        with patch("aum.api.deps.make_search_backend", return_value=mock_backend) as make_mock:
            runner.invoke(main, ["search", "russia", "--index", "my-index"])
            make_mock.assert_called_once()
            call_args = make_mock.call_args
            assert call_args.kwargs.get("index") == "my-index" or call_args[1].get("index") == "my-index"


# --- Multi-index search ---


class TestMultiIndex:
    @pytest.fixture(autouse=True)
    def setup(self, _patch_cli):
        pass

    def test_multiple_index_options_joined(self, runner, mock_backend):
        with patch("aum.api.deps.make_search_backend", return_value=mock_backend) as make_mock:
            result = runner.invoke(main, ["search", "russia", "--index", "idx1", "--index", "idx2"])
            assert result.exit_code == 0
            make_mock.assert_called_once()
            call_args = make_mock.call_args
            idx = call_args.kwargs.get("index") or call_args[1].get("index")
            assert idx == "idx1,idx2"

    def test_multi_index_shows_index_per_result(self, runner, mock_backend):
        results_with_index = [
            SearchResult(
                doc_id="d1",
                source_path="/a",
                display_path="a.pdf",
                score=5.0,
                snippet="text",
                metadata={},
                index="idx1",
            ),
            SearchResult(
                doc_id="d2",
                source_path="/b",
                display_path="b.pdf",
                score=4.0,
                snippet="text",
                metadata={},
                index="idx2",
            ),
        ]
        mock_backend.search_text.return_value = (results_with_index, 2, None)
        result = runner.invoke(main, ["search", "test", "--index", "idx1", "--index", "idx2"])
        assert result.exit_code == 0
        assert "[idx1]" in result.output
        assert "[idx2]" in result.output

    def test_single_index_no_index_prefix(self, runner, mock_backend):
        results_with_index = [
            SearchResult(
                doc_id="d1",
                source_path="/a",
                display_path="a.pdf",
                score=5.0,
                snippet="text",
                metadata={},
                index="myidx",
            ),
        ]
        mock_backend.search_text.return_value = (results_with_index, 1, None)
        result = runner.invoke(main, ["search", "test", "--index", "myidx"])
        assert result.exit_code == 0
        # Single index: no [myidx] prefix shown
        assert "[myidx]" not in result.output

    def test_hybrid_multi_index_no_embeddings(self, runner, mock_backend, monkeypatch, tmp_path):
        mock_tracker = MagicMock()
        mock_tracker.get_embedding_model.return_value = None
        monkeypatch.setattr("aum.api.deps.make_tracker", lambda *a, **kw: mock_tracker)

        result = runner.invoke(main, ["search", "test", "--type", "hybrid", "--index", "idx1", "--index", "idx2"])
        assert result.exit_code != 0
        assert (
            "no embeddings found for index 'idx1'" in result.output.lower() or "no embeddings" in result.output.lower()
        )

    def test_hybrid_multi_index_model_mismatch(self, runner, mock_backend, monkeypatch, tmp_path):
        mock_tracker = MagicMock()
        mock_tracker.get_embedding_model.side_effect = [
            ("model-a", "ollama", 768),
            ("model-b", "ollama", 768),
        ]
        monkeypatch.setattr("aum.api.deps.make_tracker", lambda *a, **kw: mock_tracker)

        result = runner.invoke(main, ["search", "test", "--type", "hybrid", "--index", "idx1", "--index", "idx2"])
        assert result.exit_code != 0
        assert "mismatch" in result.output.lower()


# --- Sort ---


class TestSort:
    @pytest.fixture(autouse=True)
    def setup(self, _patch_cli):
        pass

    def test_no_sort_passes_none(self, runner, mock_backend):
        runner.invoke(main, ["search", "russia"])
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["sort"] is None

    def test_sort_date_desc(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--sort", "date:desc"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["sort"] == "date:desc"

    def test_sort_date_asc(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--sort", "date:asc"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["sort"] == "date:asc"

    def test_sort_size_desc(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--sort", "size:desc"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["sort"] == "size:desc"

    def test_sort_size_asc(self, runner, mock_backend):
        result = runner.invoke(main, ["search", "russia", "--sort", "size:asc"])
        assert result.exit_code == 0
        call_kwargs = mock_backend.search_text.call_args
        assert call_kwargs.kwargs["sort"] == "size:asc"

    def test_invalid_sort_value(self, runner):
        result = runner.invoke(main, ["search", "russia", "--sort", "invalid"])
        assert result.exit_code != 0
