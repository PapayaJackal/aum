"""Tests for the Meilisearch search backend.

All tests mock the meilisearch.Client so no running Meilisearch instance is
needed.  The suite covers:
  - Metadata extraction parity with the Elasticsearch backend
  - Filter string generation
  - Facet distribution parsing
  - Document body construction
  - Hit / result parsing
  - initialize(), index_batch(), search_text(), search_vector(),
    search_hybrid(), delete_index(), document_count(), get_document(),
    find_attachments(), find_by_display_path(), list_indices(),
    count_unembedded(), scroll_unembedded(), scroll_document_ids(),
    update_embeddings()
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from aum.models import Document
from aum.search.meilisearch import (
    MeilisearchBackend,
    _build_doc_body,
    _build_filter_string,
    _escape_filter_value,
    _extract_indexed_meta,
    _parse_facet_distribution,
    _parse_hit,
)


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------


def _make_document(
    *,
    content: str = "hello world",
    source_path: str = "/data/file.pdf",
    metadata: dict | None = None,
) -> Document:
    if metadata is None:
        metadata = {
            "_aum_display_path": "file.pdf",
            "_aum_extracted_from": "",
            "Content-Type": "application/pdf",
            "dc:creator": "Alice",
            "dcterms:created": "2023-06-15T00:00:00Z",
        }
    return Document(source_path=Path(source_path), content=content, metadata=metadata)


def _make_task(uid: int = 1) -> MagicMock:
    t = MagicMock()
    t.task_uid = uid
    return t


def _make_task_result(status: str = "succeeded") -> MagicMock:
    r = MagicMock()
    r.status = status
    r.error = None
    return r


@pytest.fixture()
def mock_client():
    """Patch meilisearch.Client and return the mock instance."""
    with patch("aum.search.meilisearch.meilisearch.Client") as MockClient:
        client = MockClient.return_value
        client.wait_for_task.return_value = _make_task_result()
        client.create_index.return_value = _make_task()
        client.delete_index.return_value = _make_task()
        client.update_experimental_features.return_value = {}
        yield client


@pytest.fixture()
def mock_index(mock_client: MagicMock) -> MagicMock:
    """Return the mock Index that mock_client.index() returns."""
    idx = MagicMock()
    mock_client.index.return_value = idx

    # Default successful task for any mutating operation
    idx.add_documents.return_value = _make_task()
    idx.update_documents.return_value = _make_task()
    idx.update_settings.return_value = _make_task()

    # Default empty search response
    idx.search.return_value = {"hits": [], "estimatedTotalHits": 0}

    return idx


@pytest.fixture()
def backend(mock_client: MagicMock, mock_index: MagicMock) -> MeilisearchBackend:
    return MeilisearchBackend(url="http://localhost:7700", api_key="", index="test")


# ---------------------------------------------------------------------------
# _extract_indexed_meta – parity with elasticsearch.py
# ---------------------------------------------------------------------------


class TestExtractIndexedMeta:
    def test_content_type_extracted(self):
        meta = _extract_indexed_meta({"Content-Type": "application/pdf"})
        assert meta["content_type"] == "application/pdf"

    def test_content_type_parameters_stripped(self):
        meta = _extract_indexed_meta({"Content-Type": "text/html; charset=UTF-8"})
        assert meta["content_type"] == "text/html"

    def test_creator_fallback_chain(self):
        # dc:creator wins over Author
        meta = _extract_indexed_meta({"dc:creator": "Alice", "Author": "Bob"})
        assert meta["creator"] == "Alice"

    def test_creator_fallback_to_author(self):
        meta = _extract_indexed_meta({"Author": "Bob"})
        assert meta["creator"] == "Bob"

    def test_created_date(self):
        meta = _extract_indexed_meta({"dcterms:created": "2023-06-15T00:00:00Z"})
        assert meta["created"] == "2023-06-15T00:00:00Z"

    def test_modified_date(self):
        meta = _extract_indexed_meta({"dcterms:modified": "2024-01-01"})
        assert meta["modified"] == "2024-01-01"

    def test_email_addresses_merged_and_deduplicated(self):
        meta = _extract_indexed_meta(
            {
                "Message-From": "Alice <alice@example.com>",
                "Message-To": "Bob <bob@example.com>",
                "Message-CC": "alice@example.com",  # duplicate
            }
        )
        assert set(meta["email_addresses"]) == {"alice@example.com", "bob@example.com"}  # type: ignore[arg-type]
        assert len(meta["email_addresses"]) == 2  # type: ignore[arg-type]

    def test_email_addresses_normalised_to_lowercase(self):
        meta = _extract_indexed_meta({"Message-From": "USER@EXAMPLE.COM"})
        assert meta["email_addresses"] == ["user@example.com"]  # type: ignore[comparison-overlap]

    def test_invalid_email_header_skipped(self):
        meta = _extract_indexed_meta({"Message-To": "undisclosed-recipients:;"})
        assert "email_addresses" not in meta

    def test_missing_fields_not_included(self):
        meta = _extract_indexed_meta({"Content-Type": "text/plain"})
        assert "creator" not in meta
        assert "email_addresses" not in meta


# ---------------------------------------------------------------------------
# _build_filter_string
# ---------------------------------------------------------------------------


class TestBuildFilterString:
    def test_empty_filters(self):
        assert _build_filter_string({}) == ""

    def test_file_type_filter_maps_alias_to_mime(self):
        f = _build_filter_string({"File Type": ["PDF"]})
        assert "application/pdf" in f
        assert "meta_content_type" in f

    def test_file_type_filter_in_syntax(self):
        f = _build_filter_string({"File Type": ["Word"]})
        assert "IN [" in f

    def test_creator_filter(self):
        f = _build_filter_string({"Creator": ["Alice", "Bob"]})
        assert 'meta_creator IN ["Alice", "Bob"]' == f

    def test_email_filter(self):
        f = _build_filter_string({"Email Addresses": ["alice@example.com"]})
        assert 'meta_email_addresses IN ["alice@example.com"]' == f

    def test_date_filter_range(self):
        f = _build_filter_string({"Created": ["2020", "2023"]})
        # No quotes: meta_created_year is stored as an integer, not a string.
        assert "meta_created_year >= 2020" in f
        assert "meta_created_year <= 2023" in f
        assert " AND " in f

    def test_date_filter_single_year(self):
        f = _build_filter_string({"Created": ["2021"]})
        assert "meta_created_year >= 2021" in f
        assert "<=" not in f

    def test_multiple_facets_joined_with_and(self):
        f = _build_filter_string({"Creator": ["Alice"], "Email Addresses": ["alice@example.com"]})
        assert " AND " in f

    def test_unknown_facet_ignored(self):
        f = _build_filter_string({"UnknownFacet": ["value"]})
        assert f == ""

    def test_empty_values_ignored(self):
        f = _build_filter_string({"Creator": []})
        assert f == ""

    def test_unknown_file_type_alias_passed_through(self):
        f = _build_filter_string({"File Type": ["application/x-custom"]})
        assert "application/x-custom" in f

    def test_creator_value_with_double_quote_escaped(self):
        f = _build_filter_string({"Creator": ['O"Brien']})
        assert r"O\"Brien" in f

    def test_creator_value_with_backslash_escaped(self):
        f = _build_filter_string({"Creator": [r"C:\Users\Alice"]})
        assert r"C:\\Users\\Alice" in f


# ---------------------------------------------------------------------------
# _escape_filter_value
# ---------------------------------------------------------------------------


class TestEscapeFilterValue:
    def test_double_quote_escaped(self):
        assert _escape_filter_value('say "hello"') == r"say \"hello\""

    def test_backslash_escaped(self):
        assert _escape_filter_value("a\\b") == "a\\\\b"

    def test_backslash_before_quote_both_escaped(self):
        # Backslash must be escaped before the quote so the result is \\"
        # (two-char escaped backslash) followed by \" (escaped quote).
        result = _escape_filter_value('\\"')  # input: backslash + double-quote
        assert result.startswith("\\\\")  # leading \\ = escaped backslash
        assert result.endswith('\\"')  # trailing \" = escaped quote
        assert len(result) == 4

    def test_plain_string_unchanged(self):
        assert _escape_filter_value("alice@example.com") == "alice@example.com"

    def test_empty_string(self):
        assert _escape_filter_value("") == ""


# ---------------------------------------------------------------------------
# _parse_facet_distribution
# ---------------------------------------------------------------------------


class TestParseFacetDistribution:
    def test_file_type_aliased(self):
        dist = {"meta_content_type": {"application/pdf": 5, "text/plain": 2}}
        facets = _parse_facet_distribution(dist)
        assert "File Type" in facets
        assert "PDF" in facets["File Type"]
        assert "Plain Text" in facets["File Type"]

    def test_creator_values_sorted(self):
        dist = {"meta_creator": {"Bob": 1, "Alice": 3}}
        facets = _parse_facet_distribution(dist)
        assert facets["Creator"] == ["Alice", "Bob"]

    def test_date_years_sorted(self):
        dist = {"meta_created_year": {"2023": 4, "2021": 2, "2022": 1}}
        facets = _parse_facet_distribution(dist)
        assert facets["Created"] == ["2021", "2022", "2023"]

    def test_email_addresses(self):
        dist = {"meta_email_addresses": {"alice@example.com": 2, "bob@example.com": 1}}
        facets = _parse_facet_distribution(dist)
        assert "Email Addresses" in facets
        assert sorted(facets["Email Addresses"]) == ["alice@example.com", "bob@example.com"]

    def test_zero_count_bucket_excluded(self):
        dist = {"meta_creator": {"Alice": 0, "Bob": 3}}
        facets = _parse_facet_distribution(dist)
        assert "Alice" not in facets.get("Creator", [])

    def test_empty_bucket_excluded(self):
        dist = {"meta_creator": {"": 5, "Alice": 2}}
        facets = _parse_facet_distribution(dist)
        assert "" not in facets.get("Creator", [])

    def test_missing_field_absent_from_result(self):
        facets = _parse_facet_distribution({})
        assert facets == {}


# ---------------------------------------------------------------------------
# _build_doc_body
# ---------------------------------------------------------------------------


class TestBuildDocBody:
    def test_primary_key_is_id(self):
        doc = _make_document()
        body = _build_doc_body("abc123", doc)
        assert body["id"] == "abc123"

    def test_flat_meta_fields_extracted(self):
        doc = _make_document()
        body = _build_doc_body("x", doc)
        assert body["meta_content_type"] == "application/pdf"
        assert body["meta_creator"] == "Alice"
        # Stored as integer so Meilisearch range operators (>=, <=) work correctly.
        assert body["meta_created_year"] == 2023

    def test_display_path_from_metadata(self):
        doc = _make_document()
        body = _build_doc_body("x", doc)
        assert body["display_path"] == "file.pdf"

    def test_has_embeddings_defaults_false(self):
        doc = _make_document()
        body = _build_doc_body("x", doc)
        assert body["has_embeddings"] is False

    def test_metadata_stored_as_json_string(self):
        doc = _make_document()
        body = _build_doc_body("x", doc)
        import json

        parsed = json.loads(body["metadata_json"])
        assert "Content-Type" in parsed

    def test_content_included(self):
        doc = _make_document(content="The quick brown fox")
        body = _build_doc_body("x", doc)
        assert body["content"] == "The quick brown fox"

    def test_source_path_as_string(self):
        doc = _make_document(source_path="/data/docs/file.pdf")
        body = _build_doc_body("x", doc)
        assert body["source_path"] == "/data/docs/file.pdf"

    def test_vectors_opt_out_present(self):
        """Documents must explicitly opt out of _vectors.custom to avoid errors when embedder is later added."""
        doc = _make_document()
        body = _build_doc_body("x", doc)
        assert "_vectors" in body
        assert body["_vectors"]["custom"] is None


# ---------------------------------------------------------------------------
# _parse_hit – the inverse of _build_doc_body
# ---------------------------------------------------------------------------


class TestParseHit:
    def _hit(self, **kwargs) -> dict:
        import json

        base: dict = {
            "id": "doc1",
            "source_path": "/data/file.pdf",
            "display_path": "file.pdf",
            "extracted_from": "",
            "content": "full document text",
            "metadata_json": json.dumps({"Content-Type": "application/pdf"}),
            "has_embeddings": False,
            "meta_content_type": "application/pdf",
            "meta_creator": "Alice",
            "meta_created_year": 2023,  # integer, matching what Meilisearch returns
            "_rankingScore": 0.85,
        }
        base.update(kwargs)
        return base

    def test_doc_id_mapped(self):
        r = _parse_hit(self._hit())
        assert r.doc_id == "doc1"

    def test_score_from_ranking_score(self):
        r = _parse_hit(self._hit())
        assert r.score == pytest.approx(0.85)

    def test_snippet_from_formatted_content(self):
        hit = self._hit(_formatted={"content": "...quick <mark>brown</mark> fox..."})
        r = _parse_hit(hit)
        assert "<mark>brown</mark>" in r.snippet

    def test_snippet_fallback_to_raw_content(self):
        r = _parse_hit(self._hit())
        assert r.snippet == "full document text"

    def test_snippet_truncated_to_200_chars_when_no_formatted(self):
        r = _parse_hit(self._hit(content="x" * 300))
        assert len(r.snippet) == 200

    def test_display_path_highlighted_from_formatted(self):
        hit = self._hit(_formatted={"display_path": "<mark>file</mark>.pdf", "content": ""})
        r = _parse_hit(hit)
        assert r.display_path_highlighted == "<mark>file</mark>.pdf"

    def test_display_path_highlighted_empty_when_no_formatted(self):
        r = _parse_hit(self._hit())
        assert r.display_path_highlighted == ""

    def test_file_type_aliased_in_metadata(self):
        r = _parse_hit(self._hit())
        assert r.metadata["File Type"] == "PDF"

    def test_creator_injected_in_metadata(self):
        r = _parse_hit(self._hit())
        assert r.metadata["Creator"] == "Alice"

    def test_created_year_injected_in_metadata(self):
        r = _parse_hit(self._hit())
        assert r.metadata["Created"] == "2023"

    def test_email_addresses_injected(self):
        hit = self._hit(meta_email_addresses=["a@b.com"])
        r = _parse_hit(hit)
        assert r.metadata["Email Addresses"] == ["a@b.com"]

    def test_index_name_stored(self):
        r = _parse_hit(self._hit(), index_name="my-index")
        assert r.index == "my-index"

    def test_raw_metadata_keys_preserved(self):
        import json

        hit = self._hit(metadata_json=json.dumps({"Content-Type": "application/pdf", "dc:title": "My Doc"}))
        r = _parse_hit(hit)
        assert r.metadata["dc:title"] == "My Doc"


# ---------------------------------------------------------------------------
# initialize()
# ---------------------------------------------------------------------------


class TestInitialize:
    def test_creates_index_when_not_found(self, backend: MeilisearchBackend, mock_client: MagicMock):
        mock_client.get_index.side_effect = _make_api_error("index_not_found")
        backend.initialize()
        mock_client.create_index.assert_called_once_with("test", {"primaryKey": "id"})

    def test_applies_settings_on_new_index(
        self, backend: MeilisearchBackend, mock_client: MagicMock, mock_index: MagicMock
    ):
        mock_client.get_index.side_effect = _make_api_error("index_not_found")
        backend.initialize()
        mock_index.update_settings.assert_called_once()
        settings = mock_index.update_settings.call_args[0][0]
        assert "display_path" in settings["searchableAttributes"]
        assert "content" in settings["searchableAttributes"]
        assert "has_embeddings" in settings["filterableAttributes"]

    def test_no_recreation_when_index_exists(
        self, backend: MeilisearchBackend, mock_client: MagicMock, mock_index: MagicMock
    ):
        mock_client.get_index.return_value = MagicMock()
        backend.initialize()
        mock_client.create_index.assert_not_called()
        mock_client.delete_index.assert_not_called()

    def test_always_updates_settings_even_when_index_exists(
        self, backend: MeilisearchBackend, mock_client: MagicMock, mock_index: MagicMock
    ):
        mock_client.get_index.return_value = MagicMock()
        backend.initialize()
        mock_index.update_settings.assert_called_once()

    def test_updates_settings_in_place_without_deletion(
        self, backend: MeilisearchBackend, mock_client: MagicMock, mock_index: MagicMock
    ):
        """Adding new filterable attributes or embedders never deletes the index."""
        mock_client.get_index.return_value = MagicMock()
        backend.initialize(vector_dimension=1024)
        mock_client.delete_index.assert_not_called()
        settings = mock_index.update_settings.call_args[0][0]
        assert settings["embedders"]["custom"]["dimensions"] == 1024

    def test_embedder_config_included_when_vector_dimension_set(
        self, backend: MeilisearchBackend, mock_client: MagicMock, mock_index: MagicMock
    ):
        mock_client.get_index.side_effect = _make_api_error("index_not_found")
        backend.initialize(vector_dimension=512)
        settings = mock_index.update_settings.call_args[0][0]
        assert settings["embedders"]["custom"]["dimensions"] == 512
        assert settings["embedders"]["custom"]["source"] == "userProvided"

    def test_no_embedder_config_without_vector_dimension(
        self, backend: MeilisearchBackend, mock_client: MagicMock, mock_index: MagicMock
    ):
        mock_client.get_index.side_effect = _make_api_error("index_not_found")
        backend.initialize()
        settings = mock_index.update_settings.call_args[0][0]
        assert "embedders" not in settings


# ---------------------------------------------------------------------------
# index_batch()
# ---------------------------------------------------------------------------


class TestIndexBatch:
    def test_empty_batch_returns_no_failures(self, backend: MeilisearchBackend):
        assert backend.index_batch([]) == []

    def test_documents_submitted_to_meilisearch(self, backend: MeilisearchBackend, mock_index: MagicMock):
        doc = _make_document()
        backend.index_batch([("id1", doc)])
        mock_index.add_documents.assert_called_once()
        submitted = mock_index.add_documents.call_args[0][0]
        assert len(submitted) == 1
        assert submitted[0]["id"] == "id1"

    def test_returns_empty_on_success(self, backend: MeilisearchBackend, mock_index: MagicMock):
        doc = _make_document()
        failures = backend.index_batch([("id1", doc)])
        assert failures == []

    def test_returns_all_ids_on_task_failure(
        self, backend: MeilisearchBackend, mock_client: MagicMock, mock_index: MagicMock
    ):
        mock_client.wait_for_task.return_value = _make_task_result("failed")
        doc = _make_document()
        failures = backend.index_batch([("id1", doc), ("id2", doc)])
        assert {f[0] for f in failures} == {"id1", "id2"}

    def test_primary_key_passed_to_add_documents(self, backend: MeilisearchBackend, mock_index: MagicMock):
        backend.index_batch([("id1", _make_document())])
        _, kwargs = mock_index.add_documents.call_args
        assert kwargs.get("primary_key") == "id"


# ---------------------------------------------------------------------------
# search_text()
# ---------------------------------------------------------------------------


def _make_hit(doc_id: str = "doc1", score: float = 0.9, content: str = "sample content") -> dict:
    import json

    return {
        "id": doc_id,
        "source_path": "/data/file.pdf",
        "display_path": "file.pdf",
        "extracted_from": "",
        "content": content,
        "metadata_json": json.dumps({"Content-Type": "text/plain"}),
        "has_embeddings": False,
        "meta_content_type": "text/plain",
        "_rankingScore": score,
    }


class TestSearchText:
    def test_returns_search_results(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("doc1", 0.9)],
            "estimatedTotalHits": 1,
        }
        results, total, facets = backend.search_text("hello")
        assert len(results) == 1
        assert results[0].doc_id == "doc1"
        assert total == 1

    def test_returns_total_count(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 42}
        _, total, _ = backend.search_text("x")
        assert total == 42

    def test_facets_none_when_not_requested(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        _, _, facets = backend.search_text("x", include_facets=False)
        assert facets is None

    def test_facets_included_when_requested(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [],
            "estimatedTotalHits": 0,
            "facetDistribution": {
                "meta_content_type": {"application/pdf": 3},
            },
        }
        _, _, facets = backend.search_text("x", include_facets=True)
        assert facets is not None
        assert "File Type" in facets
        assert "PDF" in facets["File Type"]

    def test_facet_fields_requested_from_meilisearch(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_text("x", include_facets=True)
        params = mock_index.search.call_args[0][1]
        assert "facets" in params
        assert "meta_content_type" in params["facets"]

    def test_highlight_params_included(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_text("x")
        params = mock_index.search.call_args[0][1]
        assert params["highlightPreTag"] == "<mark>"
        assert params["highlightPostTag"] == "</mark>"
        assert "content" in params["attributesToHighlight"]

    def test_filter_passed_to_meilisearch(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_text("x", filters={"Creator": ["Alice"]})
        params = mock_index.search.call_args[0][1]
        assert "filter" in params
        assert "Alice" in params["filter"]

    def test_limit_and_offset_passed(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_text("x", limit=5, offset=10)
        params = mock_index.search.call_args[0][1]
        assert params["limit"] == 5
        assert params["offset"] == 10

    def test_score_in_result(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("d1", 0.75)],
            "estimatedTotalHits": 1,
        }
        results, _, _ = backend.search_text("x")
        assert results[0].score == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# search_vector()
# ---------------------------------------------------------------------------


class TestSearchVector:
    def test_hybrid_param_with_ratio_1(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_vector([0.1, 0.2, 0.3])
        params = mock_index.search.call_args[0][1]
        assert params["hybrid"]["semanticRatio"] == 1.0
        assert params["hybrid"]["embedder"] == "custom"

    def test_vector_param_forwarded(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        vec = [0.1, 0.2, 0.3]
        backend.search_vector(vec)
        params = mock_index.search.call_args[0][1]
        assert params["vector"] == vec

    def test_empty_query_string_used(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_vector([0.1, 0.2])
        query = mock_index.search.call_args[0][0]
        assert query == ""

    def test_no_highlight_params(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_vector([0.1])
        params = mock_index.search.call_args[0][1]
        assert "attributesToHighlight" not in params
        # Crop is always enabled so vector search still returns a useful snippet.
        assert "attributesToCrop" in params
        assert "content" in params["attributesToCrop"]

    def test_returns_results_and_total(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("d1")],
            "estimatedTotalHits": 1,
        }
        results, total, _ = backend.search_vector([0.1])
        assert len(results) == 1
        assert total == 1


# ---------------------------------------------------------------------------
# search_hybrid()
# ---------------------------------------------------------------------------


class TestSearchHybrid:
    def test_hybrid_param_default_ratio(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_hybrid("query", [0.1, 0.2])
        params = mock_index.search.call_args[0][1]
        assert params["hybrid"]["semanticRatio"] == pytest.approx(0.75)
        assert params["hybrid"]["embedder"] == "custom"

    def test_hybrid_param_custom_ratio(self, mock_client: MagicMock, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend = MeilisearchBackend(semantic_ratio=0.9)
        backend._client = mock_client
        backend.search_hybrid("q", [0.1])
        params = mock_index.search.call_args[0][1]
        assert params["hybrid"]["semanticRatio"] == pytest.approx(0.9)

    def test_query_text_forwarded(self, backend: MagicMock, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_hybrid("my query", [0.1])
        query = mock_index.search.call_args[0][0]
        assert query == "my query"

    def test_highlight_params_included(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.search_hybrid("q", [0.1])
        params = mock_index.search.call_args[0][1]
        assert params["highlightPreTag"] == "<mark>"

    def test_returns_results_total_facets(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("d1", 0.8)],
            "estimatedTotalHits": 1,
            "facetDistribution": {"meta_creator": {"Alice": 1}},
        }
        results, total, facets = backend.search_hybrid("q", [0.1], include_facets=True)
        assert len(results) == 1
        assert total == 1
        assert facets is not None
        assert "Creator" in facets


# ---------------------------------------------------------------------------
# Multi-index search
# ---------------------------------------------------------------------------


class TestMultiIndexSearch:
    def test_results_merged_and_sorted_by_score(self, mock_client: MagicMock, mock_index: MagicMock):
        b = MeilisearchBackend(url="http://x", index="idx1,idx2")

        hit_a = _make_hit("a", score=0.9)
        hit_b = _make_hit("b", score=0.5)

        call_count = 0

        def search_side_effect(query, params):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"hits": [hit_a], "estimatedTotalHits": 1}
            return {"hits": [hit_b], "estimatedTotalHits": 1}

        mock_index.search.side_effect = search_side_effect
        results, total, _ = b.search_text("q")
        assert results[0].doc_id == "a"
        assert results[1].doc_id == "b"
        assert total == 2

    def test_index_name_set_per_result(self, mock_client: MagicMock, mock_index: MagicMock):
        b = MeilisearchBackend(url="http://x", index="idx1,idx2")

        def search_side_effect(query, params):
            return {"hits": [_make_hit("d1")], "estimatedTotalHits": 1}

        mock_index.search.side_effect = search_side_effect
        results, _, _ = b.search_text("q")
        # Both results should have a non-empty index name
        assert all(r.index for r in results)


# ---------------------------------------------------------------------------
# delete_index()
# ---------------------------------------------------------------------------


class TestDeleteIndex:
    def test_calls_delete_on_client(self, backend: MeilisearchBackend, mock_client: MagicMock):
        backend.delete_index()
        mock_client.delete_index.assert_called_once_with("test")

    def test_no_error_when_index_not_found(self, backend: MeilisearchBackend, mock_client: MagicMock):
        mock_client.delete_index.side_effect = _make_api_error("index_not_found")
        # Should not raise
        backend.delete_index()

    def test_other_errors_propagated(self, backend: MeilisearchBackend, mock_client: MagicMock):
        mock_client.delete_index.side_effect = _make_api_error("internal_error")
        with pytest.raises(meilisearch.errors.MeilisearchApiError):
            backend.delete_index()


# ---------------------------------------------------------------------------
# document_count()
# ---------------------------------------------------------------------------


class TestDocumentCount:
    def test_returns_number_of_documents(self, backend: MeilisearchBackend, mock_index: MagicMock):
        stats = MagicMock()
        stats.number_of_documents = 42
        mock_index.get_stats.return_value = stats
        assert backend.document_count() == 42

    def test_returns_zero_when_index_not_found(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.get_stats.side_effect = _make_api_error("index_not_found")
        assert backend.document_count() == 0


# ---------------------------------------------------------------------------
# get_document()
# ---------------------------------------------------------------------------


class TestGetDocument:
    def _make_doc_obj(self, **kwargs):
        import json
        from meilisearch.models.document import Document as MeiliDocument

        data = {
            "id": "doc1",
            "source_path": "/data/file.pdf",
            "display_path": "file.pdf",
            "extracted_from": "",
            "content": "full content here",
            "metadata_json": json.dumps({"Content-Type": "application/pdf"}),
            "has_embeddings": False,
            "meta_content_type": "application/pdf",
            "_rankingScore": 1.0,
        }
        data.update(kwargs)
        # Use the real Document class — dict() works via Document.__iter__
        return MeiliDocument(data)

    def test_returns_search_result(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.get_document.return_value = self._make_doc_obj()
        result = backend.get_document("doc1")
        assert result is not None
        assert result.doc_id == "doc1"

    def test_full_content_returned_as_snippet(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.get_document.return_value = self._make_doc_obj(content="full content here")
        result = backend.get_document("doc1")
        assert result is not None
        assert result.snippet == "full content here"

    def test_returns_none_when_not_found(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.get_document.side_effect = _make_api_error("document_not_found")
        assert backend.get_document("missing") is None

    def test_other_errors_propagated(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.get_document.side_effect = _make_api_error("internal_error")
        with pytest.raises(meilisearch.errors.MeilisearchApiError):
            backend.get_document("doc1")


# ---------------------------------------------------------------------------
# find_attachments() / find_by_display_path()
# ---------------------------------------------------------------------------


class TestFindAttachments:
    def test_searches_with_extracted_from_filter(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.find_attachments("parent/doc.pdf")
        params = mock_index.search.call_args[0][1]
        assert "extracted_from" in params["filter"]
        assert "parent/doc.pdf" in params["filter"]

    def test_returns_list_of_results(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("child1"), _make_hit("child2")],
            "estimatedTotalHits": 2,
        }
        results = backend.find_attachments("parent.pdf")
        assert len(results) == 2


class TestFindByDisplayPath:
    def test_searches_with_display_path_filter(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.find_by_display_path("some/path.pdf")
        params = mock_index.search.call_args[0][1]
        assert "display_path" in params["filter"]
        assert "some/path.pdf" in params["filter"]

    def test_returns_none_when_not_found(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        assert backend.find_by_display_path("nonexistent.pdf") is None

    def test_returns_first_result(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("doc1")],
            "estimatedTotalHits": 1,
        }
        result = backend.find_by_display_path("file.pdf")
        assert result is not None
        assert result.doc_id == "doc1"


# ---------------------------------------------------------------------------
# list_indices()
# ---------------------------------------------------------------------------


class TestListIndices:
    def test_returns_sorted_index_names(self, backend: MeilisearchBackend, mock_client: MagicMock):
        idx_b = MagicMock()
        idx_b.uid = "beta"
        idx_a = MagicMock()
        idx_a.uid = "alpha"
        mock_client.get_indexes.return_value = {"results": [idx_b, idx_a]}
        assert backend.list_indices() == ["alpha", "beta"]

    def test_hidden_indices_excluded(self, backend: MeilisearchBackend, mock_client: MagicMock):
        hidden = MagicMock()
        hidden.uid = ".internal"
        visible = MagicMock()
        visible.uid = "public"
        mock_client.get_indexes.return_value = {"results": [hidden, visible]}
        assert backend.list_indices() == ["public"]

    def test_returns_empty_on_error(self, backend: MeilisearchBackend, mock_client: MagicMock):
        mock_client.get_indexes.side_effect = Exception("network error")
        assert backend.list_indices() == []


# ---------------------------------------------------------------------------
# count_unembedded()
# ---------------------------------------------------------------------------


class TestCountUnembedded:
    def test_returns_estimated_total_hits(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 17}
        assert backend.count_unembedded() == 17

    def test_filter_targets_has_embeddings_false(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        backend.count_unembedded()
        params = mock_index.search.call_args[0][1]
        assert params["filter"] == "has_embeddings = false"

    def test_returns_zero_when_index_not_found(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.side_effect = _make_api_error("index_not_found")
        assert backend.count_unembedded() == 0


# ---------------------------------------------------------------------------
# scroll_unembedded()
# ---------------------------------------------------------------------------


class TestScrollUnembedded:
    def test_yields_batches(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.side_effect = [
            {"hits": [{"id": "d1", "content": "text1"}, {"id": "d2", "content": "text2"}], "estimatedTotalHits": 2},
            {"hits": [], "estimatedTotalHits": 0},
        ]
        batches = list(backend.scroll_unembedded(batch_size=2))
        assert len(batches) == 1
        assert batches[0] == [("d1", "text1"), ("d2", "text2")]

    def test_stops_when_fewer_docs_than_batch_size(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [{"id": "d1", "content": "text"}],
            "estimatedTotalHits": 1,
        }
        batches = list(backend.scroll_unembedded(batch_size=64))
        assert len(batches) == 1
        assert mock_index.search.call_count == 1

    def test_returns_no_batches_when_index_not_found(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.side_effect = _make_api_error("index_not_found")
        assert list(backend.scroll_unembedded()) == []

    def test_filter_targets_unembedded_docs(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        list(backend.scroll_unembedded())
        params = mock_index.search.call_args[0][1]
        assert params["filter"] == "has_embeddings = false"

    def test_always_uses_offset_zero(self, backend: MeilisearchBackend, mock_index: MagicMock):
        """Each batch must start at offset 0 because processed docs are marked embedded."""
        mock_index.search.side_effect = [
            {"hits": [{"id": f"d{i}", "content": "x"} for i in range(2)], "estimatedTotalHits": 4},
            {"hits": [{"id": f"d{i}", "content": "x"} for i in range(2)], "estimatedTotalHits": 2},
            {"hits": [], "estimatedTotalHits": 0},
        ]
        list(backend.scroll_unembedded(batch_size=2))
        for call in mock_index.search.call_args_list:
            params = call[0][1]
            assert params["offset"] == 0


# ---------------------------------------------------------------------------
# scroll_document_ids()
# ---------------------------------------------------------------------------


class TestScrollDocumentIds:
    def test_yields_batches_of_content(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [
                {"id": "d1", "content": "content 1"},
                {"id": "d2", "content": "content 2"},
            ],
            "estimatedTotalHits": 2,
        }
        batches = list(backend.scroll_document_ids(["d1", "d2"], batch_size=10))
        assert len(batches) == 1
        assert ("d1", "content 1") in batches[0]
        assert ("d2", "content 2") in batches[0]

    def test_missing_docs_skipped(self, backend: MeilisearchBackend, mock_index: MagicMock):
        # Search only returns one of the two requested docs (other not found).
        mock_index.search.return_value = {
            "hits": [{"id": "d1", "content": "content 1"}],
            "estimatedTotalHits": 1,
        }
        batches = list(backend.scroll_document_ids(["d1", "d2"]))
        assert batches == [[("d1", "content 1")]]

    def test_respects_batch_size(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        list(backend.scroll_document_ids(["d1", "d2", "d3"], batch_size=2))
        # Should issue two search calls: one for [d1, d2], one for [d3].
        assert mock_index.search.call_count == 2

    def test_filter_uses_id_in_syntax(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {"hits": [], "estimatedTotalHits": 0}
        list(backend.scroll_document_ids(["d1", "d2"]))
        params = mock_index.search.call_args[0][1]
        assert "id IN" in params["filter"]
        assert '"d1"' in params["filter"]
        assert '"d2"' in params["filter"]

    def test_empty_ids_yields_nothing(self, backend: MeilisearchBackend, mock_index: MagicMock):
        assert list(backend.scroll_document_ids([])) == []

    def test_index_not_found_stops_iteration(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.side_effect = _make_api_error("index_not_found")
        assert list(backend.scroll_document_ids(["d1"])) == []


# ---------------------------------------------------------------------------
# update_embeddings()
# ---------------------------------------------------------------------------


class TestUpdateEmbeddings:
    def test_empty_updates_returns_zero_failures(self, backend: MeilisearchBackend):
        assert backend.update_embeddings([]) == 0

    def test_vectors_stored_with_correct_structure(self, backend: MeilisearchBackend, mock_index: MagicMock):
        vecs = [[0.1, 0.2], [0.3, 0.4]]
        backend.update_embeddings([("doc1", vecs)])
        docs = mock_index.update_documents.call_args[0][0]
        assert len(docs) == 1
        assert docs[0]["id"] == "doc1"
        assert docs[0]["_vectors"]["custom"]["embeddings"] == vecs
        assert docs[0]["_vectors"]["custom"]["regenerate"] is False

    def test_has_embeddings_set_to_true(self, backend: MeilisearchBackend, mock_index: MagicMock):
        backend.update_embeddings([("doc1", [[0.1, 0.2]])])
        docs = mock_index.update_documents.call_args[0][0]
        assert docs[0]["has_embeddings"] is True

    def test_returns_zero_on_success(self, backend: MeilisearchBackend, mock_index: MagicMock):
        assert backend.update_embeddings([("doc1", [[0.1]])]) == 0

    def test_returns_failure_count_on_task_failure(
        self, backend: MeilisearchBackend, mock_client: MagicMock, mock_index: MagicMock
    ):
        mock_client.wait_for_task.return_value = _make_task_result("failed")
        count = backend.update_embeddings([("d1", [[0.1]]), ("d2", [[0.2]])])
        assert count == 2

    def test_multiple_docs_in_single_task(self, backend: MeilisearchBackend, mock_index: MagicMock):
        backend.update_embeddings([("d1", [[0.1]]), ("d2", [[0.2]])])
        # Should be a single update_documents call with both docs
        mock_index.update_documents.assert_called_once()
        docs = mock_index.update_documents.call_args[0][0]
        assert len(docs) == 2


# ---------------------------------------------------------------------------
# SearchResult parity – same fields as Elasticsearch backend
# ---------------------------------------------------------------------------


class TestSearchResultParity:
    """Verify SearchResult produced by Meilisearch has the same shape as ES would return."""

    def test_all_fields_present(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("d1", content="Some content")],
            "estimatedTotalHits": 1,
        }
        results, _, _ = backend.search_text("q")
        r = results[0]
        # These are all the fields the SearchResult dataclass defines
        assert hasattr(r, "doc_id")
        assert hasattr(r, "source_path")
        assert hasattr(r, "display_path")
        assert hasattr(r, "score")
        assert hasattr(r, "snippet")
        assert hasattr(r, "metadata")
        assert hasattr(r, "extracted_from")
        assert hasattr(r, "display_path_highlighted")
        assert hasattr(r, "index")

    def test_metadata_contains_file_type_as_human_label(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("d1")],
            "estimatedTotalHits": 1,
        }
        results, _, _ = backend.search_text("q")
        # "Plain Text" not "text/plain"
        assert results[0].metadata.get("File Type") == "Plain Text"

    def test_score_is_float(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("d1", score=0.77)],
            "estimatedTotalHits": 1,
        }
        results, _, _ = backend.search_text("q")
        assert isinstance(results[0].score, float)

    def test_snippet_is_str(self, backend: MeilisearchBackend, mock_index: MagicMock):
        mock_index.search.return_value = {
            "hits": [_make_hit("d1")],
            "estimatedTotalHits": 1,
        }
        results, _, _ = backend.search_text("q")
        assert isinstance(results[0].snippet, str)


# ---------------------------------------------------------------------------
# Helper – construct a MeilisearchApiError without an HTTP response object
# ---------------------------------------------------------------------------

import meilisearch.errors


def _make_api_error(code: str) -> meilisearch.errors.MeilisearchApiError:
    """Create a MeilisearchApiError that can be raised as a side_effect."""
    exc = meilisearch.errors.MeilisearchApiError.__new__(meilisearch.errors.MeilisearchApiError)
    exc.code = code
    exc.message = f"error: {code}"
    exc.status_code = 404 if "not_found" in code else 500
    exc.link = None
    exc.type = None
    exc.args = (exc.message,)
    return exc
