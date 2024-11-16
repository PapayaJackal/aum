from pathlib import Path
from unittest.mock import MagicMock

import pytest

from aum.cli import create_index, decode_base64, encode_base64
from aum.meilisearch import MeilisearchBackend
from aum.sonic import SonicBackend
from aum.tika import TikaTextExtractor

TEST_DIRECTORY = Path(__file__).parent / "data"
TEST_STRING = "Would it save you a lot of time if I just gave up and went mad now?"


def test_create_index():
    text_extractor = MagicMock()
    mock_search_engine = MagicMock()
    text_extractor.extract_text.return_value = ({}, TEST_STRING)

    create_index(mock_search_engine, text_extractor, "test_index", TEST_DIRECTORY)

    mock_search_engine.create_index.assert_called_once_with("test_index")
    assert mock_search_engine.index_documents.call_count == 3

    mock_search_engine.index_documents.assert_any_call(
        "test_index",
        [{"id": encode_base64("test.docx"), "metadata": {}, "content": TEST_STRING}],
    )
    mock_search_engine.index_documents.assert_any_call(
        "test_index",
        [{"id": encode_base64("test.pdf"), "metadata": {}, "content": TEST_STRING}],
    )
    mock_search_engine.index_documents.assert_any_call(
        "test_index",
        [
            {
                "id": encode_base64("test/test.txt"),
                "metadata": {},
                "content": TEST_STRING,
            }
        ],
    )


@pytest.mark.integration
def test_create_index_integration_meilisearch(request):
    text_extractor = TikaTextExtractor()
    search_engine = MeilisearchBackend("http://127.0.0.1:7700", "aMasterKey")

    def cleanup():
        search_engine.delete_index("test_index")

    request.addfinalizer(cleanup)

    create_index(search_engine, text_extractor, "test_index", TEST_DIRECTORY)

    results = search_engine.search("test_index", TEST_STRING)["hits"]
    assert len(results) == 3

    ids_in_results = [decode_base64(x["id"]) for x in results]
    assert "test.docx" in ids_in_results
    assert "test.pdf" in ids_in_results
    assert "test/test.txt" in ids_in_results


@pytest.mark.integration
def test_create_index_integration_sonic(request):
    text_extractor = TikaTextExtractor()
    search_engine = SonicBackend("::1", 1491, "SecretPassword")

    def cleanup():
        search_engine.delete_index("test_index")

    request.addfinalizer(cleanup)

    create_index(search_engine, text_extractor, "test_index", TEST_DIRECTORY)

    results = search_engine.search("test_index", TEST_STRING)
    assert len(results) == 3

    ids_in_results = [decode_base64(x) for x in results]
    assert "test.docx" in ids_in_results
    assert "test.pdf" in ids_in_results
    assert "test/test.txt" in ids_in_results
