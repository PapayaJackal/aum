# pylint: disable=W0621
from unittest.mock import MagicMock, patch

import pytest

from aum.meilisearch import MeilisearchBackend
from aum.util import encode_base64


@pytest.fixture
def meilisearch_backend():
    """Fixture to create an instance of MeilisearchBackend."""
    with patch("meilisearch.Client") as _:
        yield MeilisearchBackend("http://127.0.0.1:7700", "aMasterKey")


def test_create_index(meilisearch_backend):
    """Test the create_index method."""
    index_name = "test_index"

    mock_client_instance = meilisearch_backend.meilisearch
    mock_task = MagicMock()
    mock_task.task_uid = 1
    mock_client_instance.create_index.return_value = mock_task

    meilisearch_backend.create_index(index_name)

    mock_client_instance.create_index.assert_called_once_with(
        index_name, {"primaryKey": "id"}
    )
    mock_client_instance.wait_for_task.assert_called_once_with(mock_task.task_uid)


def test_delete_index(meilisearch_backend):
    """Test the delete_index method."""
    index_name = "test_index"

    mock_client_instance = meilisearch_backend.meilisearch
    mock_task = MagicMock()
    mock_task.task_uid = 1
    mock_client_instance.delete_index.return_value = mock_task

    meilisearch_backend.delete_index(index_name)

    mock_client_instance.delete_index.assert_called_once_with(index_name)
    mock_client_instance.wait_for_task.assert_called_once_with(mock_task.task_uid)


def test_index_documents(meilisearch_backend):
    """Test the index_documents method."""
    index_name = "test_index"
    documents = [
        {"id": "test/test.docx", "title": "Document 1"},
        {"id": "test/test2.docx", "title": "Document 2"},
    ]

    mock_client_instance = meilisearch_backend.meilisearch
    mock_task = MagicMock()
    mock_task.task_uid = 1
    mock_client_instance.index(index_name).add_documents.return_value = mock_task

    meilisearch_backend.index_documents(index_name, documents)

    mock_client_instance.index(index_name).add_documents.assert_called_once_with(
        [
            {"id": encode_base64("test/test.docx"), "title": "Document 1"},
            {"id": encode_base64("test/test2.docx"), "title": "Document 2"},
        ]
    )
    mock_client_instance.wait_for_task.assert_called_once_with(mock_task.task_uid)


def test_search(meilisearch_backend):
    """Test the search method."""
    index_name = "test_index"
    query = "search term"
    limit = 20

    mock_client_instance = meilisearch_backend.meilisearch
    mock_search_result = {
        "hits": [{"id": encode_base64("test/test.docx"), "title": "Document 1"}],
        "limit": limit,
    }
    mock_client_instance.index(index_name).search.return_value = mock_search_result

    result = meilisearch_backend.search(index_name, query, limit)

    mock_client_instance.index(index_name).search.assert_called_once_with(
        query, {"limit": limit}
    )
    assert result == {
        "hits": [{"id": "test/test.docx", "title": "Document 1"}],
        "limit": limit,
    }


@pytest.mark.integration
def test_integration_meilisearch_backend(request):
    """Integration test for MeilisearchBackend."""
    # Initialize the Meilisearch client with the actual host and master key
    host = "http://127.0.0.1:7700"
    master_key = "aMasterKey"  # Use your actual master key
    backend = MeilisearchBackend(host, master_key)

    # Create an index
    index_name = "integration_test_index"
    backend.create_index(index_name)

    # Clean up: delete the index after the test
    def cleanup():
        backend.meilisearch.delete_index(index_name)

    request.addfinalizer(cleanup)

    # Index some documents
    documents = [
        {"id": "test/test.docx", "title": "First Document"},
        {"id": "test/test2.docx", "title": "Second Document"},
    ]
    backend.index_documents(index_name, documents)

    # Perform a search
    query = "First"
    results = backend.search(index_name, query)

    # Assert that the search results contain the expected document
    assert len(results["hits"]) > 0
    assert results["hits"][0]["id"] == "test/test.docx"
    assert results["hits"][0]["title"] == "First Document"

    backend.meilisearch.delete_index(index_name)
