import time
from unittest.mock import MagicMock

import pytest
from starlette.testclient import TestClient

from aum.api import app_init
from aum.meilisearch import MeilisearchBackend
from aum.sonic import SonicBackend


def test_search_success():
    query = "example"
    mock_result = {
        "hits": [
            {
                "id": 2742,
                "title": "Naked Lunch",
                "release_date": "1991-12-27",
            },
        ],
        "offset": 0,
        "limit": 20,
        "estimatedTotalHits": 976,
        "processingTimeMs": 35,
        "query": query,
    }
    mock_search_engine = MagicMock()
    mock_search_engine.search.return_value = mock_result
    app = app_init(mock_search_engine, "test_index")
    client = TestClient(app)

    response = client.get(f"/search?q={query}")

    assert response.status_code == 200
    assert mock_result == response.json()
    mock_search_engine.search.assert_called_once_with("test_index", query)


def test_search_missing_query():
    client = TestClient(app_init(MagicMock(), "test_index"))
    response = client.get("/search")
    assert response.status_code == 400
    assert response.json() == {"error": 'Query parameter "q" is required.'}


def test_serves_static():
    client = TestClient(app_init(MagicMock(), "test_index"))
    response = client.get("/")
    assert response.status_code == 200
    assert "<html" in response.text


@pytest.mark.integration
def test_search_meilisearch_integration(request):
    search_engine = MeilisearchBackend("http://127.0.0.1:7700", "aMasterKey")
    index_name = "test_index"

    def cleanup():
        search_engine.delete_index(index_name)

    request.addfinalizer(cleanup)

    documents = [
        {"id": "test/file1.docx", "content": "First Document"},
        {"id": "test/file2.docx", "content": "Second Document"},
    ]
    search_engine.index_documents(index_name, documents)
    time.sleep(1)

    app = app_init(search_engine, index_name)
    client = TestClient(app)
    query = "document"

    response = client.get(f"/search?q={query}")

    assert response.status_code == 200
    response = response.json()
    assert "hits" in response
    hit_ids = [x["id"] for x in response["hits"]]
    assert len(hit_ids) == 2
    assert "test/file1.docx" in hit_ids
    assert "test/file2.docx" in hit_ids
    assert response["offset"] == 0
    assert response["limit"] == 20
    assert response["estimatedTotalHits"] == 2
    assert "processingTimeMs" in response
    assert response["query"] == query


@pytest.mark.integration
def test_search_sonic_integration(request):
    search_engine = SonicBackend("::1", 1491, "SecretPassword")
    index_name = "test_index"

    def cleanup():
        search_engine.delete_index(index_name)

    request.addfinalizer(cleanup)

    documents = [
        {"id": "test/file1.docx", "content": "first document"},
        {"id": "test/file2.docx", "content": "second document"},
    ]
    search_engine.index_documents(index_name, documents)

    app = app_init(search_engine, index_name)
    client = TestClient(app)
    query = "document"

    response = client.get(f"/search?q={query}")

    assert response.status_code == 200
    response = response.json()
    assert "hits" in response
    hit_ids = [x["id"] for x in response["hits"]]
    assert len(hit_ids) == 2
    assert "test/file1.docx" in hit_ids
    assert "test/file2.docx" in hit_ids
    assert response["offset"] == 0
    assert response["limit"] == 20
    assert response["estimatedTotalHits"] is None
    assert "processingTimeMs" in response
    assert response["query"] == query
