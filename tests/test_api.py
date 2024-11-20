import time
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY
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
    request_count_before = (
        REGISTRY.get_sample_value(
            "http_request_total", {"method": "GET", "endpoint": "/search"}
        )
        or 0
    )
    search_count_before = (
        REGISTRY.get_sample_value(
            "http_request_total", {"method": "GET", "endpoint": "/search"}
        )
        or 0
    )
    mock_search_engine = MagicMock()
    mock_search_engine.search.return_value = mock_result
    app = app_init(mock_search_engine, "test_index")
    client = TestClient(app)

    response = client.get(f"/search?q={query}")

    request_count_after = REGISTRY.get_sample_value(
        "http_request_total", {"method": "GET", "endpoint": "/search"}
    )
    search_count_after = REGISTRY.get_sample_value(
        "http_request_total", {"method": "GET", "endpoint": "/search"}
    )
    assert response.status_code == 200
    assert mock_result == response.json()
    mock_search_engine.search.assert_called_once_with("test_index", query)
    assert (request_count_after - request_count_before) == 1.0
    assert (search_count_after - search_count_before) == 1.0


def test_search_metrics():
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
    request_count_before = (
        REGISTRY.get_sample_value(
            "http_request_total", {"method": "GET", "endpoint": "/search"}
        )
        or 0
    )
    search_count_before = (
        REGISTRY.get_sample_value(
            "aum_search_query_total", {"index_name": "test_index"}
        )
        or 0
    )
    mock_search_engine = MagicMock()
    mock_search_engine.search.return_value = mock_result
    app = app_init(mock_search_engine, "test_index")
    client = TestClient(app)

    _ = client.get(f"/search?q={query}")

    request_count_after = REGISTRY.get_sample_value(
        "http_request_total", {"method": "GET", "endpoint": "/search"}
    )
    search_count_after = REGISTRY.get_sample_value(
        "aum_search_query_total", {"index_name": "test_index"}
    )
    assert (request_count_after - request_count_before) == 1.0
    assert (search_count_after - search_count_before) == 1.0


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


def test_metrics():
    client = TestClient(app_init(MagicMock(), "test_index"))
    response = client.get("/metrics")
    assert response.status_code == 200
    assert "aum_search_query_total" in response.text
    assert "http_exception_total" in response.text
    assert "http_request_latency_bucket" in response.text
    assert "http_request_total" in response.text


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
