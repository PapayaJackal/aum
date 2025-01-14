import socket
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import REGISTRY

from aum.sonic import SonicBackend
from aum.util import encode_base64


@patch("socket.socket")
@patch("aum.sonic.read_line")
@patch("aum.sonic.send_command")
def test_delete_index(mock_send_command, mock_read_line, mock_socket):
    mock_instance = MagicMock()
    mock_socket.return_value.__enter__.return_value = mock_instance
    mock_read_line.side_effect = [
        "CONNECTED <sonic-server v1.0.0>",
        "STARTED ingest protocol(1) buffer(20000)",
        "OK",
    ]
    delete_counter_before = (
        REGISTRY.get_sample_value(
            "sonic_index_delete_total", {"index_name": "test_index"}
        )
        or 0.0
    )

    backend = SonicBackend("localhost", 1491, "SecretPassword")

    backend.delete_index("test_index")

    delete_counter_after = REGISTRY.get_sample_value(
        "sonic_index_delete_total", {"index_name": "test_index"}
    )
    mock_socket.assert_called_once_with(socket.AF_INET6, socket.SOCK_STREAM)
    mock_instance.connect.assert_called_once_with(("localhost", 1491))
    mock_send_command.assert_any_call(mock_instance, "START ingest SecretPassword")
    mock_send_command.assert_any_call(mock_instance, "FLUSHB documents test_index")
    assert (delete_counter_after - delete_counter_before) == 1.0


@patch("socket.socket")
@patch("aum.sonic.read_line")
@patch("aum.sonic.send_command")
def test_index_documents(mock_send_command, mock_read_line, mock_socket):
    mock_instance = MagicMock()
    mock_socket.return_value.__enter__.return_value = mock_instance
    mock_read_line.side_effect = [
        "CONNECTED <sonic-server v1.0.0>",
        "STARTED ingest protocol(1) buffer(20000)",
        "OK",
    ]
    index_counter_before = (
        REGISTRY.get_sample_value(
            "sonic_document_index_total", {"index_name": "test_index"}
        )
        or 0.0
    )

    backend = SonicBackend("localhost", 1491, "SecretPassword")
    documents = [{"id": "test/test.docx", "content": "test document"}]

    backend.index_documents("test_index", documents)

    index_counter_after = REGISTRY.get_sample_value(
        "sonic_document_index_total", {"index_name": "test_index"}
    )
    mock_socket.assert_called_once_with(socket.AF_INET6, socket.SOCK_STREAM)
    mock_instance.connect.assert_called_once_with(("localhost", 1491))
    mock_send_command.assert_any_call(mock_instance, "START ingest SecretPassword")
    mock_send_command.assert_any_call(
        mock_instance,
        f'PUSH documents test_index {encode_base64("test/test.docx")} "test document"',
    )
    assert (index_counter_after - index_counter_before) == 1.0


@patch("socket.socket")
@patch("aum.sonic.read_line")
@patch("aum.sonic.send_command")
def test_search(mock_send_command, mock_read_line, mock_socket):
    mock_instance = MagicMock()
    mock_socket.return_value.__enter__.return_value = mock_instance
    mock_read_line.side_effect = [
        "CONNECTED <sonic-server v1.0.0>",
        "STARTED search protocol(1) buffer(20000)",
        "PENDING Bt2m2gYa",
        f"EVENT QUERY Bt2m2gYa {encode_base64('result1')} {encode_base64('result2')}",
    ]
    search_counter_before = (
        REGISTRY.get_sample_value(
            "sonic_search_request_total", {"index_name": "test_index"}
        )
        or 0.0
    )

    backend = SonicBackend("localhost", 1491, "SecretPassword")

    results = backend.search("test_index", "test query")

    search_counter_after = REGISTRY.get_sample_value(
        "sonic_search_request_total", {"index_name": "test_index"}
    )
    mock_socket.assert_called_once_with(socket.AF_INET6, socket.SOCK_STREAM)
    mock_instance.connect.assert_called_once_with(("localhost", 1491))
    mock_send_command.assert_any_call(mock_instance, "START search SecretPassword")
    mock_send_command.assert_any_call(
        mock_instance, 'QUERY documents test_index "test query" LIMIT(20)'
    )
    ids_in_results = [x["id"] for x in results["hits"]]
    assert ids_in_results == ["result1", "result2"]
    assert (search_counter_after - search_counter_before) == 1.0


@pytest.mark.integration
def test_integration_sonic_backend(request):
    # Set up the SonicBackend instance with the appropriate parameters
    backend = SonicBackend("::1", 1491, "SecretPassword")
    index_name = "test_index"
    documents = [{"id": "document:1", "content": "test document"}]

    # Ensure the index is created before tests
    backend.create_index(index_name)

    def cleanup():
        backend.delete_index(index_name)

    request.addfinalizer(cleanup)

    # Index documents
    backend.index_documents(index_name, documents)

    # Search for the document
    results = backend.search(index_name, "test document")

    # Check if the results contain the indexed document
    assert len(results) > 0
    assert "document:1" == results["hits"][0]["id"]
