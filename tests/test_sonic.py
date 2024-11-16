import socket
from unittest.mock import MagicMock, patch

import pytest

from aum.sonic import SonicBackend


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

    backend = SonicBackend("localhost", 1491, "SecretPassword")

    backend.delete_index("test_index")

    mock_socket.assert_called_once_with(socket.AF_INET6, socket.SOCK_STREAM)
    mock_instance.connect.assert_called_once_with(("localhost", 1491))
    mock_send_command.assert_any_call(mock_instance, "START ingest SecretPassword")
    mock_send_command.assert_any_call(mock_instance, "FLUSHB documents test_index")


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

    backend = SonicBackend("localhost", 1491, "SecretPassword")
    documents = [{"id": 1, "content": "test document"}]

    backend.index_documents("test_index", documents)

    mock_socket.assert_called_once_with(socket.AF_INET6, socket.SOCK_STREAM)
    mock_instance.connect.assert_called_once_with(("localhost", 1491))
    mock_send_command.assert_any_call(mock_instance, "START ingest SecretPassword")
    mock_send_command.assert_any_call(
        mock_instance, 'PUSH documents test_index 1 "test document"'
    )


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
        "EVENT QUERY Bt2m2gYa result1 result2",
    ]

    backend = SonicBackend("localhost", 1491, "SecretPassword")

    results = backend.search("test_index", "test query")

    mock_socket.assert_called_once_with(socket.AF_INET6, socket.SOCK_STREAM)
    mock_instance.connect.assert_called_once_with(("localhost", 1491))
    mock_send_command.assert_any_call(mock_instance, "START search SecretPassword")
    mock_send_command.assert_any_call(
        mock_instance, 'QUERY documents test_index "test query" LIMIT(20)'
    )
    assert results == ["result1", "result2"]


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
    assert documents[0]["id"] in results
