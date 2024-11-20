import json
import socket
import time
from functools import wraps

from prometheus_client import Counter, Histogram

from . import SearchEngineBackend
from .util import decode_base64, encode_base64


def escape_query(query):
    return json.dumps(str(query))


def read_line(sock):
    line = b""
    while True:
        char = sock.recv(1)
        if not char:
            break
        line += char
        if char == b"\n":
            break
    return line.decode("utf-8").strip()


def send_command(sock, command):
    sock.sendall(command.encode("utf-8") + b"\r\n")


INDEX_DELETE_COUNTER = Counter(
    "sonic_index_delete_total", "Total number of index delete requests", ["index_name"]
)
DOCUMENT_INDEX_COUNTER = Counter(
    "sonic_document_index_total", "Total number of documents indexed", ["index_name"]
)
SEARCH_REQUEST_COUNTER = Counter(
    "sonic_search_request_total", "Total number of search requests", ["index_name"]
)
SEARCH_DURATION_HISTOGRAM = Histogram(
    "sonic_search_duration_seconds",
    "Duration of search requests in seconds",
    ["index_name"],
)
DOCUMENT_INDEX_DURATION_HISTOGRAM = Histogram(
    "sonic_document_index_duration_seconds",
    "Duration of document indexing in seconds",
    ["index_name"],
)
EXCEPTION_COUNTER = Counter(
    "sonic_exception_total",
    "Total exceptions raised for Sonic requests",
    ["index_name", "exception_type"],
)


def catch_sonic_exceptions(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):
        index_name = kwargs.get("index_name", args[0] if args else None)
        try:
            return func(self, *args, **kwargs)
        except Exception as e:
            EXCEPTION_COUNTER.labels(
                index_name=index_name, exception_type=type(e).__name__
            ).inc()
            raise e from None

    return wrapper


class SonicBackend(SearchEngineBackend):

    def __init__(self, host, port, password):
        self.host = host
        self.port = int(port)
        self.password = password

    def create_index(self, index_name):
        # noop in sonic
        pass

    @catch_sonic_exceptions
    def delete_index(self, index_name):
        INDEX_DELETE_COUNTER.labels(index_name=index_name).inc()
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            read_line(sock)
            send_command(sock, f"START ingest {self.password}")
            read_line(sock)
            send_command(sock, f"FLUSHB documents {index_name}")
            read_line(sock)

    @catch_sonic_exceptions
    def index_documents(self, index_name, documents):
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            read_line(sock)
            send_command(sock, f"START ingest {self.password}")
            read_line(sock)
            for document in documents:
                with DOCUMENT_INDEX_DURATION_HISTOGRAM.labels(
                    index_name=index_name
                ).time():
                    DOCUMENT_INDEX_COUNTER.labels(index_name=index_name).inc()
                    document["id"] = encode_base64(document["id"])
                    msg = (
                        "PUSH documents "
                        + f"{index_name} {document['id']} {escape_query(document['content'])}"
                    )
                    send_command(sock, msg)
                    read_line(sock)

    @catch_sonic_exceptions
    def search(self, index_name, query, limit=20):
        SEARCH_REQUEST_COUNTER.labels(index_name=index_name).inc()
        query = query.replace('"', '\\"')
        start_time = time.time()
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            read_line(sock)
            send_command(sock, f"START search {self.password}")
            read_line(sock)
            send_command(
                sock,
                f"QUERY documents {index_name} {escape_query(query)} LIMIT({limit})",
            )
            read_line(sock)
            event = read_line(sock)
        end_time = time.time()
        SEARCH_DURATION_HISTOGRAM.labels(index_name=index_name).observe(
            end_time - start_time
        )
        results = [{"id": decode_base64(x)} for x in event.split(" ")[3:]]
        return {
            "hits": results,
            "offset": 0,
            "limit": limit,
            "estimatedTotalHits": None,
            "processingTimeMs": int((end_time - start_time) * 1000),
            "query": query,
        }
