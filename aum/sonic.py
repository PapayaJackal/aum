import json
import socket
import time

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


class SonicBackend(SearchEngineBackend):

    def __init__(self, host, port, password):
        self.host = host
        self.port = int(port)
        self.password = password

    def create_index(self, index_name):
        # noop in sonic
        pass

    def delete_index(self, index_name):
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            read_line(sock)
            send_command(sock, f"START ingest {self.password}")
            read_line(sock)
            send_command(sock, f"FLUSHB documents {index_name}")
            read_line(sock)

    def index_documents(self, index_name, documents):
        with socket.socket(socket.AF_INET6, socket.SOCK_STREAM) as sock:
            sock.connect((self.host, self.port))
            read_line(sock)
            send_command(sock, f"START ingest {self.password}")
            read_line(sock)
            for document in documents:
                document["id"] = encode_base64(document["id"])
                msg = (
                    "PUSH documents "
                    + f"{index_name} {document['id']} {escape_query(document['content'])}"
                )
                send_command(sock, msg)
                read_line(sock)

    def search(self, index_name, query, limit=20):
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
        results = [{"id": decode_base64(x)} for x in event.split(" ")[3:]]
        return {
            "hits": results,
            "offset": 0,
            "limit": limit,
            "estimatedTotalHits": None,
            "processingTimeMs": int((end_time - start_time) * 1000),
            "query": query,
        }
