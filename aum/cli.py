import argparse
import base64
import logging
import os
from pathlib import Path

from .dirwalk import dirwalk
from .meilisearch import MeilisearchBackend
from .sonic import SonicBackend
from .tika import TikaTextExtractor

logging.basicConfig(
    level=logging.INFO, format="%(levelname)s:%(filename)s:%(lineno)d:%(message)s"
)


def encode_base64(input_string):
    output_bytes = base64.urlsafe_b64encode(input_string.encode("utf-8"))
    output_string = output_bytes.decode("ascii")
    return output_string.rstrip("=")


def decode_base64(input_string):
    input_bytes = input_string.encode("ascii")
    input_len = len(input_bytes)
    padding = b"=" * (3 - ((input_len + 3) % 4))

    # Passing altchars here allows decoding both standard and urlsafe base64
    output_bytes = base64.b64decode(input_bytes + padding, altchars=b"-_")
    return output_bytes.decode("utf-8")


def create_index(search_engine, text_extractor, index_name, directory):
    search_engine.create_index(index_name)

    for file in dirwalk(directory):
        logging.info("indexing %s", file)

        file_id = encode_base64(file)
        metadata, content = text_extractor.extract_text(directory / file)
        document = {"id": file_id, "metadata": metadata, "content": content}
        search_engine.index_documents(index_name, [document])


def main():
    parser = argparse.ArgumentParser(description="ॐ the tiny document search engine")
    parser.add_argument(
        "--backend",
        choices=["meilisearch", "sonic"],
        default=os.environ.get("AUM_BACKEND", "meilisearch"),
        help="Type of search engine backend to use (meilisearch or sonic)",
    )
    parser.add_argument(
        "--meilisearch-url",
        type=str,
        default=os.environ.get("AUM_MEILISEARCH_URL", "http://127.0.0.1:7700"),
        help="URL of the Meilisearch server",
    )
    parser.add_argument(
        "--meilisearch-master-key",
        type=str,
        default=os.environ.get("AUM_MEILISEARCH_MASTER_KEY", "aMasterKey"),
        help="Master key for Meilisearch",
    )
    parser.add_argument(
        "--sonic-host",
        type=str,
        default=os.environ.get("AUM_SONIC_HOST", "::1"),
        help="Host where Sonic is running",
    )
    parser.add_argument(
        "--sonic-port",
        type=int,
        default=os.environ.get("AUM_SONIC_PORT", "1491"),
        help="Port to connect to Sonic",
    )
    parser.add_argument(
        "--sonic-password",
        type=str,
        default=os.environ.get("AUM_SONIC_PASSWORD", "SecretPassword"),
        help="Password for Sonic",
    )

    subparsers = parser.add_subparsers(dest="command")

    index_parser = subparsers.add_parser(
        "index", help="Scan and index a directory of documents"
    )
    index_parser.add_argument(
        "index_name", type=str, help="Name of the index to create"
    )
    index_parser.add_argument(
        "directory", type=Path, help="Directory to scan for documents"
    )
    index_parser.add_argument(
        "--tika-url",
        type=str,
        default=os.environ.get("AUM_TIKA_URL", None),
        help="URL of the Tika server (default: starts a local instance if not specified).",
    )

    serve_parser = subparsers.add_parser(
        "serve", help="Serve the search engine web interface"
    )
    serve_parser.add_argument("index_name", type=str, help="Name of the index to serve")

    args = parser.parse_args()

    logging.info("Starting ॐ the tiny document search engine")
    if args.backend == "meilisearch":
        logging.info("Using meilisearch backend at %s", args.meilisearch_url)
        search_engine = MeilisearchBackend(
            args.meilisearch_url, args.meilisearch_master_key
        )
    elif args.backend == "sonic":
        logging.info("Using sonic backend at %s:%d", args.sonic_host, args.sonic_port)
        logging.info("Gotta go fast!")
        search_engine = SonicBackend(
            args.sonic_host, args.sonic_port, args.sonic_password
        )
    else:
        raise NotImplementedError

    if args.command == "index":
        logging.info("I'm an indexing worker process using Apache Tika")
        text_extractor = TikaTextExtractor(args.tika_url)
        create_index(search_engine, text_extractor, args.index_name, args.directory)
    elif args.command == "serve":
        raise NotImplementedError
