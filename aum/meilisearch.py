from functools import wraps

import meilisearch
from prometheus_client import Counter, Histogram

from . import SearchEngineBackend
from .util import decode_base64, encode_base64

INDEX_CREATE_COUNTER = Counter(
    "meilisearch_index_create_total",
    "Total number of index create requests",
    ["index_name"],
)
INDEX_DELETE_COUNTER = Counter(
    "meilisearch_index_delete_total",
    "Total number of index delete requests",
    ["index_name"],
)
DOCUMENT_INDEX_COUNTER = Counter(
    "meilisearch_document_index_total",
    "Total number of documents indexed",
    ["index_name"],
)
DOCUMENT_INDEX_DURATION_HISTOGRAM = Histogram(
    "meilisearch_document_index_duration_seconds",
    "Duration of document indexing in seconds per batch",
    ["index_name"],
)
SEARCH_REQUEST_COUNTER = Counter(
    "meilisearch_search_request_total",
    "Total number of search requests",
    ["index_name"],
)
SEARCH_DURATION_HISTOGRAM = Histogram(
    "meilisearch_search_duration_seconds",
    "Duration of search requests in seconds",
    ["index_name"],
)
EXCEPTION_COUNTER = Counter(
    "meilisearch_exception_total",
    "Total exceptions raised for Meilisearch requests",
    ["index_name", "exception_type"],
)


def catch_meilisearch_exceptions(func):
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


class MeilisearchBackend(SearchEngineBackend):
    def __init__(self, host, master_key):
        self.meilisearch = meilisearch.Client(host, master_key)

    @catch_meilisearch_exceptions
    def create_index(self, index_name):
        INDEX_CREATE_COUNTER.labels(index_name=index_name).inc()
        task = self.meilisearch.create_index(index_name, {"primaryKey": "id"})
        self.meilisearch.wait_for_task(task.task_uid)

    @catch_meilisearch_exceptions
    def delete_index(self, index_name):
        INDEX_DELETE_COUNTER.labels(index_name=index_name).inc()
        task = self.meilisearch.delete_index(index_name)
        self.meilisearch.wait_for_task(task.task_uid)

    @catch_meilisearch_exceptions
    def index_documents(self, index_name, documents):
        for document in documents:
            document["id"] = encode_base64(document["id"])
        with DOCUMENT_INDEX_DURATION_HISTOGRAM.labels(index_name=index_name).time():
            task = self.meilisearch.index(index_name).add_documents(documents)
            self.meilisearch.wait_for_task(task.task_uid)
        DOCUMENT_INDEX_COUNTER.labels(index_name=index_name).inc(len(documents))

    @catch_meilisearch_exceptions
    def search(self, index_name, query, limit=20):
        SEARCH_REQUEST_COUNTER.labels(index_name=index_name).inc()
        res = self.meilisearch.index(index_name).search(query, {"limit": limit})
        SEARCH_DURATION_HISTOGRAM.labels(index_name=index_name).observe(
            res["processingTimeMs"] / 1000
        )
        for hit in res["hits"]:
            hit["id"] = decode_base64(hit["id"])
        return res
