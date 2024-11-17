import meilisearch

from . import SearchEngineBackend
from .util import decode_base64, encode_base64


class MeilisearchBackend(SearchEngineBackend):
    def __init__(self, host, master_key):
        self.meilisearch = meilisearch.Client(host, master_key)

    def create_index(self, index_name):
        task = self.meilisearch.create_index(index_name, {"primaryKey": "id"})
        self.meilisearch.wait_for_task(task.task_uid)

    def delete_index(self, index_name):
        task = self.meilisearch.delete_index(index_name)
        self.meilisearch.wait_for_task(task.task_uid)

    def index_documents(self, index_name, documents):
        for document in documents:
            document["id"] = encode_base64(document["id"])

        task = self.meilisearch.index(index_name).add_documents(documents)
        self.meilisearch.wait_for_task(task.task_uid)

    def search(self, index_name, query, limit=20):
        res = self.meilisearch.index(index_name).search(query, {"limit": limit})
        for hit in res["hits"]:
            hit["id"] = decode_base64(hit["id"])
        return res
