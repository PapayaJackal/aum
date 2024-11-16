import meilisearch

from . import SearchEngineBackend


class MeilisearchBackend(SearchEngineBackend):
    def __init__(self, *args, **kwargs):
        self.meilisearch = meilisearch.Client(*args, **kwargs)

    def create_index(self, index_name):
        task = self.meilisearch.create_index(index_name, {"primaryKey": "id"})
        self.meilisearch.wait_for_task(task.task_uid)

    def delete_index(self, index_name):
        task = self.meilisearch.delete_index(index_name)
        self.meilisearch.wait_for_task(task.task_uid)

    def index_documents(self, index_name, documents):
        task = self.meilisearch.index(index_name).add_documents(documents)
        self.meilisearch.wait_for_task(task.task_uid)

    def search(self, index_name, query, limit=20):
        return self.meilisearch.index(index_name).search(query, {"limit": limit})
