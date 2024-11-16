from abc import ABC, abstractmethod


class SearchEngineBackend(ABC):
    """
    Abstract base class for a search engine backend.

    This class defines the interface for a search engine backend,
    which includes methods for creating an index, indexing documents,
    and performing searches. Concrete implementations of this class
    should provide the specific functionality for a particular search engine.
    """

    @abstractmethod
    def create_index(self, index_name):
        """Create a new index with the specified name."""
        pass

    @abstractmethod
    def delete_index(self, index_name):
        """Delete the index with the specified name."""
        pass

    @abstractmethod
    def index_documents(self, index_name, documents):
        """Index documents in the specified index."""
        pass

    @abstractmethod
    def search(self, index_name, query, limit=10):
        """Search for documents in the specified index that match the given query."""
        pass


class TextExtractor(ABC):

    @abstractmethod
    def extract_text(self, document_path):
        """Extract text from a document at the given path"""
        pass
