from abc import ABC, abstractmethod


class TextExtractor(ABC):

    @abstractmethod
    def extract_text(self, document_path):
        """Extract text from a document at the given path"""
        pass
