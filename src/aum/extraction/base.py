from __future__ import annotations

from pathlib import Path
from typing import Protocol

from aum.models import Document


class ExtractionError(Exception):
    """Raised when text extraction fails for a document."""


class ExtractionDepthError(ExtractionError):
    """Raised when the maximum nested extraction depth is exceeded."""


class Extractor(Protocol):
    def extract(self, file_path: Path) -> list[Document]:
        """Extract text and metadata from a file, including embedded documents.

        Returns one Document per content part (e.g. email body + each attachment).
        Raises ExtractionError on failure.
        """
        ...

    def supports(self, mime_type: str) -> bool:
        """Return whether this extractor can handle the given MIME type."""
        ...
