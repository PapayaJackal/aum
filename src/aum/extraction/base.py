from __future__ import annotations

from pathlib import Path
from typing import Callable, Protocol

from aum.models import Document

RecordErrorFn = Callable[[Path, str, str], None]


class ExtractionError(Exception):
    """Raised when text extraction fails for a document."""


class ExtractionDepthError(ExtractionError):
    """Raised when the maximum nested extraction depth is exceeded."""


class Extractor(Protocol):
    def extract(
        self,
        file_path: Path,
        record_error: RecordErrorFn | None = None,
    ) -> list[Document]:
        """Extract text and metadata from a file, including embedded documents.

        Returns one Document per content part (e.g. email body + each attachment).
        ``record_error``, if provided, is called for each non-fatal sub-error
        (e.g. a failed attachment) with (path, error_type, message).
        Raises ExtractionError on failure of the top-level file.
        """
        ...

    def supports(self, mime_type: str) -> bool:
        """Return whether this extractor can handle the given MIME type."""
        ...
