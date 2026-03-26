from __future__ import annotations

import enum
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path


class JobStatus(enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


class JobType(enum.Enum):
    INGEST = "ingest"
    EMBED = "embed"


@dataclass
class Document:
    """A document extracted and ready for indexing."""

    source_path: Path
    content: str
    metadata: dict[str, str | list[str]]


@dataclass
class IngestError:
    file_path: Path
    error_type: str
    message: str
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))


@dataclass
class IngestJob:
    job_id: str
    source_dir: Path
    index_name: str = "aum"
    job_type: JobType = JobType.INGEST
    status: JobStatus = JobStatus.PENDING
    total_files: int = 0
    extracted: int = 0
    processed: int = 0
    failed: int = 0
    empty: int = 0
    errors: list[IngestError] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    finished_at: datetime | None = None
