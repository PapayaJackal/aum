from __future__ import annotations

import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from threading import Lock

import structlog

from aum.models import IngestError, IngestJob, JobStatus

log = structlog.get_logger()

SCHEMA = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    source_dir TEXT NOT NULL,
    index_name TEXT NOT NULL DEFAULT 'aum',
    status TEXT NOT NULL DEFAULT 'pending',
    total_files INTEGER DEFAULT 0,
    extracted INTEGER DEFAULT 0,
    processed INTEGER DEFAULT 0,
    failed INTEGER DEFAULT 0,
    empty INTEGER DEFAULT 0,
    created_at TEXT NOT NULL,
    finished_at TEXT
);

CREATE TABLE IF NOT EXISTS job_errors (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL REFERENCES jobs(job_id),
    file_path TEXT NOT NULL,
    error_type TEXT NOT NULL,
    message TEXT NOT NULL,
    timestamp TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS index_embeddings (
    index_name TEXT PRIMARY KEY,
    model TEXT NOT NULL,
    backend TEXT NOT NULL DEFAULT 'ollama',
    dimension INTEGER NOT NULL,
    updated_at TEXT NOT NULL
);

"""


class JobTracker:
    """SQLite-backed ingest job tracker."""

    def __init__(self, db_path: str = "aum.db") -> None:
        self._lock = Lock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.executescript(SCHEMA)
        self._migrate()
        self._conn.commit()

    def _migrate(self) -> None:
        """Add columns that may be missing from older databases."""
        cols = {row["name"] for row in self._conn.execute("PRAGMA table_info(jobs)").fetchall()}
        if "index_name" not in cols:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN index_name TEXT NOT NULL DEFAULT 'aum'")
        if "extracted" not in cols:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN extracted INTEGER DEFAULT 0")
        if "empty" not in cols:
            self._conn.execute("ALTER TABLE jobs ADD COLUMN empty INTEGER DEFAULT 0")

    def create_job(self, job_id: str, source_dir: Path, total_files: int, index_name: str = "aum") -> IngestJob:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT INTO jobs (job_id, source_dir, index_name, status, total_files, created_at)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (job_id, str(source_dir), index_name, JobStatus.RUNNING.value, total_files, now),
            )
            self._conn.commit()
        log.info(
            "created ingest job", job_id=job_id, source_dir=str(source_dir),
            index_name=index_name, total_files=total_files,
        )
        return IngestJob(
            job_id=job_id,
            source_dir=source_dir,
            index_name=index_name,
            status=JobStatus.RUNNING,
            total_files=total_files,
            created_at=datetime.fromisoformat(now),
        )

    def update_total_files(self, job_id: str, total_files: int) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET total_files = ? WHERE job_id = ?",
                (total_files, job_id),
            )
            self._conn.commit()

    def update_progress(self, job_id: str, extracted: int, processed: int, failed: int, empty: int = 0) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET extracted = ?, processed = ?, failed = ?, empty = ? WHERE job_id = ?",
                (extracted, processed, failed, empty, job_id),
            )
            self._conn.commit()

    def record_error(self, job_id: str, file_path: Path, error_type: str, message: str) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT INTO job_errors (job_id, file_path, error_type, message, timestamp) VALUES (?, ?, ?, ?, ?)",
                (job_id, str(file_path), error_type, message, now),
            )
            self._conn.commit()
        log.warning("ingest error", job_id=job_id, file_path=str(file_path), error_type=error_type, message=message)

    def complete_job(self, job_id: str, status: JobStatus = JobStatus.COMPLETED) -> None:
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                "UPDATE jobs SET status = ?, finished_at = ? WHERE job_id = ?",
                (status.value, now, job_id),
            )
            self._conn.commit()
        log.info("completed ingest job", job_id=job_id, status=status.value)

    def get_job(self, job_id: str) -> IngestJob | None:
        row = self._conn.execute("SELECT * FROM jobs WHERE job_id = ?", (job_id,)).fetchone()
        if row is None:
            return None
        return self._row_to_job(row)

    def list_jobs(self, status: JobStatus | None = None) -> list[IngestJob]:
        if status:
            rows = self._conn.execute(
                "SELECT * FROM jobs WHERE status = ? ORDER BY created_at DESC", (status.value,)
            ).fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM jobs ORDER BY created_at DESC").fetchall()
        return [self._row_to_job(row) for row in rows]

    def get_errors(self, job_id: str) -> list[IngestError]:
        rows = self._conn.execute(
            "SELECT * FROM job_errors WHERE job_id = ? ORDER BY timestamp", (job_id,)
        ).fetchall()
        return [
            IngestError(
                file_path=Path(row["file_path"]),
                error_type=row["error_type"],
                message=row["message"],
                timestamp=datetime.fromisoformat(row["timestamp"]),
            )
            for row in rows
        ]

    def _row_to_job(self, row: sqlite3.Row) -> IngestJob:
        errors = self.get_errors(row["job_id"])
        keys = row.keys()
        return IngestJob(
            job_id=row["job_id"],
            source_dir=Path(row["source_dir"]),
            index_name=row["index_name"] if "index_name" in keys else "aum",
            status=JobStatus(row["status"]),
            total_files=row["total_files"],
            extracted=row["extracted"] if "extracted" in keys else 0,
            processed=row["processed"],
            failed=row["failed"],
            empty=row["empty"] if "empty" in keys else 0,
            errors=errors,
            created_at=datetime.fromisoformat(row["created_at"]),
            finished_at=datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None,
        )

    # --- Embedding model tracking ---

    def get_embedding_model(self, index_name: str) -> tuple[str, str, int] | None:
        """Return (model, backend, dimension) for the given index, or None if not set."""
        row = self._conn.execute(
            "SELECT model, backend, dimension FROM index_embeddings WHERE index_name = ?",
            (index_name,),
        ).fetchone()
        if row is None:
            return None
        return row["model"], row["backend"], row["dimension"]

    def set_embedding_model(self, index_name: str, model: str, backend: str, dimension: int) -> None:
        """Record the embedding model and backend used for an index (upsert)."""
        now = datetime.now(UTC).isoformat()
        with self._lock:
            self._conn.execute(
                "INSERT INTO index_embeddings (index_name, model, backend, dimension, updated_at)"
                " VALUES (?, ?, ?, ?, ?)"
                " ON CONFLICT(index_name) DO UPDATE SET model = ?, backend = ?, dimension = ?, updated_at = ?",
                (index_name, model, backend, dimension, now, model, backend, dimension, now),
            )
            self._conn.commit()
        log.info("stored embedding model for index", index_name=index_name, model=model, backend=backend, dimension=dimension)
