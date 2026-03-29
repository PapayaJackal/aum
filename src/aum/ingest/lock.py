"""flock-based ingest lock for crash detection.

Each ingest job gets its own lock file at ``{data_dir}/ingest.{job_id}.lock``.
When the process starts it acquires an exclusive advisory lock via
``fcntl.flock``.  If the process crashes or is killed the OS automatically
releases the lock, allowing ``aum resume`` to detect the stale job and pick up
where it left off.

Multiple ingest jobs can run in parallel — each holds its own lock file.
"""

from __future__ import annotations

import fcntl
import os
from pathlib import Path

import structlog

log = structlog.get_logger()


class IngestLock:
    """Exclusive advisory lock backed by ``{data_dir}/ingest.{job_id}.lock``."""

    def __init__(self, data_dir: str | Path, job_id: str) -> None:
        self._dir = Path(data_dir)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / f"ingest.{job_id}.lock"
        self._fd: int | None = None

    @property
    def path(self) -> Path:
        return self._path

    def acquire(self) -> bool:
        """Try to acquire the lock (non-blocking).

        On success the file is written with the current PID for display
        purposes, and ``True`` is returned.  If the lock is already held by
        another process ``False`` is returned immediately.
        """
        fd = os.open(str(self._path), os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            os.close(fd)
            return False

        # Write holder info (truncate first in case the file was longer).
        os.ftruncate(fd, 0)
        os.lseek(fd, 0, os.SEEK_SET)
        os.write(fd, f"pid={os.getpid()}\n".encode())

        self._fd = fd
        log.debug("acquired ingest lock", path=str(self._path))
        return True

    def release(self) -> None:
        """Release the lock and remove the lock file."""
        if self._fd is not None:
            try:
                fcntl.flock(self._fd, fcntl.LOCK_UN)
                os.close(self._fd)
            except OSError:
                pass
            self._fd = None
            try:
                self._path.unlink(missing_ok=True)
            except OSError:
                pass
            log.debug("released ingest lock", path=str(self._path))

    def read_holder_pid(self) -> int | None:
        """Read the PID from the lock file, or ``None`` if unreadable."""
        try:
            text = self._path.read_text()
        except OSError:
            return None
        for line in text.splitlines():
            if line.startswith("pid="):
                try:
                    return int(line.split("=", 1)[1].strip())
                except ValueError:
                    return None
        return None

    def is_held(self) -> bool:
        """Return ``True`` if another process currently holds the lock."""
        if not self._path.exists():
            return False
        fd = None
        try:
            fd = os.open(str(self._path), os.O_RDWR | os.O_CREAT, 0o644)
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # We got it → nobody else holds it.  Release immediately.
            fcntl.flock(fd, fcntl.LOCK_UN)
            return False
        except BlockingIOError:
            return True
        except OSError:
            return False
        finally:
            if fd is not None:
                os.close(fd)
