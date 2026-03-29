"""Tests for the flock-based per-job ingest lock."""

from __future__ import annotations

import multiprocessing
import os
import time
from pathlib import Path

import pytest

from aum.ingest.lock import IngestLock


def _hold_lock_in_child(ready_event, data_dir):
    """Target for subprocess that holds a lock then sleeps."""
    lk = IngestLock(data_dir, "child_job")
    assert lk.acquire()
    ready_event.set()
    time.sleep(60)


@pytest.fixture
def lock(tmp_path: Path) -> IngestLock:
    return IngestLock(tmp_path, "test_job")


class TestIngestLock:
    def test_acquire_and_release(self, lock: IngestLock) -> None:
        assert lock.acquire()
        # Lock file should exist with holder info
        pid = lock.read_holder_pid()
        assert pid is not None
        assert pid == os.getpid()

        lock.release()
        # Lock file should be cleaned up after release
        assert not lock.path.exists()

    def test_acquire_is_exclusive(self, tmp_path: Path) -> None:
        lock1 = IngestLock(tmp_path, "job1")
        lock2 = IngestLock(tmp_path, "job1")

        assert lock1.acquire()
        # Second acquire on same lock file should fail
        assert not lock2.acquire()

        lock1.release()
        # After release, second lock should succeed
        assert lock2.acquire()
        lock2.release()

    def test_parallel_jobs_independent(self, tmp_path: Path) -> None:
        """Different job IDs get independent lock files."""
        lock1 = IngestLock(tmp_path, "job_a")
        lock2 = IngestLock(tmp_path, "job_b")

        assert lock1.acquire()
        assert lock2.acquire()  # Should succeed — different lock file

        lock1.release()
        lock2.release()

    def test_is_held_when_acquired(self, tmp_path: Path) -> None:
        lock1 = IngestLock(tmp_path, "job1")
        checker = IngestLock(tmp_path, "job1")

        assert not checker.is_held()
        assert lock1.acquire()
        assert checker.is_held()
        lock1.release()
        assert not checker.is_held()

    def test_read_holder_pid_returns_none_when_no_file(self, tmp_path: Path) -> None:
        lock = IngestLock(tmp_path / "nonexistent", "no_job")
        assert lock.read_holder_pid() is None

    def test_release_is_idempotent(self, lock: IngestLock) -> None:
        lock.acquire()
        lock.release()
        lock.release()  # Should not raise

    def test_released_on_process_death(self, tmp_path: Path) -> None:
        """Lock is released when the holding process exits."""
        ready = multiprocessing.Event()
        proc = multiprocessing.Process(target=_hold_lock_in_child, args=(ready, str(tmp_path)))
        proc.start()
        ready.wait(timeout=5)

        # While child holds the lock, we cannot acquire
        lock = IngestLock(tmp_path, "child_job")
        assert lock.is_held()

        # Kill the child — OS releases the flock
        proc.terminate()
        proc.join(timeout=5)

        assert not lock.is_held()
        assert lock.acquire()
        lock.release()
