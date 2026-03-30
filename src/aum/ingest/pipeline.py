from __future__ import annotations

import hashlib
import os
import sys
import time
from aum.names import generate_name
from concurrent.futures import Future, ThreadPoolExecutor
from contextlib import nullcontext
from pathlib import Path
from queue import Empty, Queue
from threading import Lock, Thread

import structlog
import structlog.contextvars
from rich.console import Console as RichConsole
from rich.live import Live
from rich.text import Text

from aum.extraction.base import ExtractionDepthError, ExtractionError, Extractor
from aum.ingest.lock import IngestLock
from aum.ingest.tracker import JobTracker
from aum.metrics import (
    DOCS_FAILED,
    DOCS_INGESTED,
    DOCS_SKIPPED,
    INGEST_DURATION,
    INGEST_JOBS_ACTIVE,
)
from aum.models import Document, IngestJob, JobStatus
from aum.pool import InstancePool
from aum.search.base import SearchBackend

log = structlog.get_logger()

_SENTINEL = object()  # signals the walker is done


def _file_doc_id(file_path: Path, index: int = 0) -> str:
    """Generate a stable document ID from a file path and part index."""
    key = f"{file_path.resolve()}:{index}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


def _make_progress_line(
    start: float,
    discovered: int,
    walker_done: bool,
    files_done: int,
    in_flight: int,
    indexed: int,
    failed: int,
    empty: int,
    timing_count: int,
    total_extraction_time: float,
    skipped: int = 0,
) -> Text:
    """Build a single-line rich Text for the live progress display."""
    elapsed = time.monotonic() - start

    t = Text(no_wrap=True, overflow="crop")

    # Progress bar — shown once we know total; scan indicator while still walking
    if discovered > 0:
        pct = min(files_done / discovered * 100, 100)
        filled = int(20 * pct / 100)
        t.append("[", style="dim")
        t.append("█" * filled, style="blue")
        t.append("░" * (20 - filled), style="dim blue")
        t.append("] ", style="dim")
        t.append(f"{files_done:,}/{discovered:,} ({pct:.0f}%)", style="white")
    else:
        t.append(f"{files_done:,} files", style="white")

    # Directory scan status
    if walker_done:
        t.append("  scan:done", style="dim green")
    else:
        t.append(f"  scan:{discovered:,}", style="dim yellow")

    # In-flight Tika requests
    t.append(f"  tika:{in_flight}", style="cyan")

    # Average extraction time per file
    avg = total_extraction_time / timing_count if timing_count > 0 else 0.0
    t.append(f"  {avg:.3f}s/file", style="yellow")

    # Indexed, empty, skipped, and failed counts
    t.append(f"  idx:{indexed}", style="green")
    if skipped > 0:
        t.append(f"  skip:{skipped}", style="dim cyan")
    if empty > 0:
        t.append(f"  empty:{empty}", style="yellow")
    if failed > 0:
        t.append(f"  fail:{failed}", style="bold red")

    # Elapsed wall-clock time
    m, s = divmod(int(elapsed), 60)
    t.append(f"  {m:02d}:{s:02d}", style="dim")

    return t


def _walk_files(
    root: Path,
    queue: Queue[Path | object],
    tracker: JobTracker,
    job_id: str,
    discovered: list[int],
) -> int:
    """Walk a directory tree using os.scandir and stream paths into a queue.

    Returns total number of files discovered. Updates the tracker's total_files
    as the walk progresses so the UI can show a live discovery count.
    ``discovered`` is a single-element list used as a shared mutable counter
    that the main thread can read without a database round-trip.
    """
    count = 0
    stack = [root]

    while stack:
        current = stack.pop()
        try:
            with os.scandir(current) as entries:
                for entry in entries:
                    try:
                        if entry.is_file(follow_symlinks=False):
                            queue.put(Path(entry.path))
                            count += 1
                            discovered[0] = count
                            # Update total periodically so progress is visible during discovery
                            if count % 500 == 0:
                                tracker.update_total_files(job_id, count)
                                log.debug("file discovery in progress", job_id=job_id, discovered=count)
                        elif entry.is_dir(follow_symlinks=False):
                            stack.append(Path(entry.path))
                    except OSError as exc:
                        log.warning("skipping entry", path=entry.path, error=str(exc))
        except OSError as exc:
            log.warning("skipping directory", path=str(current), error=str(exc))

    # Final total update
    tracker.update_total_files(job_id, count)
    discovered[0] = count
    queue.put(_SENTINEL)
    log.info("file discovery complete", job_id=job_id, total_files=count)
    return count


class IngestPipeline:
    """Orchestrates document ingestion with streaming file discovery.

    Architecture:
    - A walker thread streams file paths via os.scandir into a queue
    - Worker threads pull from the queue and extract documents via Tika
    - The main thread collects extracted docs into batches, embeds, and indexes
    """

    def __init__(
        self,
        extractor_pool: InstancePool[Extractor],
        search_backend: SearchBackend,
        tracker: JobTracker,
        index_name: str = "aum",
        batch_size: int = 50,
        max_workers: int | None = None,
        data_dir: str | Path | None = None,
    ) -> None:
        self._extractor_pool = extractor_pool
        self._backend = search_backend
        self._tracker = tracker
        self._index_name = index_name
        self._batch_size = batch_size
        self._max_workers = max_workers or extractor_pool.total_concurrency
        self._data_dir = Path(data_dir) if data_dir else None

    def close(self) -> None:
        """Release resources held by the extractor pool."""
        self._extractor_pool.close()

    def run_retry(self, file_paths: list[Path], source_dir: Path) -> tuple[IngestJob, float, float]:
        """Re-run ingest for specific file paths (retry failed items).

        Works like ``run()`` but feeds explicit paths instead of walking a
        directory.  The resulting job is a normal tracked job that can itself
        be retried later.

        Returns (job, elapsed_seconds, avg_extraction_seconds).
        """
        self._backend.initialize()

        source_dir = source_dir.resolve()
        job_id = generate_name()
        lock = self._make_lock(job_id)

        total = len(file_paths)
        self._tracker.create_job(job_id, source_dir, total_files=total, index_name=self._index_name)
        self._tracker.update_total_files(job_id, total)

        log.info("starting retry ingest", job_id=job_id, source_dir=str(source_dir), files=total)
        return self._run_pipeline(job_id, source_dir, file_paths, lock=lock)

    def run(self, source_dir: Path) -> tuple[IngestJob, float, float]:
        """Run a full ingest job on a directory.

        Returns (job, elapsed_seconds, avg_extraction_seconds).
        """
        self._backend.initialize()

        source_dir = source_dir.resolve()
        job_id = generate_name()
        lock = self._make_lock(job_id)

        # Create with total_files=0; the walker updates it as it discovers files
        self._tracker.create_job(job_id, source_dir, total_files=0, index_name=self._index_name)

        log.info("starting ingest", job_id=job_id, source_dir=str(source_dir))
        return self._run_pipeline(job_id, source_dir, lock=lock)

    def run_resume(self, source_dir: Path, parent_job_id: str) -> tuple[IngestJob, float, float]:
        """Resume an interrupted ingest, skipping already-indexed documents.

        Re-walks *source_dir* but filters out files whose primary document ID
        already exists in the search index.  Creates a new tracked job.

        Returns (job, elapsed_seconds, avg_extraction_seconds).
        """
        self._backend.initialize()

        source_dir = source_dir.resolve()
        job_id = generate_name()
        lock = self._make_lock(job_id)

        self._tracker.create_job(job_id, source_dir, total_files=0, index_name=self._index_name)

        log.info("starting resume ingest", job_id=job_id, source_dir=str(source_dir), parent=parent_job_id)
        return self._run_pipeline(job_id, source_dir, skip_existing=True, lock=lock)

    def _make_lock(self, job_id: str) -> IngestLock | None:
        """Create and acquire a per-job lock, or return ``None`` if no data_dir."""
        if not self._data_dir:
            return None
        lock = IngestLock(self._data_dir, job_id)
        if not lock.acquire():
            raise RuntimeError(f"Could not acquire lock for job {job_id}")
        return lock

    def _run_pipeline(
        self,
        job_id: str,
        source_dir: Path,
        explicit_paths: list[Path] | None = None,
        *,
        skip_existing: bool = False,
        lock: IngestLock | None = None,
    ) -> tuple[IngestJob, float, float]:
        """Core pipeline loop shared by run(), run_retry(), and run_resume().

        When *explicit_paths* is ``None`` a walker thread discovers files from
        *source_dir*.  When a list is provided those paths are fed directly
        into the extraction workers.

        When *skip_existing* is ``True`` a filter thread sits between the
        walker and the extraction workers, batch-checking which file doc IDs
        already exist in the search index and dropping those that do.
        """
        INGEST_JOBS_ACTIVE.inc()

        show_progress = sys.stderr.isatty()
        console = RichConsole(stderr=True) if show_progress else None

        job_start = time.monotonic()
        extracted = 0
        processed = 0
        failed = 0
        empty = 0
        extraction_time = 0.0
        timing_count = 0
        files_done = 0  # all completed futures, including failures (for progress bar)

        # Single-element list so the walker thread can update it without a lock
        discovered: list[int] = [0]
        # Skipped files counter (written by filter thread, read by main thread)
        skip_counter: list[int] = [0]

        # Track truly concurrent extractions (threads actively inside _extract_one)
        in_flight_lock = Lock()
        in_flight_count: list[int] = [0]

        # When skip_existing is enabled the walker feeds into a separate queue
        # and a filter thread sits between the walker and the workers.
        if skip_existing:
            walker_queue: Queue[Path | object] = Queue(maxsize=1000)
            file_queue: Queue[Path | object] = Queue(maxsize=1000)
        else:
            walker_queue = file_queue = Queue(maxsize=1000)

        if explicit_paths is not None:
            # Feed explicit paths into the queue directly
            def _feed_paths() -> int:
                target = walker_queue
                for p in explicit_paths:
                    target.put(p)
                    discovered[0] += 1
                target.put(_SENTINEL)
                return len(explicit_paths)

            walker = Thread(target=_feed_paths, daemon=True)
        else:
            walker = Thread(
                target=_walk_files,
                args=(source_dir, walker_queue, self._tracker, job_id, discovered),
                daemon=True,
            )
        walker.start()

        filter_thread: Thread | None = None
        if skip_existing:
            filter_thread = Thread(
                target=self._filter_existing_worker,
                args=(walker_queue, file_queue, skip_counter),
                daemon=True,
            )
            filter_thread.start()

        ctx: Live | nullcontext = (  # type: ignore[type-arg]
            Live(
                _make_progress_line(job_start, 0, False, 0, 0, 0, 0, 0, 0, 0.0),
                console=console,
                refresh_per_second=4,
                transient=True,
            )
            if show_progress
            else nullcontext()
        )

        try:
            batch: list[tuple[str, Document]] = []

            with ctx as live:
                with ThreadPoolExecutor(max_workers=self._max_workers) as pool:
                    pending_futures: dict[Future[tuple[list[Document], float, int]], Path] = {}
                    walker_done = False

                    while True:
                        # Submit new extraction tasks while we have capacity and files
                        while not walker_done and len(pending_futures) < self._max_workers * 2:
                            try:
                                file_path = file_queue.get(timeout=0.05)
                            except Empty:
                                break
                            if file_path is _SENTINEL:
                                walker_done = True
                                break
                            future = pool.submit(self._extract_one, file_path, job_id, in_flight_lock, in_flight_count)
                            pending_futures[future] = file_path

                        # Collect completed extractions
                        done_futures = [f for f in pending_futures if f.done()]
                        for future in done_futures:
                            file_path = pending_futures.pop(future)
                            files_done += 1
                            try:
                                docs, ext_time, n_empty = future.result()
                                extraction_time += ext_time
                                timing_count += 1
                                empty += n_empty
                                extracted += max(0, len(docs) - 1)
                                for i, doc in enumerate(docs):
                                    self._set_display_path(doc, source_dir)
                                    doc_id = _file_doc_id(file_path, i)
                                    batch.append((doc_id, doc))
                                    log.debug(
                                        "queued for indexing",
                                        doc_id=doc_id,
                                        source_path=str(doc.source_path),
                                        part=i,
                                        content_length=len(doc.content),
                                    )
                            except ExtractionDepthError as exc:
                                failed += 1
                                DOCS_FAILED.labels(error_type="ExtractionDepthError").inc()
                                self._tracker.record_error(job_id, file_path, "ExtractionDepthError", str(exc))
                            except ExtractionError as exc:
                                failed += 1
                                DOCS_FAILED.labels(error_type="ExtractionError").inc()
                                self._tracker.record_error(job_id, file_path, "ExtractionError", str(exc))
                            except Exception as exc:
                                failed += 1
                                DOCS_FAILED.labels(error_type=type(exc).__name__).inc()
                                self._tracker.record_error(job_id, file_path, type(exc).__name__, str(exc))

                        # Flush batch when full
                        if len(batch) >= self._batch_size:
                            n_processed, n_failed, _, _ = self._flush_batch(job_id, batch)
                            processed += n_processed
                            failed += n_failed
                            batch = []
                            self._tracker.update_progress(
                                job_id, extracted, processed, failed, empty, skipped=skip_counter[0]
                            )
                            log.info(
                                "batch complete",
                                job_id=job_id,
                                extracted=extracted,
                                processed=processed,
                                failed=failed,
                                empty=empty,
                                skipped=skip_counter[0],
                            )

                        # Update live display
                        if live is not None:
                            live.update(
                                _make_progress_line(
                                    job_start,
                                    discovered[0],
                                    walker_done,
                                    files_done,
                                    in_flight_count[0],
                                    processed,
                                    failed,
                                    empty,
                                    timing_count,
                                    extraction_time,
                                    skipped=skip_counter[0],
                                )
                            )

                        # Exit when walker is done and all futures are collected
                        if walker_done and not pending_futures:
                            break

                    # Flush remaining documents
                    if batch:
                        n_processed, n_failed, _, _ = self._flush_batch(job_id, batch)
                        processed += n_processed
                        failed += n_failed

                    # Always persist final counts — the empty/failed counters
                    # may have changed after the last mid-loop batch flush.
                    self._tracker.update_progress(job_id, extracted, processed, failed, empty, skipped=skip_counter[0])

            walker.join(timeout=5)
            if filter_thread is not None:
                filter_thread.join(timeout=5)
            elapsed = time.monotonic() - job_start
            self._tracker.complete_job(job_id, JobStatus.COMPLETED)
            log.info(
                "ingest complete",
                job_id=job_id,
                extracted=extracted,
                processed=processed,
                failed=failed,
                empty=empty,
                skipped=skip_counter[0],
            )

        except Exception:
            elapsed = time.monotonic() - job_start
            self._tracker.complete_job(job_id, JobStatus.FAILED)
            log.exception("ingest job failed", job_id=job_id)
            raise
        finally:
            INGEST_JOBS_ACTIVE.dec()
            if lock:
                lock.release()

        avg_extraction = extraction_time / timing_count if timing_count > 0 else 0.0
        return self._tracker.get_job(job_id), elapsed, avg_extraction  # type: ignore[return-value]

    def _filter_existing_worker(
        self,
        input_queue: Queue[Path | object],
        output_queue: Queue[Path | object],
        skip_counter: list[int],
        check_batch_size: int = 200,
    ) -> None:
        """Filter thread: drop files whose primary doc ID already exists.

        Reads paths from *input_queue* in batches, queries the search backend
        for existing document IDs, and forwards only new paths to
        *output_queue*.  Increments ``skip_counter[0]`` for each skipped file.
        """
        batch: list[Path] = []
        while True:
            try:
                path = input_queue.get(timeout=0.1)
            except Empty:
                continue
            if path is _SENTINEL:
                if batch:
                    self._flush_filter_batch(batch, output_queue, skip_counter)
                output_queue.put(_SENTINEL)
                return
            batch.append(path)
            if len(batch) >= check_batch_size:
                self._flush_filter_batch(batch, output_queue, skip_counter)
                batch = []

    def _flush_filter_batch(
        self,
        paths: list[Path],
        output_queue: Queue[Path | object],
        skip_counter: list[int],
    ) -> None:
        """Check a batch of paths against the index and forward new ones."""
        doc_ids = [_file_doc_id(p, 0) for p in paths]
        id_to_path = dict(zip(doc_ids, paths))
        try:
            existing = self._backend.get_existing_doc_ids(doc_ids)
        except Exception:
            log.warning("skip-existing check failed, processing all files in batch", exc_info=True)
            existing = set()
        n_skipped = len(existing)
        if n_skipped:
            skip_counter[0] += n_skipped
            DOCS_SKIPPED.inc(n_skipped)
        for doc_id, path in id_to_path.items():
            if doc_id not in existing:
                output_queue.put(path)

    @staticmethod
    def _set_display_path(doc: Document, source_dir: Path) -> None:
        """Compute a relative display path and store it in document metadata.

        For attachments extracted from containers the extractor already sets
        ``_aum_display_path`` to an absolute logical path (e.g.
        ``/data/inbox/email.eml/attachment.pdf``).  For top-level documents
        it is unset.  In both cases we resolve it to a path relative to the
        source directory so the search API can return it directly.

        Also relativizes ``_aum_extracted_from`` (the parent container's
        display path) so it matches the parent document's stored display_path.
        """
        logical = doc.metadata.get("_aum_display_path") or str(doc.source_path)
        p = Path(logical)
        try:
            display = str(p.relative_to(source_dir))
        except ValueError:
            display = p.name
        doc.metadata["_aum_display_path"] = display

        extracted_from = doc.metadata.get("_aum_extracted_from")
        if extracted_from:
            ef = Path(str(extracted_from))
            try:
                doc.metadata["_aum_extracted_from"] = str(ef.relative_to(source_dir))
            except ValueError:
                # Can't relativize — drop the field rather than leak an
                # absolute path through the API.
                del doc.metadata["_aum_extracted_from"]

    def _extract_one(
        self,
        file_path: Path,
        job_id: str,
        in_flight_lock: Lock,
        in_flight_count: list[int],
    ) -> tuple[list[Document], float, int]:
        """Extract documents from a file. Returns (docs, elapsed, empty_count)."""
        structlog.contextvars.bind_contextvars(job_id=job_id)
        with in_flight_lock:
            in_flight_count[0] += 1
        try:
            start = time.monotonic()
            empty_count: list[int] = [0]

            def _record_sub_error(path: Path, etype: str, msg: str) -> None:
                DOCS_FAILED.labels(error_type=etype).inc()
                self._tracker.record_error(job_id, path, etype, msg)
                if etype == "EmptyExtraction":
                    empty_count[0] += 1

            with self._extractor_pool.acquire() as extractor:
                docs = extractor.extract(file_path, record_error=_record_sub_error)
            elapsed = time.monotonic() - start
            INGEST_DURATION.labels(stage="extraction").observe(elapsed)
            return docs, elapsed, empty_count[0]
        finally:
            with in_flight_lock:
                in_flight_count[0] -= 1

    def _flush_batch(self, job_id: str, batch: list[tuple[str, Document]]) -> tuple[int, int, float, float]:
        """Index a batch. Returns (processed, failed, embed_time, index_time)."""
        if not batch:
            return 0, 0, 0.0, 0.0

        t0 = time.monotonic()
        n_processed, n_failed = self._index_batch(job_id, batch)
        idx_time = time.monotonic() - t0

        return n_processed, n_failed, 0.0, idx_time

    def _index_batch(self, job_id: str, docs: list[tuple[str, Document]]) -> tuple[int, int]:
        """Index a batch. Returns (processed, failed) counts."""
        start = time.monotonic()
        try:
            result = self._backend.index_batch(docs)
            INGEST_DURATION.labels(stage="indexing").observe(time.monotonic() - start)
        except Exception as exc:
            log.error("batch indexing failed", job_id=job_id, error=str(exc))
            for _, doc in docs:
                DOCS_FAILED.labels(error_type="IndexingError").inc()
                self._tracker.record_error(job_id, doc.source_path, "IndexingError", str(exc))
            return 0, len(docs)

        # Record any content truncations so they show up in `aum status --errors`.
        docs_by_id = {doc_id: doc for doc_id, doc in docs}
        for doc_id, original_chars, truncated_chars in result.truncated:
            doc = docs_by_id.get(doc_id)
            path = doc.source_path if doc else doc_id
            msg = f"content truncated from {original_chars} to {truncated_chars} chars to fit payload limit"
            self._tracker.record_error(job_id, path, "ContentTruncated", msg)

        if result.failures:
            failed_ids = {doc_id for doc_id, _ in result.failures}
            for doc_id, reason in result.failures:
                DOCS_FAILED.labels(error_type="IndexingError").inc()
                doc = docs_by_id.get(doc_id)
                path = doc.source_path if doc else doc_id
                self._tracker.record_error(job_id, path, "IndexingError", reason)
            indexed = len(docs) - len(failed_ids)
            failed_count = len(failed_ids)
        else:
            indexed = len(docs)
            failed_count = 0

        DOCS_INGESTED.labels(backend=type(self._backend).__name__).inc(indexed)
        return indexed, failed_count
