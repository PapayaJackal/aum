from __future__ import annotations

import hashlib
import os
import sys
import time
import uuid
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

from aum.embeddings.base import Embedder
from aum.extraction.base import ExtractionDepthError, ExtractionError, Extractor
from aum.ingest.tracker import JobTracker
from aum.metrics import (
    DOCS_FAILED,
    DOCS_INGESTED,
    INGEST_DURATION,
    INGEST_JOBS_ACTIVE,
)
from aum.models import Document, IngestJob, JobStatus
from aum.search.base import SearchBackend

log = structlog.get_logger()

_SENTINEL = None  # signals the walker is done


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
    timing_count: int,
    total_extraction_time: float,
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

    # Indexed and failed counts
    t.append(f"  idx:{indexed}", style="green")
    if failed > 0:
        t.append(f"  fail:{failed}", style="bold red")

    # Elapsed wall-clock time
    m, s = divmod(int(elapsed), 60)
    t.append(f"  {m:02d}:{s:02d}", style="dim")

    return t


def _walk_files(
    root: Path,
    queue: Queue[Path | None],
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
        extractor: Extractor,
        search_backend: SearchBackend,
        tracker: JobTracker,
        embedder: Embedder | None = None,
        index_name: str = "aum",
        batch_size: int = 50,
        max_workers: int = 4,
    ) -> None:
        self._extractor = extractor
        self._backend = search_backend
        self._tracker = tracker
        self._embedder = embedder
        self._index_name = index_name
        self._batch_size = batch_size
        self._max_workers = max_workers

    def run(self, source_dir: Path) -> tuple[IngestJob, float, float]:
        """Run a full ingest job on a directory.

        Returns (job, elapsed_seconds, avg_extraction_seconds).
        """
        # Ensure the search index exists with the correct mapping before
        # ingesting any documents.  This is a no-op when the index already
        # exists and the mapping is up to date.
        vector_dim = self._embedder.dimension if self._embedder else None
        self._backend.initialize(vector_dimension=vector_dim)

        source_dir = source_dir.resolve()
        job_id = uuid.uuid4().hex[:12]
        # Create with total_files=0; the walker updates it as it discovers files
        self._tracker.create_job(job_id, source_dir, total_files=0, index_name=self._index_name)

        log.info("starting ingest", job_id=job_id, source_dir=str(source_dir))
        INGEST_JOBS_ACTIVE.inc()

        show_progress = sys.stderr.isatty()
        console = RichConsole(stderr=True) if show_progress else None

        job_start = time.monotonic()
        extracted = 0
        processed = 0
        failed = 0
        extraction_time = 0.0
        timing_count = 0
        files_done = 0     # all completed futures, including failures (for progress bar)

        # Single-element list so the walker thread can update it without a lock
        discovered: list[int] = [0]

        # Track truly concurrent extractions (threads actively inside _extract_one)
        in_flight_lock = Lock()
        in_flight_count: list[int] = [0]

        file_queue: Queue[Path | None] = Queue(maxsize=self._max_workers * 4)

        walker = Thread(
            target=_walk_files,
            args=(source_dir, file_queue, self._tracker, job_id, discovered),
            daemon=True,
        )
        walker.start()

        ctx: Live | nullcontext = (  # type: ignore[type-arg]
            Live(
                _make_progress_line(job_start, 0, False, 0, 0, 0, 0, 0, 0.0),
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
                    pending_futures: dict[Future[tuple[list[Document], float]], Path] = {}
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
                                docs, ext_time = future.result()
                                extraction_time += ext_time
                                timing_count += 1
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
                            self._tracker.update_progress(job_id, extracted, processed, failed)
                            log.info("batch complete", job_id=job_id, extracted=extracted, processed=processed, failed=failed)

                        # Update live display
                        if live is not None:
                            live.update(_make_progress_line(
                                job_start, discovered[0], walker_done, files_done,
                                in_flight_count[0], processed, failed,
                                timing_count, extraction_time,
                            ))

                        # Exit when walker is done and all futures are collected
                        if walker_done and not pending_futures:
                            break

                    # Flush remaining documents
                    if batch:
                        n_processed, n_failed, _, _ = self._flush_batch(job_id, batch)
                        processed += n_processed
                        failed += n_failed
                        self._tracker.update_progress(job_id, extracted, processed, failed)

            walker.join(timeout=5)
            elapsed = time.monotonic() - job_start
            self._tracker.complete_job(job_id, JobStatus.COMPLETED)
            log.info("ingest complete", job_id=job_id, extracted=extracted, processed=processed, failed=failed)

        except Exception:
            elapsed = time.monotonic() - job_start
            self._tracker.complete_job(job_id, JobStatus.FAILED)
            log.exception("ingest job failed", job_id=job_id)
            raise
        finally:
            INGEST_JOBS_ACTIVE.dec()

        avg_extraction = extraction_time / timing_count if timing_count > 0 else 0.0
        return self._tracker.get_job(job_id), elapsed, avg_extraction  # type: ignore[return-value]

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
    ) -> tuple[list[Document], float]:
        structlog.contextvars.bind_contextvars(job_id=job_id)
        with in_flight_lock:
            in_flight_count[0] += 1
        try:
            start = time.monotonic()
            docs = self._extractor.extract(file_path)
            elapsed = time.monotonic() - start
            INGEST_DURATION.labels(stage="extraction").observe(elapsed)
            return docs, elapsed
        finally:
            with in_flight_lock:
                in_flight_count[0] -= 1

    def _flush_batch(self, job_id: str, batch: list[tuple[str, Document]]) -> tuple[int, int, float, float]:
        """Embed (if configured) and index a batch. Returns (processed, failed, embed_time, index_time)."""
        if not batch:
            return 0, 0, 0.0, 0.0

        emb_time = 0.0
        if self._embedder is not None:
            t0 = time.monotonic()
            self._embed_batch(batch)
            emb_time = time.monotonic() - t0

        t0 = time.monotonic()
        n_processed, n_failed = self._index_batch(job_id, batch)
        idx_time = time.monotonic() - t0

        return n_processed, n_failed, emb_time, idx_time

    def _embed_batch(self, docs: list[tuple[str, Document]]) -> None:
        assert self._embedder is not None
        texts = [doc.content for _, doc in docs]
        start = time.monotonic()
        embeddings = self._embedder.embed_batch(texts)
        INGEST_DURATION.labels(stage="embedding").observe(time.monotonic() - start)

        for (_, doc), emb in zip(docs, embeddings):
            doc.embedding = emb

    def _index_batch(self, job_id: str, docs: list[tuple[str, Document]]) -> tuple[int, int]:
        """Index a batch. Returns (processed, failed) counts."""
        start = time.monotonic()
        try:
            failures = self._backend.index_batch(docs)
            INGEST_DURATION.labels(stage="indexing").observe(time.monotonic() - start)
        except Exception as exc:
            log.error("batch indexing failed", job_id=job_id, error=str(exc))
            for _, doc in docs:
                DOCS_FAILED.labels(error_type="IndexingError").inc()
                self._tracker.record_error(job_id, doc.source_path, "IndexingError", str(exc))
            return 0, len(docs)

        if failures:
            failed_ids = {doc_id for doc_id, _ in failures}
            docs_by_id = {doc_id: doc for doc_id, doc in docs}
            for doc_id, reason in failures:
                DOCS_FAILED.labels(error_type="ElasticsearchError").inc()
                doc = docs_by_id.get(doc_id)
                path = doc.source_path if doc else doc_id
                self._tracker.record_error(job_id, path, "ElasticsearchError", reason)
            indexed = len(docs) - len(failed_ids)
            failed_count = len(failed_ids)
        else:
            indexed = len(docs)
            failed_count = 0

        DOCS_INGESTED.labels(backend=type(self._backend).__name__).inc(indexed)
        return indexed, failed_count
