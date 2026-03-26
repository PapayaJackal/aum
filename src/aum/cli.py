from __future__ import annotations

import getpass
import secrets
import string
import sys
from pathlib import Path

import click
import structlog

from aum import __version__
from aum.config import AumConfig
from aum.logging import configure_logging

log = structlog.get_logger()


def _load_config() -> AumConfig:
    return AumConfig()


def _setup(config: AumConfig) -> None:
    configure_logging(level=config.log_level, fmt=config.log_format)


_MIN_JWT_SECRET_BYTES = 32


def _ensure_jwt_secret(config: AumConfig) -> None:
    """Validate or auto-generate the JWT signing secret."""
    if not config.jwt_secret or config.jwt_secret == "change-me-in-production":
        config.jwt_secret = secrets.token_urlsafe(48)
        log.warning(
            "no jwt_secret configured — generated a random secret; "
            "sessions will not survive server restarts. "
            "Set AUM_JWT_SECRET or jwt_secret in aum.toml for persistence."
        )
    elif len(config.jwt_secret.encode()) < _MIN_JWT_SECRET_BYTES:
        click.echo(
            f"Error: jwt_secret is {len(config.jwt_secret.encode())} bytes, "
            f"minimum is {_MIN_JWT_SECRET_BYTES} bytes for {config.jwt_algorithm}.",
            err=True,
        )
        raise SystemExit(1)


@click.group()
@click.version_option(version=__version__)
def main() -> None:
    """aum - document search engine."""


# --- Ingest & Index ---


def _make_ingest_pipeline(
    config: AumConfig,
    idx: str,
    batch_size: int | None = None,
    workers: int | None = None,
    ocr: bool | None = None,
    ocr_language: str | None = None,
):
    """Create the extractor + pipeline objects used by ingest and retry."""
    from aum.api.deps import make_search_backend, make_tracker
    from aum.extraction.tika import TikaExtractor
    from aum.ingest.pipeline import IngestPipeline

    extractor = TikaExtractor(
        server_url=config.tika_server_url,
        ocr_enabled=ocr if ocr is not None else config.ocr_enabled,
        ocr_language=ocr_language or config.ocr_language,
        extract_dir=config.extract_dir,
        index_name=idx,
        max_depth=config.ingest_max_extract_depth,
    )
    backend = make_search_backend(config, index=idx)
    tracker = make_tracker(config)

    pipeline = IngestPipeline(
        extractor=extractor,
        search_backend=backend,
        tracker=tracker,
        index_name=idx,
        batch_size=batch_size or config.ingest_batch_size,
        max_workers=workers or config.ingest_max_workers,
    )
    return pipeline


def _print_ingest_summary(job, elapsed: float, avg_extraction: float) -> None:
    throughput = job.total_files / elapsed if elapsed > 0 else 0.0
    click.echo(f"Job {job.job_id} [{job.index_name}]: {job.status.value}")
    click.echo(f"  Files:      {job.total_files}")
    click.echo(f"  Extracted:  {job.extracted}")
    click.echo(f"  Indexed:    {job.processed}")
    click.echo(f"  Empty:      {job.empty}")
    click.echo(f"  Failed:     {job.failed}")
    click.echo(f"  Time:       {elapsed:.1f}s  ({throughput:.1f} files/s)")
    if avg_extraction > 0:
        click.echo(f"  Avg/file:   {avg_extraction:.3f}s")
    if job.failed > 0 or job.empty > 0:
        click.echo(f"  Run 'aum job {job.job_id} --errors' for details")


@main.command()
@click.argument("directory", type=click.Path(exists=True, file_okay=False, path_type=Path))
@click.option("--index", default=None, help="Target index name (default: from config)")
@click.option("--batch-size", default=None, type=int, help="Batch size for indexing")
@click.option("--workers", default=None, type=int, help="Number of extraction workers")
@click.option("--ocr/--no-ocr", default=None, help="Enable or disable OCR (default: from config)")
@click.option("--ocr-language", default=None, help="OCR language (e.g. eng, deu, fra+eng)")
def ingest(
    directory: Path,
    index: str | None,
    batch_size: int | None,
    workers: int | None,
    ocr: bool | None,
    ocr_language: str | None,
) -> None:
    """Ingest documents from a directory."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import default_index_name

    idx = index or default_index_name(config)
    pipeline = _make_ingest_pipeline(config, idx, batch_size, workers, ocr, ocr_language)

    job, elapsed, avg_extraction = pipeline.run(directory)
    _print_ingest_summary(job, elapsed, avg_extraction)


@main.command("init")
@click.option("--index", default=None, help="Index name (default: from config)")
def init_index(index: str | None) -> None:
    """Initialize the search index."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import default_index_name, make_search_backend

    idx = index or default_index_name(config)
    backend = make_search_backend(config, index=idx)
    vector_dim = config.embeddings_dimension if config.embeddings_enabled else None
    backend.initialize(vector_dimension=vector_dim)
    click.echo(f"Index '{idx}' initialized (backend={config.search_backend}, vectors={'yes' if vector_dim else 'no'})")


@main.command("reset")
@click.option("--index", default=None, help="Index name (default: from config)")
@click.confirmation_option(prompt="This will delete all indexed documents. Continue?")
def reset_index(index: str | None) -> None:
    """Delete and recreate the search index."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import default_index_name, make_search_backend

    idx = index or default_index_name(config)
    backend = make_search_backend(config, index=idx)
    backend.delete_index()
    vector_dim = config.embeddings_dimension if config.embeddings_enabled else None
    backend.initialize(vector_dimension=vector_dim)
    click.echo(f"Index '{idx}' reset.")


# --- Embedding ---


def _setup_embedder(config: AumConfig, idx: str, pull: bool = True):
    """Create search backend, tracker, and embedder for embedding operations.

    Validates model consistency and optionally auto-pulls Ollama models.
    Returns (search_backend, tracker, embedder).
    """
    from aum.api.deps import make_embedder, make_search_backend, make_tracker

    search = make_search_backend(config, index=idx)
    tracker = make_tracker(config)
    embedder = make_embedder(config)

    # Ensure the index has a vector field
    search.initialize(vector_dimension=embedder.dimension)

    # Check for embedding model mismatch
    prev = tracker.get_embedding_model(idx)
    if prev is not None:
        prev_model, prev_backend, prev_dim = prev
        if prev_model != config.embeddings_model:
            click.echo(
                f"WARNING: index '{idx}' was previously embedded with '{prev_model}' "
                f"but current model is '{config.embeddings_model}'.\n"
                f"Mixing models in one index will produce bad search results.\n"
                f"Run 'aum reset --index {idx}' and re-ingest to switch models.",
                err=True,
            )
            sys.exit(1)

    # Auto-pull for Ollama
    if pull and config.embeddings_backend == "ollama":
        from aum.embeddings.ollama import OllamaEmbedder

        if isinstance(embedder, OllamaEmbedder):
            click.echo(f"Pulling model '{config.embeddings_model}' (if needed)...")
            embedder.ensure_model()

    return search, tracker, embedder


@main.command()
@click.option("--index", default=None, help="Target index name (default: from config)")
@click.option("--batch-size", default=None, type=int, help="Batch size for embedding")
@click.option("--backend", default=None, type=click.Choice(["ollama", "openai"]), help="Embedding backend")
@click.option("--model", default=None, help="Embedding model name")
@click.option("--pull/--no-pull", default=True, help="Auto-pull model in Ollama (default: yes)")
def embed(
    index: str | None,
    batch_size: int | None,
    backend: str | None,
    model: str | None,
    pull: bool,
) -> None:
    """Generate embeddings for documents that don't have them yet.

    Runs as a separate job from ingest — queries the search index for
    un-embedded documents, generates embeddings via the configured backend
    (Ollama or OpenAI-compatible API), and updates them in place.
    """
    from aum.api.deps import default_index_name

    config = _load_config()
    _setup(config)

    if backend:
        config.embeddings_backend = backend
    if model:
        config.embeddings_model = model

    idx = index or default_index_name(config)
    search, tracker, embedder = _setup_embedder(config, idx, pull)

    bs = batch_size or config.embeddings_batch_size
    job = _run_embed_job(config, search, tracker, embedder, idx, bs)

    if job is not None and job.processed > 0:
        tracker.set_embedding_model(idx, config.embeddings_model, config.embeddings_backend, embedder.dimension)


def _run_embed_job(
    config: AumConfig,
    search,
    tracker,
    embedder,
    idx: str,
    bs: int,
    doc_ids: list[str] | None = None,
):
    """Run an embedding job, optionally for specific document IDs (retry).

    When *doc_ids* is None, processes all unembedded documents.  When
    provided, only re-embeds those specific documents.

    Returns the completed IngestJob.
    """
    import time
    from contextlib import nullcontext

    from aum.names import generate_name

    from rich.console import Console as RichConsole
    from rich.live import Live
    from rich.text import Text

    from aum.metrics import EMBEDDING_DOCS_FAILED, EMBEDDING_DOCS_PROCESSED, EMBEDDING_JOBS_ACTIVE
    from aum.models import JobStatus, JobType

    if doc_ids is not None:
        total = len(doc_ids)
        scroll_source = search.scroll_document_ids(doc_ids, batch_size=bs)
    else:
        total = search.count_unembedded()
        if total == 0:
            click.echo(f"All documents in '{idx}' already have embeddings.")
            return None
        scroll_source = search.scroll_unembedded(batch_size=bs)

    job_id = generate_name()
    tracker.create_job(
        job_id, source_dir=Path("."), total_files=total,
        index_name=idx, job_type=JobType.EMBED,
    )

    click.echo(
        f"Embedding {total} documents in '{idx}'"
        f" using {config.embeddings_backend}/{config.embeddings_model}"
        f" (batch_size={bs}, num_ctx={config.embeddings_context_length})"
        f"  [job {job_id}]"
    )

    EMBEDDING_JOBS_ACTIVE.inc()
    embedded = 0
    failed = 0
    job_start = time.monotonic()
    show_progress = sys.stderr.isatty()

    def _make_progress(total: int, done: int, n_failed: int, start: float) -> Text:
        elapsed = time.monotonic() - start
        pct = min(done / total * 100, 100) if total else 0
        rate = done / elapsed if elapsed > 0 else 0
        filled = int(20 * pct / 100)

        t = Text(no_wrap=True, overflow="crop")
        t.append("[", style="dim")
        t.append("█" * filled, style="blue")
        t.append("░" * (20 - filled), style="dim blue")
        t.append("] ", style="dim")
        t.append(f"{done}/{total} ({pct:.0f}%)", style="white")
        t.append(f"  {rate:.1f} docs/s", style="yellow")
        if n_failed:
            t.append(f"  fail:{n_failed}", style="bold red")
        m, s = divmod(int(elapsed), 60)
        t.append(f"  {m:02d}:{s:02d}", style="dim")
        return t

    try:
        console = RichConsole(stderr=True) if show_progress else None
        ctx: Live | nullcontext = (
            Live(
                _make_progress(total, 0, 0, job_start),
                console=console,
                refresh_per_second=4,
                transient=True,
            )
            if show_progress
            else nullcontext()
        )

        from aum.embeddings.chunking import chunk_text

        max_chunk_chars = config.embeddings_context_length * 4  # ~4 chars/token
        overlap_chars = config.embeddings_chunk_overlap

        with ctx as live:
            for scroll_batch in scroll_source:
                # Process each document: chunk, embed all chunks, store as nested array
                updates: list[tuple[str, list[list[float]]]] = []

                for doc_id, content in scroll_batch:
                    chunks = chunk_text(content, max_chars=max_chunk_chars, overlap_chars=overlap_chars)

                    try:
                        chunk_vectors = embedder.embed_documents(chunks)
                    except Exception as exc:
                        log.error("embedding failed", doc_id=doc_id, error=str(exc), n_chunks=len(chunks))
                        failed += 1
                        EMBEDDING_DOCS_FAILED.inc()
                        tracker.record_error(job_id, Path(doc_id), "EmbeddingError", str(exc))
                        continue

                    updates.append((doc_id, chunk_vectors))

                if updates:
                    n_failed = search.update_embeddings(updates)
                    batch_ok = len(updates) - n_failed
                    embedded += batch_ok
                    failed += n_failed
                    EMBEDDING_DOCS_PROCESSED.inc(batch_ok)
                    if n_failed:
                        EMBEDDING_DOCS_FAILED.inc(n_failed)

                tracker.update_progress(job_id, extracted=0, processed=embedded, failed=failed)
                elapsed = time.monotonic() - job_start
                rate = embedded / elapsed if elapsed > 0 else 0
                log.info("embedding batch complete", job_id=job_id, embedded=embedded, failed=failed, total=total, rate=f"{rate:.1f} docs/s")
                if live is not None:
                    live.update(_make_progress(total, embedded + failed, failed, job_start))

    except Exception:
        tracker.complete_job(job_id, JobStatus.FAILED)
        log.exception("embedding job failed", job_id=job_id)
        raise
    finally:
        EMBEDDING_JOBS_ACTIVE.dec()

    tracker.complete_job(job_id, JobStatus.COMPLETED)

    elapsed = time.monotonic() - job_start
    rate = embedded / elapsed if elapsed > 0 else 0
    click.echo(f"Job {job_id} [{idx}]: completed")
    click.echo(f"  Embedded:  {embedded}")
    click.echo(f"  Failed:    {failed}")
    click.echo(f"  Time:      {elapsed:.1f}s ({rate:.1f} docs/s)")
    if failed > 0:
        click.echo(f"  Run 'aum job {job_id} --errors' for details")

    return tracker.get_job(job_id)


# --- Retry ---


@main.command()
@click.argument("job_id")
@click.option("--include-empty/--no-include-empty", default=True, help="Retry files with empty extractions (default: yes)")
@click.option("--batch-size", default=None, type=int, help="Override batch size")
@click.option("--workers", default=None, type=int, help="Override worker count (ingest only)")
@click.option("--ocr/--no-ocr", default=None, help="Enable or disable OCR (ingest only)")
@click.option("--ocr-language", default=None, help="OCR language (ingest only, e.g. eng, deu, fra+eng)")
@click.option("--pull/--no-pull", default=True, help="Auto-pull model in Ollama (embed only)")
def retry(
    job_id: str,
    include_empty: bool,
    batch_size: int | None,
    workers: int | None,
    ocr: bool | None,
    ocr_language: str | None,
    pull: bool,
) -> None:
    """Retry failed items from a previous job.

    Looks up which items failed in the given job and re-processes only those.
    Works for both ingest and embedding jobs.  The retry itself is tracked as
    a new job that can be inspected and retried again if needed.

    By default, both failed and empty items are retried.  Use --no-include-empty
    to skip files that had empty extractions.
    """
    from aum.api.deps import make_tracker
    from aum.models import JobType

    config = _load_config()
    _setup(config)

    tracker = make_tracker(config)
    job = tracker.get_job(job_id)

    if job is None:
        click.echo(f"Job not found: {job_id}", err=True)
        sys.exit(1)

    if job.failed == 0 and job.empty == 0:
        click.echo(f"Job {job_id} has no failed or empty items.")
        return

    if job.job_type == JobType.INGEST:
        _retry_ingest(config, job, include_empty, batch_size, workers, ocr, ocr_language)
    elif job.job_type == JobType.EMBED:
        _retry_embed(config, job, batch_size, pull)
    else:
        click.echo(f"Unknown job type: {job.job_type.value}", err=True)
        sys.exit(1)


def _retry_ingest(
    config: AumConfig,
    job,
    include_empty: bool,
    batch_size: int | None,
    workers: int | None,
    ocr: bool | None,
    ocr_language: str | None,
) -> None:
    from aum.api.deps import make_tracker

    tracker = make_tracker(config)
    failed_paths = tracker.get_failed_paths(job.job_id, include_empty=include_empty)
    if not failed_paths:
        click.echo(f"No retryable errors in job {job.job_id}.")
        return

    # Filter to paths that still exist on disk
    existing = [p for p in failed_paths if p.exists()]
    skipped = len(failed_paths) - len(existing)
    if skipped:
        log.warning("skipping missing files", skipped=skipped, total=len(failed_paths))

    if not existing:
        click.echo("All failed files have been removed from disk.")
        return

    click.echo(f"Retrying {len(existing)} files from job {job.job_id} (index: {job.index_name})")
    if skipped:
        click.echo(f"  ({skipped} files no longer on disk, skipped)")

    pipeline = _make_ingest_pipeline(config, job.index_name, batch_size, workers, ocr, ocr_language)
    retry_job, elapsed, avg_extraction = pipeline.run_retry(existing, job.source_dir)
    _print_ingest_summary(retry_job, elapsed, avg_extraction)


def _retry_embed(config: AumConfig, job, batch_size: int | None, pull: bool) -> None:
    from aum.api.deps import make_tracker

    tracker = make_tracker(config)
    failed_doc_ids = tracker.get_failed_doc_ids(job.job_id)
    if not failed_doc_ids:
        click.echo(f"No retryable errors in job {job.job_id}.")
        return

    click.echo(f"Retrying {len(failed_doc_ids)} failed documents from job {job.job_id} (index: {job.index_name})")

    idx = job.index_name
    search, tracker, embedder = _setup_embedder(config, idx, pull)

    bs = batch_size or config.embeddings_batch_size
    retry_job = _run_embed_job(config, search, tracker, embedder, idx, bs, doc_ids=failed_doc_ids)

    if retry_job is not None and retry_job.processed > 0:
        tracker.set_embedding_model(idx, config.embeddings_model, config.embeddings_backend, embedder.dimension)


# --- Search ---


@main.command()
@click.argument("query")
@click.option("--index", multiple=True, help="Index name(s) to search (repeatable, default: from config)")
@click.option("--type", "search_type", type=click.Choice(["text", "hybrid"]), default="text")
@click.option("--limit", default=20, type=int, help="Max results")
@click.option("--offset", default=0, type=int, help="Offset for pagination")
@click.option("--file-type", multiple=True, help="Filter by file type (e.g. PDF, Word)")
@click.option("--creator", multiple=True, help="Filter by creator/author")
@click.option("--email", multiple=True, help="Filter by email address")
@click.option("--created-from", default=None, help="Filter by creation year (from)")
@click.option("--created-to", default=None, help="Filter by creation year (to)")
@click.option("--show-facets", is_flag=True, help="Display available facet values")
def search(
    query: str,
    index: tuple[str, ...],
    search_type: str,
    limit: int,
    offset: int,
    file_type: tuple[str, ...],
    creator: tuple[str, ...],
    email: tuple[str, ...],
    created_from: str | None,
    created_to: str | None,
    show_facets: bool,
) -> None:
    """Search indexed documents."""
    import re

    config = _load_config()
    _setup(config)

    from aum.api.deps import default_index_name, make_search_backend
    from aum.search.base import SearchResult

    idx_list = list(index) if index else [default_index_name(config)]
    joined_index = ",".join(idx_list)
    multi_index = len(idx_list) > 1
    backend = make_search_backend(config, index=joined_index)

    # Build filters from CLI options
    filters: dict[str, list[str]] = {}
    if file_type:
        filters["File Type"] = list(file_type)
    if creator:
        filters["Creator"] = list(creator)
    if email:
        filters["Email Addresses"] = list(email)
    if created_from or created_to:
        filters["Created"] = [created_from or "1900", created_to or "2099"]

    include_facets = show_facets
    search_filters = filters or None

    results: list[SearchResult]
    total: int
    facets: dict[str, list[str]] | None
    if search_type == "text":
        results, total, facets = backend.search_text(query, limit=limit, offset=offset, include_facets=include_facets, filters=search_filters)
    elif search_type == "hybrid":
        from aum.api.deps import make_embedder, make_tracker

        tracker = make_tracker(config)

        # Validate all indices have embeddings with the same model
        model_info: tuple[str, str, int] | None = None
        for idx in idx_list:
            prev = tracker.get_embedding_model(idx)
            if prev is None:
                click.echo(f"Error: no embeddings found for index '{idx}'. Run 'aum embed --index {idx}' first.", err=True)
                sys.exit(1)
            if model_info is None:
                model_info = prev
            else:
                prev_model, prev_backend, _ = prev
                if (prev_model, prev_backend) != (model_info[0], model_info[1]):
                    click.echo(
                        f"Error: embedding model mismatch across indices. "
                        f"'{idx_list[0]}' uses '{model_info[1]}/{model_info[0]}' "
                        f"but '{idx}' uses '{prev_backend}/{prev_model}'.",
                        err=True,
                    )
                    sys.exit(1)

        assert model_info is not None
        prev_model, prev_backend, _ = model_info
        config.embeddings_model = prev_model
        config.embeddings_backend = prev_backend
        embedder = make_embedder(config)
        vector = embedder.embed_query(query)
        results, total, facets = backend.search_hybrid(query, vector, limit=limit, offset=offset, include_facets=include_facets, filters=search_filters)
    else:
        results, total, facets = [], 0, None

    if not results:
        click.echo("No results found.")
        return

    if show_facets and facets:
        click.echo("--- Available Facets ---")
        for label, values in facets.items():
            click.echo(f"\n  {label}:")
            for v in values:
                click.echo(f"    - {v}")
        click.echo("")

    click.echo(f"Showing {offset + 1}-{offset + len(results)} of {total} results\n")

    for i, r in enumerate(results, offset + 1):
        index_prefix = f"[{r.index}] " if multi_index else ""
        click.echo(f"{i}. [{r.score:.3f}] {index_prefix}{r.display_path or r.source_path}")
        # Show key metadata inline
        meta_parts = []
        for key in ("File Type", "Creator", "Created"):
            val = r.metadata.get(key)
            if val:
                if isinstance(val, list):
                    val = ", ".join(val)
                meta_parts.append(f"{key}: {val}")
        if meta_parts:
            click.echo(f"   [{' | '.join(meta_parts)}]")
        # Strip HTML highlight tags for terminal display
        snippet = re.sub(r"</?mark>", "", r.snippet)
        snippet = snippet.replace("\n", " ").strip()
        if len(snippet) > 200:
            snippet = snippet[:200] + "..."
        click.echo(f"   {snippet}")
        click.echo("")


# --- Index management ---


@main.command("indices")
def list_indices() -> None:
    """List available search indices."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_search_backend

    backend = make_search_backend(config)
    indices = backend.list_indices()

    if not indices:
        click.echo("No indices found.")
        return

    click.echo(f"{'INDEX':<30} {'DOCS'}")
    click.echo("-" * 42)
    for idx in indices:
        idx_backend = make_search_backend(config, index=idx)
        count = idx_backend.document_count()
        click.echo(f"{idx:<30} {count}")


# --- Job monitoring ---


@main.command("jobs")
@click.option("--status", type=click.Choice(["pending", "running", "completed", "failed"]), default=None)
def list_jobs(status: str | None) -> None:
    """List all jobs (ingest and embedding)."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_tracker
    from aum.models import JobStatus

    tracker = make_tracker(config)
    job_status = JobStatus(status) if status else None
    jobs = tracker.list_jobs(status=job_status)

    if not jobs:
        click.echo("No jobs found.")
        return

    click.echo(f"{'JOB ID':<26} {'TYPE':<8} {'INDEX':<16} {'STATUS':<12} {'FILES':<8} {'OK':<8} {'EMPTY':<8} {'FAILED':<8} {'CREATED'}")
    click.echo("-" * 120)
    for j in jobs:
        files = str(j.total_files) if j.total_files else "?"
        created = f"{j.created_at:%Y-%m-%d %H:%M}"
        click.echo(f"{j.job_id:<26} {j.job_type.value:<8} {j.index_name:<16} {j.status.value:<12} {files:<8} {j.processed:<8} {j.empty:<8} {j.failed:<8} {created}")


@main.command("job")
@click.argument("job_id")
@click.option("--errors", is_flag=True, help="Show error details")
def show_job(job_id: str, errors: bool) -> None:
    """Show details of a specific job."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_tracker

    tracker = make_tracker(config)
    job = tracker.get_job(job_id)

    if job is None:
        click.echo(f"Job not found: {job_id}", err=True)
        sys.exit(1)

    from aum.models import JobType

    click.echo(f"Job ID:     {job.job_id}")
    click.echo(f"Type:       {job.job_type.value}")
    click.echo(f"Index:      {job.index_name}")
    if job.job_type == JobType.INGEST:
        click.echo(f"Source:     {job.source_dir}")
    click.echo(f"Status:     {job.status.value}")
    click.echo(f"Files:      {job.total_files}")
    if job.job_type == JobType.INGEST:
        click.echo(f"Extracted:  {job.extracted}")
    click.echo(f"Processed:  {job.processed}")
    if job.job_type == JobType.INGEST:
        click.echo(f"Empty:      {job.empty}")
    click.echo(f"Failed:     {job.failed}")
    click.echo(f"Created:    {job.created_at:%Y-%m-%d %H:%M:%S}")
    if job.finished_at:
        click.echo(f"Finished:   {job.finished_at:%Y-%m-%d %H:%M:%S}")
    if job.failed > 0:
        click.echo(f"\nRetry with: aum retry {job.job_id}")

    if errors and job.errors:
        click.echo(f"\nErrors ({len(job.errors)}):")
        click.echo("-" * 70)
        for e in job.errors:
            click.echo(f"  {e.file_path}")
            click.echo(f"    [{e.error_type}] {e.message}")


# --- User management ---


@main.group()
def user() -> None:
    """Manage users."""


def _generate_password(length: int = 20) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return "".join(secrets.choice(alphabet) for _ in range(length))


@user.command("create")
@click.argument("username")
@click.option("--admin", is_flag=True, help="Create as admin user")
@click.option("--generate-password", is_flag=True, help="Generate a secure random password")
def user_create(username: str, admin: bool, generate_password: bool) -> None:
    """Create a local user."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_local_auth

    if generate_password:
        password = _generate_password()
    else:
        password = getpass.getpass("Password: ")
        confirm = getpass.getpass("Confirm password: ")
        if password != confirm:
            click.echo("Passwords do not match.", err=True)
            sys.exit(1)

    auth = make_local_auth(config)
    try:
        user = auth.create_user(username, password, is_admin=admin)
        click.echo(f"User created: {user.username} (admin={user.is_admin})")
        if generate_password:
            click.echo(f"Generated password: {password}")
    except Exception as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


@user.command("list")
def user_list() -> None:
    """List all users."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_local_auth

    auth = make_local_auth(config)
    users = auth.list_users()

    if not users:
        click.echo("No users found.")
        return

    click.echo(f"{'USERNAME':<20} {'ADMIN':<8} {'AUTH'}")
    click.echo("-" * 40)
    for u in users:
        auth_type = "local" if u.password_hash else "oauth"
        click.echo(f"{u.username:<20} {'yes' if u.is_admin else 'no':<8} {auth_type}")


@user.command("delete")
@click.argument("username")
@click.confirmation_option(prompt="Are you sure you want to delete this user?")
def user_delete(username: str) -> None:
    """Delete a user."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_local_auth

    auth = make_local_auth(config)
    if auth.delete_user(username):
        click.echo(f"User deleted: {username}")
    else:
        click.echo(f"User not found: {username}", err=True)
        sys.exit(1)


@user.command("grant")
@click.argument("username")
@click.argument("index_name")
def user_grant(username: str, index_name: str) -> None:
    """Grant a user access to an index."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_permission_manager

    perms = make_permission_manager(config)
    try:
        if perms.grant(username, index_name):
            click.echo(f"Granted {username} access to '{index_name}'")
        else:
            click.echo(f"{username} already has access to '{index_name}'")
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


@user.command("revoke")
@click.argument("username")
@click.argument("index_name")
def user_revoke(username: str, index_name: str) -> None:
    """Revoke a user's access to an index."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_permission_manager

    perms = make_permission_manager(config)
    try:
        if perms.revoke(username, index_name):
            click.echo(f"Revoked {username} access to '{index_name}'")
        else:
            click.echo(f"{username} did not have access to '{index_name}'")
    except ValueError as exc:
        click.echo(f"Error: {exc}", err=True)
        sys.exit(1)


@user.command("set-password")
@click.argument("username")
@click.option("--generate-password", is_flag=True, help="Generate a secure random password")
def user_set_password(username: str, generate_password: bool) -> None:
    """Reset a user's password."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_local_auth

    if generate_password:
        password = _generate_password()
    else:
        password = getpass.getpass("New password: ")
        confirm = getpass.getpass("Confirm new password: ")
        if password != confirm:
            click.echo("Passwords do not match.", err=True)
            sys.exit(1)

    auth = make_local_auth(config)
    if auth.set_password(username, password):
        click.echo(f"Password updated for {username}")
        if generate_password:
            click.echo(f"Generated password: {password}")
    else:
        click.echo(f"User not found: {username}", err=True)
        sys.exit(1)


@user.command("set-admin")
@click.argument("username")
@click.option("--revoke", is_flag=True, help="Remove admin status")
def user_set_admin(username: str, revoke: bool) -> None:
    """Set or revoke admin status for a user."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_local_auth

    auth = make_local_auth(config)
    is_admin = not revoke
    if auth.set_admin(username, is_admin):
        click.echo(f"{'Granted' if is_admin else 'Revoked'} admin status for {username}")
    else:
        click.echo(f"User not found: {username}", err=True)
        sys.exit(1)


@user.command("token")
@click.argument("username")
@click.option("--days", default=365, type=int, help="Token lifetime in days (default: 365)")
def user_token(username: str, days: int) -> None:
    """Generate a long-lived API token for a user."""
    config = _load_config()
    _setup(config)
    _ensure_jwt_secret(config)

    from aum.api.deps import make_local_auth, make_token_manager

    auth = make_local_auth(config)
    user_obj = auth.get_user_by_username(username)
    if user_obj is None:
        click.echo(f"User not found: {username}", err=True)
        sys.exit(1)

    token_mgr = make_token_manager(config)
    token = token_mgr.create_api_token(user_obj, expire_days=days)
    click.echo(f"API token for {username} (expires in {days} days):")
    click.echo(token)


# --- Config ---


@main.command("config")
def show_config() -> None:
    """Show resolved configuration."""
    config = _load_config()
    for key, value in config.model_dump().items():
        if "secret" in key.lower() or "password" in key.lower():
            value = "***"
        click.echo(f"{key}: {value}")


# --- Server ---


@main.command("serve")
@click.option("--host", default=None, help="Bind host")
@click.option("--port", default=None, type=int, help="Bind port")
def serve(host: str | None, port: int | None) -> None:
    """Start the web server (API + frontend)."""
    config = _load_config()
    _setup(config)

    _ensure_jwt_secret(config)

    import uvicorn

    from aum.api.app import create_app

    app = create_app(config)
    uvicorn.run(
        app,
        host=host or config.host,
        port=port or config.port,
        log_level=config.log_level.lower(),
    )
