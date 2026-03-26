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


@click.group()
@click.version_option(version=__version__)
def main() -> None:
    """aum - document search engine."""


# --- Ingest & Index ---


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

    from aum.api.deps import default_index_name, make_search_backend, make_tracker
    from aum.extraction.tika import TikaExtractor
    from aum.ingest.pipeline import IngestPipeline

    idx = index or default_index_name(config)

    extractor = TikaExtractor(
        server_url=config.tika_server_url,
        ocr_enabled=ocr if ocr is not None else config.ocr_enabled,
        ocr_language=ocr_language or config.ocr_language,
        extract_dir=config.extract_dir,
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

    job, elapsed, avg_extraction = pipeline.run(directory)
    throughput = job.total_files / elapsed if elapsed > 0 else 0.0
    click.echo(f"Job {job.job_id} [{job.index_name}]: {job.status.value}")
    click.echo(f"  Files:      {job.total_files}")
    click.echo(f"  Extracted:  {job.extracted}")
    click.echo(f"  Indexed:    {job.processed}")
    click.echo(f"  Failed:     {job.failed}")
    click.echo(f"  Time:       {elapsed:.1f}s  ({throughput:.1f} files/s)")
    if avg_extraction > 0:
        click.echo(f"  Avg/file:   {avg_extraction:.3f}s")
    if job.failed > 0:
        click.echo(f"  Run 'aum job {job.job_id} --errors' for details")


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
    import time
    from contextlib import nullcontext

    from rich.console import Console as RichConsole
    from rich.live import Live
    from rich.text import Text

    from aum.api.deps import default_index_name, make_embedder, make_search_backend
    from aum.metrics import EMBEDDING_DOCS_FAILED, EMBEDDING_DOCS_PROCESSED, EMBEDDING_JOBS_ACTIVE

    config = _load_config()
    _setup(config)

    if backend:
        config.embeddings_backend = backend
    if model:
        config.embeddings_model = model

    from aum.api.deps import make_tracker

    idx = index or default_index_name(config)
    search = make_search_backend(config, index=idx)
    tracker = make_tracker(config)
    embedder = make_embedder(config)

    # Ensure the index has a vector field
    search.initialize(vector_dimension=embedder.dimension)

    # Check for embedding model mismatch
    prev = tracker.get_embedding_model(idx)
    if prev is not None:
        prev_model, prev_dim = prev
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

    total_unembedded = search.count_unembedded()
    if total_unembedded == 0:
        click.echo(f"All documents in '{idx}' already have embeddings.")
        return

    bs = batch_size or config.embeddings_batch_size
    click.echo(
        f"Embedding {total_unembedded} documents in '{idx}'"
        f" using {config.embeddings_backend}/{config.embeddings_model}"
        f" (batch_size={bs}, num_ctx={config.embeddings_context_length})"
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
                _make_progress(total_unembedded, 0, 0, job_start),
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
            for scroll_batch in search.scroll_unembedded(batch_size=bs):
                # Process each document: chunk, embed all chunks, store as nested array
                updates: list[tuple[str, list[list[float]]]] = []
                batch_failed = False

                for doc_id, content in scroll_batch:
                    chunks = chunk_text(content, max_chars=max_chunk_chars, overlap_chars=overlap_chars)

                    try:
                        chunk_vectors = embedder.embed_documents(chunks)
                    except Exception as exc:
                        log.error("embedding failed", doc_id=doc_id, error=str(exc), n_chunks=len(chunks))
                        failed += 1
                        EMBEDDING_DOCS_FAILED.inc()
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

                if live is not None:
                    live.update(_make_progress(total_unembedded, embedded, failed, job_start))
                else:
                    log.info("embedding progress", embedded=embedded, failed=failed, total=total_unembedded)

    finally:
        EMBEDDING_JOBS_ACTIVE.dec()

    elapsed = time.monotonic() - job_start
    rate = embedded / elapsed if elapsed > 0 else 0
    click.echo(f"Embedded:  {embedded}")
    click.echo(f"Failed:    {failed}")
    click.echo(f"Time:      {elapsed:.1f}s ({rate:.1f} docs/s)")

    if embedded > 0:
        tracker.set_embedding_model(idx, config.embeddings_model, embedder.dimension)


# --- Search ---


@main.command()
@click.argument("query")
@click.option("--index", default=None, help="Index name (default: from config)")
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
    index: str | None,
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

    idx = index or default_index_name(config)
    backend = make_search_backend(config, index=idx)

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
        prev = tracker.get_embedding_model(idx)
        if prev is None:
            click.echo(f"Error: no embeddings found for index '{idx}'. Run 'aum embed' first.", err=True)
            sys.exit(1)

        # Use the model that was actually used to embed this index
        prev_model, _ = prev
        config.embeddings_model = prev_model
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
        click.echo(f"{i}. [{r.score:.3f}] {r.display_path or r.source_path}")
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
    """List ingest jobs."""
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

    click.echo(f"{'JOB ID':<14} {'INDEX':<16} {'STATUS':<12} {'FILES':<8} {'EXTRACTED':<11} {'INDEXED':<9} {'FAILED':<8} {'CREATED'}")
    click.echo("-" * 100)
    for j in jobs:
        files = str(j.total_files) if j.total_files else "?"
        created = f"{j.created_at:%Y-%m-%d %H:%M}"
        click.echo(f"{j.job_id:<14} {j.index_name:<16} {j.status.value:<12} {files:<8} {j.extracted:<11} {j.processed:<9} {j.failed:<8} {created}")


@main.command("job")
@click.argument("job_id")
@click.option("--errors", is_flag=True, help="Show error details")
def show_job(job_id: str, errors: bool) -> None:
    """Show details of a specific ingest job."""
    config = _load_config()
    _setup(config)

    from aum.api.deps import make_tracker

    tracker = make_tracker(config)
    job = tracker.get_job(job_id)

    if job is None:
        click.echo(f"Job not found: {job_id}", err=True)
        sys.exit(1)

    click.echo(f"Job ID:     {job.job_id}")
    click.echo(f"Index:      {job.index_name}")
    click.echo(f"Source:     {job.source_dir}")
    click.echo(f"Status:     {job.status.value}")
    click.echo(f"Files:      {job.total_files}")
    click.echo(f"Extracted:  {job.extracted}")
    click.echo(f"Indexed:    {job.processed}")
    click.echo(f"Failed:     {job.failed}")
    click.echo(f"Created:    {job.created_at:%Y-%m-%d %H:%M:%S}")
    if job.finished_at:
        click.echo(f"Finished:   {job.finished_at:%Y-%m-%d %H:%M:%S}")

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

    import uvicorn

    from aum.api.app import create_app

    app = create_app(config)
    uvicorn.run(
        app,
        host=host or config.host,
        port=port or config.port,
        log_level=config.log_level.lower(),
    )
