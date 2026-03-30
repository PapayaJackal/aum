from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse, Response
from pydantic import BaseModel

from aum.api.deps import (
    default_index_name,
    get_config,
    get_optional_user,
    get_permission_manager,
    make_search_backend,
)
from aum.api.email_preview import extract_email_html
from aum.auth.models import User
from aum.auth.permissions import PermissionDeniedError, PermissionManager
from aum.metrics import DOCUMENT_DOWNLOADS, DOCUMENT_PREVIEWS, DOCUMENT_VIEWS, THREAD_LOOKUPS
from aum.search.base import HIDDEN_METADATA_KEYS, normalize_message_id

log = structlog.get_logger()
router = APIRouter(prefix="/api", tags=["search"])

_PREVIEWABLE_TYPES = frozenset(
    {
        "image/jpeg",
        "image/png",
        "image/gif",
        "image/webp",
        "image/bmp",
        "application/pdf",
        "message/rfc822",
        "text/html",
    }
)
_BLOCKED_PREVIEW_TYPES = frozenset({"image/svg+xml"})
_HTML_CSP = "default-src 'none'; style-src 'unsafe-inline'; img-src data:; sandbox"
_BINARY_CSP = "default-src 'none'; style-src 'unsafe-inline'"

_INTERNAL_METADATA_KEYS = {"_aum_display_path", "_aum_extracted_from"}
_EXCLUDED_METADATA_KEYS = _INTERNAL_METADATA_KEYS | HIDDEN_METADATA_KEYS


def _clean_metadata(metadata: dict) -> dict:
    return {k: v for k, v in metadata.items() if k not in _EXCLUDED_METADATA_KEYS}


class SearchResultResponse(BaseModel):
    doc_id: str
    display_path: str
    display_path_highlighted: str
    score: float
    snippet: str
    metadata: dict[str, str | list[str]]
    index: str = ""


class SearchResponse(BaseModel):
    results: list[SearchResultResponse]
    total: int
    facets: dict[str, list[str]] | None = None


class AttachmentResponse(BaseModel):
    doc_id: str
    display_path: str


class ExtractedFromResponse(BaseModel):
    doc_id: str
    display_path: str


class ThreadMessageResponse(BaseModel):
    doc_id: str
    display_path: str
    subject: str
    sender: str
    date: str
    snippet: str


class DocumentResponse(BaseModel):
    doc_id: str
    display_path: str
    content: str
    metadata: dict[str, str | list[str]]
    attachments: list[AttachmentResponse] = []
    extracted_from: ExtractedFromResponse | None = None
    thread: list[ThreadMessageResponse] = []


class IndexInfo(BaseModel):
    name: str
    has_embeddings: bool


class IndicesResponse(BaseModel):
    indices: list[IndexInfo]


@router.get("/indices", response_model=IndicesResponse)
async def list_indices(
    user: Annotated[User | None, Depends(get_optional_user)],
) -> IndicesResponse:
    config = get_config()
    backend = make_search_backend(config)
    all_indices = backend.list_indices()
    if user is not None and not user.is_admin:
        perms = get_permission_manager()
        all_indices = [idx for idx in all_indices if perms.check(user, idx)]
    from aum.api.deps import make_tracker

    tracker = make_tracker(config)
    return IndicesResponse(
        indices=[
            IndexInfo(name=idx, has_embeddings=tracker.get_embedding_model(idx) is not None) for idx in all_indices
        ]
    )


def _check_index_access(user: User | None, index: str, perms: PermissionManager) -> None:
    if user is None:
        return  # Public mode — allow all
    try:
        perms.require(user, index)
    except PermissionDeniedError as exc:
        raise HTTPException(status_code=403, detail=str(exc))


@router.get("/search", response_model=SearchResponse)
async def search(
    q: Annotated[str, Query(min_length=1)],
    user: Annotated[User | None, Depends(get_optional_user)],
    index: str = "",
    type: str = "text",
    limit: Annotated[int, Query(ge=1, le=200)] = 20,
    offset: Annotated[int, Query(ge=0, le=100_000)] = 0,
    filters: Annotated[str | None, Query()] = None,
    semantic_ratio: Annotated[float | None, Query(ge=0.0, le=1.0)] = None,
    sort: Annotated[str | None, Query(pattern=r"^(date|size):(asc|desc)$")] = None,
) -> SearchResponse:
    config = get_config()
    idx_raw = index or default_index_name(config)
    idx_list = [i.strip() for i in idx_raw.split(",") if i.strip()]

    perms = get_permission_manager()
    for idx in idx_list:
        _check_index_access(user, idx, perms)

    joined_index = ",".join(idx_list)
    backend = make_search_backend(config, index=joined_index)

    parsed_filters: dict[str, list[str]] | None = None
    if filters:
        try:
            parsed_filters = json.loads(filters)
        except (json.JSONDecodeError, TypeError):
            raise HTTPException(status_code=400, detail="Invalid filters JSON")

    include_facets = offset == 0 or bool(parsed_filters)

    if type == "text":
        results, total, facets = backend.search_text(
            q, limit=limit, offset=offset, include_facets=include_facets, filters=parsed_filters, sort=sort
        )
    elif type == "hybrid":
        embedder = _get_embedder_for_indices(idx_list)
        vector = embedder.embed_query(q)
        results, total, facets = backend.search_hybrid(
            q,
            vector,
            limit=limit,
            offset=offset,
            include_facets=include_facets,
            filters=parsed_filters,
            semantic_ratio=semantic_ratio,
            sort=sort,
        )
    else:
        raise HTTPException(status_code=400, detail=f"Unknown search type: {type}")

    log.info(
        "search completed",
        query=q,
        type=type,
        index=joined_index,
        limit=limit,
        offset=offset,
        results=len(results),
        total=total,
        facets_included=include_facets,
        filters=parsed_filters,
        semantic_ratio=semantic_ratio,
        sort=sort,
    )

    return SearchResponse(
        results=[
            SearchResultResponse(
                doc_id=r.doc_id,
                display_path=r.display_path,
                display_path_highlighted=r.display_path_highlighted,
                score=r.score,
                snippet=r.snippet,
                metadata=_clean_metadata(r.metadata),
                index=r.index,
            )
            for r in results
        ],
        total=total,
        facets=facets,
    )


@router.get("/documents/{doc_id}", response_model=DocumentResponse)
async def get_document(
    doc_id: str,
    user: Annotated[User | None, Depends(get_optional_user)],
    index: str = "",
) -> DocumentResponse:
    config = get_config()
    idx = index or default_index_name(config)

    perms = get_permission_manager()
    _check_index_access(user, idx, perms)

    backend = make_search_backend(config, index=idx)
    doc = backend.get_document(doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")

    DOCUMENT_VIEWS.inc()

    attachments = [
        AttachmentResponse(doc_id=a.doc_id, display_path=a.display_path)
        for a in backend.find_attachments(doc.display_path)
    ]

    extracted_from: ExtractedFromResponse | None = None
    if doc.extracted_from:
        parent = backend.find_by_display_path(doc.extracted_from)
        if parent is not None:
            extracted_from = ExtractedFromResponse(
                doc_id=parent.doc_id,
                display_path=parent.display_path,
            )

    # Build email thread if document has a Message-ID header.
    # Tika may store these under "Message:Raw-Header:X" or plain "X" keys.
    thread: list[ThreadMessageResponse] = []
    raw_message_id = doc.metadata.get("Message:Raw-Header:Message-ID") or doc.metadata.get("Message-ID") or ""
    if isinstance(raw_message_id, str) and raw_message_id:
        message_id = normalize_message_id(raw_message_id)
        raw_in_reply_to = doc.metadata.get("Message:Raw-Header:In-Reply-To") or doc.metadata.get("In-Reply-To") or ""
        in_reply_to = normalize_message_id(raw_in_reply_to) if isinstance(raw_in_reply_to, str) else ""
        raw_refs = doc.metadata.get("Message:Raw-Header:References") or doc.metadata.get("References") or ""
        if isinstance(raw_refs, str) and raw_refs:
            references = [normalize_message_id(r) for r in raw_refs.split() if r.strip()]
        elif isinstance(raw_refs, list):
            references = [normalize_message_id(r) for r in raw_refs if r]
        else:
            references = []

        thread_results = backend.find_thread(message_id, in_reply_to, references)
        THREAD_LOOKUPS.inc()
        log.info("thread lookup", doc_id=doc_id, index=idx, thread_size=len(thread_results))

        for tr in thread_results:
            if tr.doc_id == doc_id:
                continue
            subject = ""
            for k in ("dc:subject", "subject"):
                val = tr.metadata.get(k, "")
                if val:
                    subject = val if isinstance(val, str) else str(val)
                    break
            sender = ""
            sender_val = tr.metadata.get("Message-From", "")
            if sender_val:
                sender = sender_val if isinstance(sender_val, str) else ", ".join(sender_val)
            date = ""
            for k in ("dcterms:created", "Creation-Date", "meta:creation-date", "created", "date"):
                date_val = tr.metadata.get(k, "")
                if date_val:
                    date = date_val if isinstance(date_val, str) else str(date_val)
                    break
            snippet = tr.snippet[:200] if tr.snippet else ""
            thread.append(
                ThreadMessageResponse(
                    doc_id=tr.doc_id,
                    display_path=tr.display_path,
                    subject=subject,
                    sender=sender,
                    date=date,
                    snippet=snippet,
                )
            )

        # Sort thread chronologically by date.
        thread.sort(key=lambda m: m.date)

    return DocumentResponse(
        doc_id=doc.doc_id,
        display_path=doc.display_path,
        content=doc.snippet,
        metadata=_clean_metadata(doc.metadata),
        attachments=attachments,
        extracted_from=extracted_from,
        thread=thread,
    )


def _safe_file_path(source_path: str) -> Path:
    """Resolve stored source_path, rejecting symlinks and paths outside allowed directories."""
    path = Path(source_path).resolve()
    if path.is_symlink():
        raise HTTPException(status_code=403, detail="Access to symlinked files is not permitted")
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Source file not found on disk")
    # Validate the resolved path falls within the extract directory.
    config = get_config()
    extract_dir = Path(config.extract_dir).resolve()
    if not str(path).startswith(str(extract_dir) + "/"):
        raise HTTPException(status_code=403, detail="Access denied: file is outside the data directory")
    return path


@router.get("/documents/{doc_id}/download")
async def download_document(
    doc_id: str,
    user: Annotated[User | None, Depends(get_optional_user)],
    index: str = "",
) -> FileResponse:
    config = get_config()
    idx = index or default_index_name(config)

    perms = get_permission_manager()
    _check_index_access(user, idx, perms)

    backend = make_search_backend(config, index=idx)
    doc = backend.get_document(doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")

    file_path = _safe_file_path(doc.source_path)
    DOCUMENT_DOWNLOADS.inc()
    log.info("document download", doc_id=doc_id, index=idx)
    return FileResponse(path=str(file_path), filename=file_path.name)


@router.get("/documents/{doc_id}/preview")
async def preview_document(
    doc_id: str,
    user: Annotated[User | None, Depends(get_optional_user)],
    index: str = "",
) -> Response:
    config = get_config()
    idx = index or default_index_name(config)

    perms = get_permission_manager()
    _check_index_access(user, idx, perms)

    backend = make_search_backend(config, index=idx)
    doc = backend.get_document(doc_id)
    if doc is None:
        raise HTTPException(status_code=404, detail="Document not found")

    raw_ct = doc.metadata.get("Content-Type", "")
    content_type = raw_ct[0] if isinstance(raw_ct, list) else raw_ct
    # Strip parameters (e.g. "image/jpeg; charset=..." → "image/jpeg")
    content_type = content_type.split(";")[0].strip().lower()

    if content_type in _BLOCKED_PREVIEW_TYPES:
        raise HTTPException(status_code=403, detail="Preview of this file type is not permitted")
    if content_type not in _PREVIEWABLE_TYPES:
        raise HTTPException(status_code=415, detail="File type is not previewable")

    file_path = _safe_file_path(doc.source_path)
    DOCUMENT_PREVIEWS.labels(content_type=content_type).inc()
    log.info("document preview", doc_id=doc_id, index=idx, content_type=content_type)

    if content_type == "message/rfc822":
        html_bytes = extract_email_html(file_path)
        response = Response(content=html_bytes, media_type="text/html")
        response.headers["Content-Disposition"] = "inline"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Content-Security-Policy"] = _HTML_CSP
        return response

    response = FileResponse(path=str(file_path), media_type=content_type, filename=file_path.name)
    response.headers["Content-Disposition"] = "inline"
    response.headers["X-Content-Type-Options"] = "nosniff"
    csp = _HTML_CSP if content_type == "text/html" else _BINARY_CSP
    response.headers["Content-Security-Policy"] = csp
    return response


def _get_embedder_for_indices(idx_list: list[str]):  # noqa: ANN202
    """Load the embedder for one or more indices, validating all have compatible embeddings."""
    config = get_config()
    from aum.api.deps import make_embedder, make_tracker

    tracker = make_tracker(config)

    model_info: tuple[str, str, int] | None = None
    for idx in idx_list:
        prev = tracker.get_embedding_model(idx)
        if prev is None:
            raise HTTPException(
                status_code=400,
                detail=f"No embeddings found for index '{idx}'. Run 'aum embed --index {idx}' first.",
            )
        if model_info is None:
            model_info = prev
        else:
            prev_model, prev_backend, _ = prev
            if (prev_model, prev_backend) != (model_info[0], model_info[1]):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Embedding model mismatch: index '{idx_list[0]}' uses "
                        f"'{model_info[1]}/{model_info[0]}' but index '{idx}' uses "
                        f"'{prev_backend}/{prev_model}'. "
                        f"Hybrid search requires all indices to use the same embedding model."
                    ),
                )

    if model_info is None:
        raise HTTPException(status_code=400, detail="No indices provided for embedding lookup.")
    idx_model, idx_backend, _ = model_info
    # Use a shallow copy to avoid mutating the @lru_cache'd config singleton
    embed_config = config.model_copy(
        update={
            "embeddings_model": idx_model,
            "embeddings_backend": idx_backend,
        }
    )
    return make_embedder(embed_config)
