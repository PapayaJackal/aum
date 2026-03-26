from __future__ import annotations

import json
from pathlib import Path
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel

from aum.api.deps import (
    default_index_name,
    get_config,
    get_current_user,
    get_permission_manager,
    make_search_backend,
)
from aum.auth.models import User
from aum.auth.permissions import PermissionDeniedError, PermissionManager
from aum.search.base import HIDDEN_METADATA_KEYS

log = structlog.get_logger()
router = APIRouter(prefix="/api", tags=["search"])

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


class DocumentResponse(BaseModel):
    doc_id: str
    display_path: str
    content: str
    metadata: dict[str, str | list[str]]
    attachments: list[AttachmentResponse] = []
    extracted_from: ExtractedFromResponse | None = None


class IndexInfo(BaseModel):
    name: str
    has_embeddings: bool


class IndicesResponse(BaseModel):
    indices: list[IndexInfo]


@router.get("/indices", response_model=IndicesResponse)
async def list_indices(
    user: Annotated[User, Depends(get_current_user)],
) -> IndicesResponse:
    config = get_config()
    backend = make_search_backend(config)
    all_indices = backend.list_indices()
    if not user.is_admin:
        perms = get_permission_manager()
        all_indices = [idx for idx in all_indices if perms.check(user, idx)]
    from aum.api.deps import make_tracker
    tracker = make_tracker(config)
    return IndicesResponse(
        indices=[
            IndexInfo(name=idx, has_embeddings=tracker.get_embedding_model(idx) is not None)
            for idx in all_indices
        ]
    )


def _check_index_access(user: User, index: str, perms: PermissionManager) -> None:
    try:
        perms.require(user, index)
    except PermissionDeniedError as exc:
        raise HTTPException(status_code=403, detail=str(exc))


@router.get("/search", response_model=SearchResponse)
async def search(
    q: Annotated[str, Query(min_length=1)],
    user: Annotated[User, Depends(get_current_user)],
    index: str = "",
    type: str = "text",
    limit: Annotated[int, Query(ge=1, le=200)] = 20,
    offset: Annotated[int, Query(ge=0, le=100_000)] = 0,
    filters: Annotated[str | None, Query()] = None,
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
        results, total, facets = backend.search_text(q, limit=limit, offset=offset, include_facets=include_facets, filters=parsed_filters)
    elif type == "hybrid":
        embedder = _get_embedder_for_indices(idx_list)
        vector = embedder.embed_query(q)
        results, total, facets = backend.search_hybrid(q, vector, limit=limit, offset=offset, include_facets=include_facets, filters=parsed_filters)
    else:
        raise HTTPException(status_code=400, detail=f"Unknown search type: {type}")

    log.info("search completed", query=q, type=type, index=joined_index, limit=limit, offset=offset, results=len(results), total=total, facets_included=include_facets, filters=parsed_filters)

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
    user: Annotated[User, Depends(get_current_user)],
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

    attachments = [
        AttachmentResponse(doc_id=a.doc_id, display_path=a.display_path)
        for a in backend.find_attachments(doc.display_path)
    ]

    extracted_from: ExtractedFromResponse | None = None
    if doc.extracted_from:
        parent = backend.find_by_display_path(doc.extracted_from)
        if parent is not None:
            extracted_from = ExtractedFromResponse(
                doc_id=parent.doc_id, display_path=parent.display_path,
            )

    return DocumentResponse(
        doc_id=doc.doc_id,
        display_path=doc.display_path,
        content=doc.snippet,
        metadata=_clean_metadata(doc.metadata),
        attachments=attachments,
        extracted_from=extracted_from,
    )


def _safe_file_path(source_path: str) -> Path:
    """Resolve stored source_path, rejecting symlinks to prevent path traversal."""
    path = Path(source_path)
    if path.is_symlink():
        raise HTTPException(status_code=403, detail="Access to symlinked files is not permitted")
    if not path.is_file():
        raise HTTPException(status_code=404, detail="Source file not found on disk")
    return path


@router.get("/documents/{doc_id}/download")
async def download_document(
    doc_id: str,
    user: Annotated[User, Depends(get_current_user)],
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
    return FileResponse(path=str(file_path), filename=file_path.name)


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

    assert model_info is not None
    prev_model, prev_backend, _ = model_info
    config.embeddings_model = prev_model
    config.embeddings_backend = prev_backend
    return make_embedder(config)
