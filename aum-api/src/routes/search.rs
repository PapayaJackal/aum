//! Search, document detail, download, and preview API routes.

use std::collections::HashMap;
use std::path::Path;

use indexmap::IndexMap;

use axum::body::Body;
use axum::extract::{Path as AxumPath, Query, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::Response;
use axum::routing::get;
use axum::{Json, Router};
use futures::StreamExt as _;
use tracing::info;

use aum_core::search::backend::SearchBackend;
use aum_core::search::constants::FACET_ORDER;
use aum_core::search::types::{FilterMap, SearchRequest, SearchResult, SortSpec};
use aum_core::search::utils::normalize_message_id;

use crate::dto::{
    AttachmentResponse, DocumentParams, DocumentResponse, ExtractedFromResponse, SearchParams,
    SearchResponse, SearchResultResponse, SearchType, ThreadMessageResponse,
};
use crate::error::ApiError;
use crate::extractors::auth::OptionalUser;
use crate::state::AppState;

/// Content types that can be previewed inline.
const PREVIEWABLE_TYPES: &[&str] = &[
    "image/jpeg",
    "image/png",
    "image/gif",
    "image/webp",
    "image/bmp",
    "application/pdf",
    "message/rfc822",
    "text/html",
];

/// Content types explicitly blocked from preview.
const BLOCKED_PREVIEW_TYPES: &[&str] = &["image/svg+xml"];

/// Content-Security-Policy for HTML content.
const HTML_CSP: &str = "default-src 'none'; style-src 'unsafe-inline'; img-src data:; sandbox";
/// Content-Security-Policy for binary content.
const BINARY_CSP: &str = "default-src 'none'; style-src 'unsafe-inline'";

/// Extract a metadata value that may be stored as a string or a single-element array.
fn meta_str(meta: &HashMap<String, serde_json::Value>, key: &str) -> String {
    meta.get(key)
        .and_then(|v| {
            v.as_str().map(String::from).or_else(|| {
                v.as_array()
                    .and_then(|a| a.first())
                    .and_then(|v| v.as_str())
                    .map(String::from)
            })
        })
        .unwrap_or_default()
}

/// Metadata keys excluded from API responses.
const EXCLUDED_META_PREFIXES: &[&str] = &[
    "X-TIKA:",
    "X-Parsed-By",
    "pdf:",
    "access_permission:",
    "dc:",
    "dcterms:",
    "meta:",
    "cp:",
    "extended-properties:",
    "_aum_",
];

/// Build the search router.
pub fn router() -> Router<AppState> {
    Router::new()
        .route("/api/search", get(search))
        .route("/api/documents/{doc_id}", get(get_document))
        .route("/api/documents/{doc_id}/download", get(download_document))
        .route("/api/documents/{doc_id}/preview", get(preview_document))
}

/// Resolve the index list from query params or the server default.
fn resolve_indices(index_param: &str, default: &str) -> Vec<String> {
    let raw = if index_param.is_empty() {
        default
    } else {
        index_param
    };
    raw.split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(String::from)
        .collect()
}

/// Resolve a single index name from query params or the server default.
fn resolve_index<'a>(index_param: &'a str, default: &'a str) -> &'a str {
    if index_param.is_empty() {
        default
    } else {
        index_param
    }
}

/// Check that a user has access to an index. In public mode (user is None), access is allowed.
async fn check_index_access(
    state: &AppState,
    user: Option<&aum_core::auth::User>,
    index: &str,
) -> Result<(), ApiError> {
    if let Some(u) = user
        && !state.auth.check_permission(u, index).await?
    {
        return Err(ApiError::Forbidden(format!(
            "Access denied to index '{index}'"
        )));
    }
    Ok(())
}

/// Parse a sort string like "date:asc" or "size:desc" into a [`SortSpec`].
fn parse_sort(sort: &str) -> Result<SortSpec, ApiError> {
    let (field_name, dir) = sort
        .split_once(':')
        .ok_or_else(|| ApiError::BadRequest(format!("Invalid sort format: '{sort}'")))?;

    let field = match field_name {
        "date" => "meta_created_year".to_owned(),
        "size" => "meta_file_size".to_owned(),
        other => {
            return Err(ApiError::BadRequest(format!(
                "Unknown sort field: '{other}'"
            )));
        }
    };

    let descending = match dir {
        "asc" => false,
        "desc" => true,
        other => {
            return Err(ApiError::BadRequest(format!(
                "Invalid sort direction: '{other}'"
            )));
        }
    };

    Ok(SortSpec { field, descending })
}

/// Filter metadata to exclude internal/hidden keys.
fn clean_metadata(meta: &HashMap<String, serde_json::Value>) -> HashMap<String, serde_json::Value> {
    meta.iter()
        .filter(|(k, _)| {
            !EXCLUDED_META_PREFIXES
                .iter()
                .any(|prefix| k.starts_with(prefix))
        })
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

/// Search documents across indices.
///
/// # Errors
///
/// Returns 400 for invalid query parameters, 401/403 for auth failures, or
/// 500 on search backend errors.
#[utoipa::path(
    get,
    path = "/api/search",
    params(
        ("q" = String, Query, description = "Search query"),
        ("index" = Option<String>, Query, description = "Comma-separated index names"),
        ("type" = Option<String>, Query, description = "Search type: text or hybrid"),
        ("limit" = Option<usize>, Query, description = "Results per page"),
        ("offset" = Option<usize>, Query, description = "Pagination offset"),
        ("filters" = Option<String>, Query, description = "JSON filter map"),
        ("semantic_ratio" = Option<f32>, Query, description = "Hybrid semantic ratio"),
        ("sort" = Option<String>, Query, description = "Sort field:direction"),
    ),
    responses((status = 200, body = SearchResponse)),
)]
pub async fn search(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
    Query(params): Query<SearchParams>,
) -> Result<Json<SearchResponse>, ApiError> {
    if params.q.is_empty() {
        return Err(ApiError::BadRequest(
            "Query parameter 'q' is required".into(),
        ));
    }
    if params.limit == 0 || params.limit > 200 {
        return Err(ApiError::BadRequest(
            "limit must be between 1 and 200".into(),
        ));
    }
    if params.offset > 100_000 {
        return Err(ApiError::BadRequest("offset must be at most 100000".into()));
    }

    let indices = resolve_indices(&params.index, &state.config.server.default_index);
    futures::future::try_join_all(
        indices
            .iter()
            .map(|idx| check_index_access(&state, user.as_ref(), idx)),
    )
    .await?;

    let filters: FilterMap = match &params.filters {
        Some(f) if !f.is_empty() => serde_json::from_str(f)
            .map_err(|_| ApiError::BadRequest("Invalid filters JSON".into()))?,
        _ => FilterMap::new(),
    };

    let sort = params.sort.as_deref().map(parse_sort).transpose()?;
    let include_facets = params.offset == 0 || !filters.is_empty();

    let request = SearchRequest {
        indices: &indices,
        query: &params.q,
        limit: params.limit,
        offset: params.offset,
        filters: &filters,
        sort,
        include_facets,
    };

    // Hoist vector outside the match so it lives long enough for the stream.
    let vector;
    let ratio;
    let stream = match params.search_type {
        SearchType::Text => state.backend.search_text(request),
        SearchType::Hybrid => {
            vector = embed_query(&state, &indices, &params.q).await?;
            ratio = params.semantic_ratio.unwrap_or(0.5);
            state.backend.search_hybrid(request, &vector, ratio)
        }
    };

    let collect_fut = async move {
        let mut results = Vec::new();
        let mut stream = stream;
        while let Some(item) = stream.next().await {
            results.push(item?);
        }
        Ok::<Vec<SearchResult>, ApiError>(results)
    };
    let count_fut = state.backend.count(&indices, Some(&params.q), &filters);

    let (results, count_result) = tokio::join!(collect_fut, count_fut);
    let results = results?;
    let (total, facets) = count_result?;
    let facets = if include_facets {
        Some(simplify_facets(&facets))
    } else {
        None
    };

    info!(
        query = %params.q,
        search_type = %params.search_type,
        index = %params.index,
        results = results.len(),
        total,
        "search completed"
    );

    let response_results: Vec<SearchResultResponse> = results
        .into_iter()
        .map(|r| SearchResultResponse {
            doc_id: r.doc_id,
            display_path: r.display_path,
            display_path_highlighted: r.display_path_highlighted,
            score: r.score,
            snippet: r.snippet,
            metadata: clean_metadata(&r.metadata),
            index: r.index,
        })
        .collect();

    Ok(Json(SearchResponse {
        results: response_results,
        total,
        facets,
    }))
}

/// Sort the values of a facet by count descending, returning just the value strings.
fn sort_facet_values(counts: &HashMap<String, u64>) -> Vec<String> {
    let mut values: Vec<(&str, u64)> = counts.iter().map(|(k, v)| (k.as_str(), *v)).collect();
    values.sort_by(|a, b| b.1.cmp(&a.1));
    values.into_iter().map(|(v, _)| v.to_owned()).collect()
}

/// Convert full facet distribution to a simplified list of values per facet label.
///
/// Returns an `IndexMap` with facets in the canonical display order defined by
/// [`FACET_ORDER`]. Any unknown facets are appended after the known ones.
fn simplify_facets(
    facets: &HashMap<String, HashMap<String, u64>>,
) -> IndexMap<String, Vec<String>> {
    let mut map = IndexMap::with_capacity(facets.len());

    for &label in FACET_ORDER {
        if let Some(counts) = facets.get(label) {
            map.insert(label.to_owned(), sort_facet_values(counts));
        }
    }

    for (label, counts) in facets {
        if !map.contains_key(label.as_str()) {
            map.insert(label.clone(), sort_facet_values(counts));
        }
    }

    map
}

/// Embed a query string using the appropriate model for the given indices.
async fn embed_query(
    state: &AppState,
    indices: &[String],
    query: &str,
) -> Result<Vec<f32>, ApiError> {
    use aum_core::config::EmbeddingsBackend;
    use aum_core::db::IndexEmbeddingRepository as _;
    use aum_core::embeddings::Embedder as _;
    use aum_core::embeddings::{OllamaEmbedder, OpenAiEmbedder};
    use futures::future::join_all;

    if indices.is_empty() {
        return Err(ApiError::BadRequest(
            "No indices provided for embedding lookup.".into(),
        ));
    }

    // Fetch embedding model info for all indices in parallel.
    let lookups: Vec<_> = indices
        .iter()
        .map(|idx| state.embeddings_repo.get_embedding_model(idx))
        .collect();
    let results = join_all(lookups).await;

    let mut model_info: Option<aum_core::models::EmbeddingModelInfo> = None;
    for (idx, result) in indices.iter().zip(results) {
        let info = result
            .map_err(|e| {
                tracing::error!(error = %e, index = %idx, "failed to get embedding model info");
                ApiError::Internal("Failed to check embedding status".into())
            })?
            .ok_or_else(|| {
                ApiError::BadRequest(format!(
                    "No embeddings found for index '{idx}'. Run 'aum embed --index {idx}' first."
                ))
            })?;

        if let Some(ref existing) = model_info {
            if existing.model != info.model || existing.backend != info.backend {
                return Err(ApiError::BadRequest(format!(
                    "Embedding model mismatch: index '{}' uses '{}/{}' but index '{idx}' uses '{}/{}'. \
                     Hybrid search requires all indices to use the same embedding model.",
                    indices[0], existing.backend, existing.model, info.backend, info.model,
                )));
            }
        } else {
            model_info = Some(info);
        }
    }

    let Some(info) = model_info else {
        return Err(ApiError::BadRequest(
            "No indices provided for embedding lookup.".into(),
        ));
    };

    // Build an embedder config from the stored per-index metadata, using the
    // global config only for infrastructure settings (URLs, API keys).
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "dimension and context_length are always small positive values"
    )]
    let query_cfg = aum_core::config::EmbeddingsConfig {
        model: info.model.clone(),
        backend: info.backend.clone(),
        dimension: info.dimension as u32,
        context_length: info.context_length as u32,
        query_prefix: info.query_prefix.clone(),
        ..state.config.embeddings.clone()
    };

    let vector: Vec<f32> = match info.backend {
        EmbeddingsBackend::Ollama => {
            OllamaEmbedder::new(&query_cfg, &query_cfg.ollama_url)
                .embed_query(query)
                .await
        }
        EmbeddingsBackend::OpenAi => {
            OpenAiEmbedder::new(&query_cfg, &query_cfg.api_url)
                .embed_query(query)
                .await
        }
    }
    .map_err(|e| {
        tracing::error!(error = %e, "failed to embed query");
        ApiError::Internal("Failed to embed query".into())
    })?;

    Ok(vector)
}

/// Get full document details by ID.
///
/// # Errors
///
/// Returns 404 if the document is not found, 401/403 for auth failures, or
/// 500 on search backend errors.
#[utoipa::path(
    get,
    path = "/api/documents/{doc_id}",
    params(
        ("doc_id" = String, Path, description = "Document ID"),
        ("index" = Option<String>, Query, description = "Index name"),
    ),
    responses(
        (status = 200, body = DocumentResponse),
        (status = 404, description = "Document not found"),
    ),
)]
pub async fn get_document(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
    AxumPath(doc_id): AxumPath<String>,
    Query(params): Query<DocumentParams>,
) -> Result<Json<DocumentResponse>, ApiError> {
    let idx = resolve_index(&params.index, &state.config.server.default_index);

    check_index_access(&state, user.as_ref(), idx).await?;

    let doc = state
        .backend
        .get_document(idx, &doc_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("Document not found".into()))?;

    // Run attachment, extracted_from, and thread lookups in parallel.
    let attachments_fut = async {
        let mut attachments = Vec::new();
        let mut att_stream = state.backend.find_attachments(idx, &doc.display_path);
        while let Some(item) = att_stream.next().await {
            if let Ok(a) = item {
                attachments.push(AttachmentResponse {
                    doc_id: a.doc_id,
                    display_path: a.display_path,
                });
            }
        }
        attachments
    };

    let extracted_from_fut = async {
        if doc.extracted_from.is_empty() {
            Ok(None)
        } else {
            Ok::<_, ApiError>(
                state
                    .backend
                    .find_by_display_path(idx, &doc.extracted_from)
                    .await?
                    .map(|parent| ExtractedFromResponse {
                        doc_id: parent.doc_id,
                        display_path: parent.display_path,
                    }),
            )
        }
    };

    let thread_fut = build_thread(&state, idx, &doc_id, &doc);

    let (attachments, extracted_from, thread) =
        tokio::join!(attachments_fut, extracted_from_fut, thread_fut);
    let extracted_from = extracted_from?;
    let thread = thread?;

    info!(doc_id = %doc_id, index = idx, "document viewed");

    Ok(Json(DocumentResponse {
        doc_id: doc.doc_id,
        display_path: doc.display_path,
        content: doc.snippet,
        metadata: clean_metadata(&doc.metadata),
        attachments,
        extracted_from,
        thread,
    }))
}

/// Maximum number of messages to include in a thread response.
const MAX_THREAD_SIZE: usize = 100;

/// Build the email thread for a document if it has email Message-ID metadata.
async fn build_thread(
    state: &AppState,
    index: &str,
    doc_id: &str,
    doc: &SearchResult,
) -> Result<Vec<ThreadMessageResponse>, ApiError> {
    // Check for Message-ID in metadata (Meilisearch stores under meta_ prefix stripped).
    let message_id = doc
        .metadata
        .get("message_id")
        .and_then(|v| v.as_str())
        .map(normalize_message_id);

    if message_id.is_none() {
        return Ok(vec![]);
    }

    let in_reply_to = doc
        .metadata
        .get("in_reply_to")
        .and_then(|v| v.as_str())
        .map(normalize_message_id);

    let references: Vec<String> = doc
        .metadata
        .get("references")
        .and_then(|v| {
            if let Some(arr) = v.as_array() {
                Some(
                    arr.iter()
                        .filter_map(|v| v.as_str().map(normalize_message_id))
                        .collect(),
                )
            } else {
                v.as_str().map(|s| {
                    s.split_whitespace()
                        .filter(|r| !r.is_empty())
                        .map(normalize_message_id)
                        .collect()
                })
            }
        })
        .unwrap_or_default();

    let mut thread_stream = state.backend.find_thread(
        index,
        message_id.as_deref(),
        in_reply_to.as_deref(),
        &references,
    );

    let mut thread = Vec::new();
    while let Some(item) = thread_stream.next().await {
        if let Ok(tr) = item {
            if tr.doc_id == doc_id {
                continue;
            }
            if thread.len() >= MAX_THREAD_SIZE {
                break;
            }

            let subject = tr
                .metadata
                .get("email_subject")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_owned();

            let sender = meta_str(&tr.metadata, "email_from");

            let date = ["created"]
                .iter()
                .find_map(|k| tr.metadata.get(*k).and_then(|v| v.as_str()))
                .unwrap_or("")
                .to_owned();

            let snippet: String = tr.snippet.chars().take(200).collect();

            thread.push(ThreadMessageResponse {
                doc_id: tr.doc_id,
                display_path: tr.display_path,
                subject,
                sender,
                date,
                snippet,
            });
        }
    }

    thread.sort_by(|a, b| a.date.cmp(&b.date));

    info!(doc_id, index, thread_size = thread.len(), "thread lookup");
    Ok(thread)
}

/// Fetch a document and resolve its source file on disk.
///
/// Shared preamble for download and preview handlers: resolves the index,
/// checks permissions, fetches the document, and validates the file path.
async fn fetch_document_file(
    state: &AppState,
    user: Option<&aum_core::auth::User>,
    doc_id: &str,
    index_param: &str,
) -> Result<(SearchResult, std::path::PathBuf), ApiError> {
    let idx = resolve_index(index_param, &state.config.server.default_index);
    check_index_access(state, user, idx).await?;
    let doc = state
        .backend
        .get_document(idx, doc_id)
        .await?
        .ok_or_else(|| ApiError::NotFound("Document not found".into()))?;
    let file_path = safe_file_path(&doc.source_path)?;
    Ok((doc, file_path))
}

/// Validate a `source_path`: reject symlinks and ensure it's a regular file.
fn safe_file_path(source_path: &str) -> Result<std::path::PathBuf, ApiError> {
    let path = Path::new(source_path);
    let meta = path
        .symlink_metadata()
        .map_err(|_| ApiError::NotFound("Source file not found on disk".into()))?;
    if meta.is_symlink() || !meta.is_file() {
        return Err(ApiError::NotFound("Source file not found on disk".into()));
    }
    Ok(path.to_path_buf())
}

/// Download the original file for a document.
///
/// # Errors
///
/// Returns 404 if the document or source file is not found, 401/403 for auth
/// failures, or 500 on internal errors.
#[utoipa::path(
    get,
    path = "/api/documents/{doc_id}/download",
    params(
        ("doc_id" = String, Path, description = "Document ID"),
        ("index" = Option<String>, Query, description = "Index name"),
    ),
    responses(
        (status = 200, description = "File download"),
        (status = 404, description = "Document not found"),
    ),
)]
pub async fn download_document(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
    AxumPath(doc_id): AxumPath<String>,
    Query(params): Query<DocumentParams>,
) -> Result<Response, ApiError> {
    let (_doc, file_path) =
        fetch_document_file(&state, user.as_ref(), &doc_id, &params.index).await?;

    let filename: String = file_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download")
        .chars()
        .filter(|c| *c != '"' && *c != '\\' && !c.is_control())
        .collect();

    info!(doc_id = %doc_id, "document download");

    let file = tokio::fs::File::open(&file_path)
        .await
        .map_err(|_| ApiError::NotFound("Source file not found on disk".into()))?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let content_type = mime_guess::from_path(&file_path)
        .first_or_octet_stream()
        .to_string();

    Response::builder()
        .header(header::CONTENT_TYPE, content_type)
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{filename}\""),
        )
        .body(body)
        .map_err(|e| ApiError::Internal(e.to_string()))
}

/// Preview a document inline.
///
/// # Errors
///
/// Returns 404 if the document or file is not found, 415 if the content type
/// is not previewable, 401/403 for auth failures, or 500 on internal errors.
#[utoipa::path(
    get,
    path = "/api/documents/{doc_id}/preview",
    params(
        ("doc_id" = String, Path, description = "Document ID"),
        ("index" = Option<String>, Query, description = "Index name"),
    ),
    responses(
        (status = 200, description = "File preview"),
        (status = 404, description = "Document not found"),
        (status = 415, description = "File type not previewable"),
    ),
)]
pub async fn preview_document(
    State(state): State<AppState>,
    OptionalUser(user): OptionalUser,
    AxumPath(doc_id): AxumPath<String>,
    Query(params): Query<DocumentParams>,
) -> Result<Response, ApiError> {
    let (doc, file_path) =
        fetch_document_file(&state, user.as_ref(), &doc_id, &params.index).await?;

    let raw_ct = meta_str(&doc.metadata, "content_type");
    let content_type = raw_ct.split(';').next().unwrap_or("").trim().to_lowercase();

    if BLOCKED_PREVIEW_TYPES.contains(&content_type.as_str()) {
        return Err(ApiError::Forbidden(
            "Preview of this file type is not permitted".into(),
        ));
    }
    if !PREVIEWABLE_TYPES.contains(&content_type.as_str()) {
        return Err(ApiError::UnsupportedMediaType(
            "File type is not previewable".into(),
        ));
    }

    info!(doc_id = %doc_id, content_type = %content_type, "document preview");

    if content_type == "message/rfc822" {
        let html_bytes = tokio::task::spawn_blocking(move || {
            crate::email_preview::extract_email_html(&file_path)
        })
        .await
        .map_err(|e| ApiError::Internal(e.to_string()))??;
        return Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, "text/html")
            .header(header::CONTENT_DISPOSITION, "inline")
            .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
            .header("Content-Security-Policy", HTML_CSP)
            .body(Body::from(html_bytes))
            .map_err(|e| ApiError::Internal(e.to_string()));
    }

    let file = tokio::fs::File::open(&file_path)
        .await
        .map_err(|_| ApiError::NotFound("Source file not found on disk".into()))?;
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = Body::from_stream(stream);

    let csp = if content_type == "text/html" {
        HTML_CSP
    } else {
        BINARY_CSP
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(
            header::CONTENT_TYPE,
            HeaderValue::from_str(&content_type)
                .unwrap_or_else(|_| HeaderValue::from_static("application/octet-stream")),
        )
        .header(header::CONTENT_DISPOSITION, "inline")
        .header(header::X_CONTENT_TYPE_OPTIONS, "nosniff")
        .header("Content-Security-Policy", csp)
        .body(body)
        .map_err(|e| ApiError::Internal(e.to_string()))
}
