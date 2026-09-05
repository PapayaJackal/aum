//! The MCP tool surface: search, document retrieval, facets, and email threads.

use std::collections::HashMap;

use futures::StreamExt as _;
use rmcp::handler::server::wrapper::{Json, Parameters};
use rmcp::model::{CallToolResult, ContentBlock, ErrorData};
use rmcp::{tool, tool_router};
use tracing::info;

use aum_core::auth::User;
use aum_core::search::backend::SearchBackend as _;
use aum_core::search::constants::{
    FACET_ORDER, HIGHLIGHT_POST_TAG, HIGHLIGHT_PRE_TAG, REVERSE_FACET_FIELDS,
};
use aum_core::search::types::{FacetMap, FilterMap, SearchRequest};

use crate::routes::search as rest;

use super::schema::{
    DocumentParams, DocumentRef, DocumentToolResult, Facet, FacetParams, FacetToolResult,
    FacetValue, FetchFileParams, IndexEntry, IndicesToolResult, McpSearchType, SearchHit,
    SearchParams, SearchToolResult, ThreadMessage, ThreadToolResult,
};
use super::{AumMcp, authenticate, to_mcp_error};

/// Upper bound on `limit` for `search_documents`.
const MAX_SEARCH_LIMIT: usize = 50;
/// Default `limit` for `search_documents`, kept small to bound context cost.
const DEFAULT_SEARCH_LIMIT: usize = 10;
/// Upper bound on values returned per facet by `list_facets`.
const MAX_FACET_VALUES: usize = 200;
/// Default values returned per facet by `list_facets`.
const DEFAULT_FACET_VALUES: usize = 20;
/// Maximum characters of extracted text returned by `get_document`.
const MAX_DOCUMENT_CHARS: usize = 100_000;
/// Maximum characters of snippet kept per search hit.
const MAX_SNIPPET_CHARS: usize = 600;
/// Maximum file size returned inline by `fetch_document_file`.
const MAX_INLINE_FILE_BYTES: u64 = 4 * 1024 * 1024;

/// Metadata keys worth spending tokens on in a search hit.
///
/// `get_document` returns everything; list results stay lean.
const HIT_METADATA_KEYS: &[&str] = &[
    "content_type",
    "created",
    "creator",
    "file_size",
    "email_subject",
    "email_from",
    "email_to",
    "email_date",
    "document_type",
];

/// Strip the highlight markup both backends inject around matched terms.
fn strip_highlights(text: &str) -> String {
    text.replace(HIGHLIGHT_PRE_TAG, "")
        .replace(HIGHLIGHT_POST_TAG, "")
}

/// Truncate to `max` characters, reporting whether anything was dropped.
fn truncate_chars(text: &str, max: usize) -> (String, bool) {
    let mut out: String = text.chars().take(max).collect();
    let truncated = out.chars().count() < text.chars().count();
    if truncated {
        out.push('…');
    }
    (out, truncated)
}

/// Keep only the metadata keys worth showing on a list result.
fn hit_metadata(meta: &HashMap<String, serde_json::Value>) -> HashMap<String, serde_json::Value> {
    HIT_METADATA_KEYS
        .iter()
        .filter_map(|k| meta.get(*k).map(|v| ((*k).to_owned(), v.clone())))
        .collect()
}

impl AumMcp {
    /// Resolve the indices to query, defaulting to the server default index,
    /// and verify the caller may read every one of them.
    async fn resolve_and_authorize(
        &self,
        user: Option<&User>,
        indices: Option<&Vec<String>>,
    ) -> Result<Vec<String>, ErrorData> {
        let joined = indices.map(|v| v.join(",")).unwrap_or_default();
        let indices = rest::resolve_indices(&joined, &self.state.config.server.default_index);
        if indices.is_empty() {
            return Err(ErrorData::invalid_params(
                "No index specified and the server has no default index configured.",
                None,
            ));
        }
        futures::future::try_join_all(
            indices
                .iter()
                .map(|idx| rest::check_index_access(&self.state, user, idx)),
        )
        .await
        .map_err(to_mcp_error)?;

        // A misspelled index name otherwise surfaces as an opaque backend
        // failure, which an agent cannot correct. Name the real ones instead.
        let known = self
            .state
            .backend
            .list_indices()
            .await
            .map_err(|e| to_mcp_error(e.into()))?;
        if let Some(missing) = indices.iter().find(|idx| !known.contains(idx)) {
            let available = super::visible_indices(&self.state, user, known).await?;
            let available = if available.is_empty() {
                "none are available to this token".to_owned()
            } else {
                format!("available: {}", available.join(", "))
            };
            return Err(ErrorData::invalid_params(
                format!("No index named '{missing}' ({available})."),
                None,
            ));
        }

        Ok(indices)
    }

    /// Deep link into the web UI for a single document.
    fn document_url(&self, doc_id: &str, index: &str) -> String {
        let base = self.state.config.server.base_url.trim_end_matches('/');
        format!("{base}/#/?doc={doc_id}&docIndex={index}")
    }
}

/// Convert a backend facet distribution into ordered, count-bearing facets.
fn build_facets(facets: &FacetMap, limit: usize) -> Vec<Facet> {
    let mut ordered: Vec<&String> = Vec::with_capacity(facets.len());
    for &label in FACET_ORDER {
        if let Some((key, _)) = facets.get_key_value(label) {
            ordered.push(key);
        }
    }
    for label in facets.keys() {
        if !FACET_ORDER.contains(&label.as_str()) {
            ordered.push(label);
        }
    }

    ordered
        .into_iter()
        .map(|label| {
            let counts = &facets[label];
            let mut values: Vec<(&String, u64)> = counts.iter().map(|(k, v)| (k, *v)).collect();
            values.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(b.0)));
            let truncated = values.len().saturating_sub(limit);
            Facet {
                label: label.clone(),
                values: values
                    .into_iter()
                    .take(limit)
                    .map(|(value, count)| FacetValue {
                        value: value.clone(),
                        count,
                    })
                    .collect(),
                truncated,
            }
        })
        .collect()
}

/// Reject filter keys that are not real facet labels, so a typo surfaces as a
/// clear error instead of silently matching nothing.
fn validate_filters(filters: &FilterMap) -> Result<(), ErrorData> {
    for label in filters.keys() {
        if !FACET_ORDER.contains(&label.as_str())
            && !REVERSE_FACET_FIELDS.values().any(|l| l == label)
        {
            let known = FACET_ORDER.join("\", \"");
            return Err(ErrorData::invalid_params(
                format!("Unknown filter facet \"{label}\". Known facets: \"{known}\"."),
                None,
            ));
        }
    }
    Ok(())
}

#[tool_router(router = tool_router, vis = "pub(super)")]
impl AumMcp {
    /// List the search indices this token can read.
    ///
    /// Call this first: index names feed the `indices` parameter of the other
    /// tools, and `has_embeddings` tells you whether `hybrid` search is
    /// available for a given index.
    #[tool(annotations(title = "List indices", read_only_hint = true))]
    pub async fn list_indices(
        &self,
        extensions: rmcp::model::Extensions,
    ) -> Result<Json<IndicesToolResult>, ErrorData> {
        let user = authenticate(&self.state, &extensions).await?;
        let all = self
            .state
            .backend
            .list_indices()
            .await
            .map_err(|e| to_mcp_error(e.into()))?;

        let visible = super::visible_indices(&self.state, user.as_ref(), all).await?;

        let checks: Vec<_> = visible
            .iter()
            .map(|name| {
                use aum_core::db::IndexEmbeddingRepository as _;
                self.state.embeddings_repo.get_embedding_model(name)
            })
            .collect();
        let results = futures::future::join_all(checks).await;

        let default_index = &self.state.config.server.default_index;
        Ok(Json(IndicesToolResult {
            indices: visible
                .into_iter()
                .zip(results)
                .map(|(name, embedding)| IndexEntry {
                    has_embeddings: embedding.ok().flatten().is_some(),
                    is_default: &name == default_index,
                    name,
                })
                .collect(),
        }))
    }

    /// Search the document corpus and return ranked excerpts.
    ///
    /// Returns short snippets, not full documents: follow up with
    /// `get_document` on the hits that look relevant. Use `text` search for
    /// names, identifiers, and exact phrases; use `hybrid` when you are
    /// searching by meaning and the index has embeddings. Narrow large result
    /// sets with `filters` rather than paging through everything.
    #[tool(annotations(title = "Search documents", read_only_hint = true))]
    pub async fn search_documents(
        &self,
        Parameters(params): Parameters<SearchParams>,
        extensions: rmcp::model::Extensions,
    ) -> Result<Json<SearchToolResult>, ErrorData> {
        let user = authenticate(&self.state, &extensions).await?;

        if params.query.is_empty() {
            return Err(ErrorData::invalid_params(
                "query must not be empty; pass \"*\" to match every document",
                None,
            ));
        }
        let limit = params.limit.unwrap_or(DEFAULT_SEARCH_LIMIT);
        if limit == 0 || limit > MAX_SEARCH_LIMIT {
            return Err(ErrorData::invalid_params(
                format!("limit must be between 1 and {MAX_SEARCH_LIMIT}"),
                None,
            ));
        }
        let offset = params.offset.unwrap_or(0);
        if offset > 100_000 {
            return Err(ErrorData::invalid_params(
                "offset must be at most 100000",
                None,
            ));
        }

        let indices = self
            .resolve_and_authorize(user.as_ref(), params.indices.as_ref())
            .await?;

        let filters: FilterMap = params.filters.unwrap_or_default();
        validate_filters(&filters)?;

        let sort = params
            .sort
            .as_deref()
            .map(rest::parse_sort)
            .transpose()
            .map_err(to_mcp_error)?;

        let request = SearchRequest {
            indices: &indices,
            query: &params.query,
            limit,
            offset,
            filters: &filters,
            sort,
            include_facets: offset == 0,
        };

        // Hoist out of the match so the borrow outlives the stream.
        let vector;
        let ratio;
        let stream = match params.search_type {
            McpSearchType::Text => self.state.backend.search_text(request),
            McpSearchType::Hybrid => {
                vector = rest::embed_query(&self.state, &indices, &params.query)
                    .await
                    .map_err(to_mcp_error)?;
                ratio = params.semantic_ratio.unwrap_or(0.5);
                self.state.backend.search_hybrid(request, &vector, ratio)
            }
        };

        let collect_fut = async move {
            let mut out = Vec::new();
            let mut stream = stream;
            while let Some(item) = stream.next().await {
                out.push(item.map_err(|e| to_mcp_error(e.into()))?);
            }
            Ok::<_, ErrorData>(out)
        };
        let count_fut = self
            .state
            .backend
            .count(&indices, Some(&params.query), &filters);

        let (results, counts) = tokio::join!(collect_fut, count_fut);
        let results = results?;
        let (total, facet_map) = counts.map_err(|e| to_mcp_error(e.into()))?;

        info!(
            query = %params.query,
            results = results.len(),
            total,
            "mcp search completed"
        );

        let hits: Vec<SearchHit> = results
            .into_iter()
            .map(|r| {
                let (snippet, _) = truncate_chars(&strip_highlights(&r.snippet), MAX_SNIPPET_CHARS);
                SearchHit {
                    url: self.document_url(&r.doc_id, &r.index),
                    doc_id: r.doc_id,
                    index: r.index,
                    display_path: r.display_path,
                    score: r.score,
                    snippet,
                    metadata: hit_metadata(&rest::clean_metadata(&r.metadata)),
                }
            })
            .collect();

        let next_offset = {
            let seen = offset.saturating_add(hits.len());
            (!hits.is_empty() && (seen as u64) < total).then_some(seen)
        };

        Ok(Json(SearchToolResult {
            results: hits,
            total,
            next_offset,
            facets: (offset == 0 && !facet_map.is_empty())
                .then(|| build_facets(&facet_map, DEFAULT_FACET_VALUES)),
        }))
    }

    /// Fetch one document's full extracted text, metadata, and relationships.
    ///
    /// Use the `doc_id` from a `search_documents` hit. The response also names
    /// the document's attachments, the container it was extracted from, and
    /// how many sibling messages its email thread holds.
    #[tool(annotations(title = "Get document", read_only_hint = true))]
    pub async fn get_document(
        &self,
        Parameters(params): Parameters<DocumentParams>,
        extensions: rmcp::model::Extensions,
    ) -> Result<Json<DocumentToolResult>, ErrorData> {
        let user = authenticate(&self.state, &extensions).await?;
        let index = params.index.unwrap_or_default();
        let index = rest::resolve_index(&index, &self.state.config.server.default_index);
        rest::check_index_access(&self.state, user.as_ref(), index)
            .await
            .map_err(to_mcp_error)?;

        let doc = self
            .state
            .backend
            .get_document(index, &params.doc_id)
            .await
            .map_err(|e| to_mcp_error(e.into()))?
            .ok_or_else(|| {
                ErrorData::resource_not_found(
                    format!("No document '{}' in index '{index}'", params.doc_id),
                    None,
                )
            })?;

        let attachments_fut = async {
            let mut out = Vec::new();
            let mut stream = self
                .state
                .backend
                .find_attachments(index, &doc.display_path);
            while let Some(item) = stream.next().await {
                if let Ok(a) = item {
                    out.push(DocumentRef {
                        doc_id: a.doc_id,
                        display_path: a.display_path,
                    });
                }
            }
            out
        };

        let extracted_from_fut = async {
            if doc.extracted_from.is_empty() {
                return Ok(None);
            }
            Ok::<_, ErrorData>(
                self.state
                    .backend
                    .find_by_display_path(index, &doc.extracted_from)
                    .await
                    .map_err(|e| to_mcp_error(e.into()))?
                    .map(|parent| DocumentRef {
                        doc_id: parent.doc_id,
                        display_path: parent.display_path,
                    }),
            )
        };

        let thread_fut = rest::build_thread(&self.state, index, &params.doc_id, &doc);

        let (attachments, extracted_from, thread) =
            tokio::join!(attachments_fut, extracted_from_fut, thread_fut);
        let thread = thread.map_err(to_mcp_error)?;

        let (content, content_truncated) =
            truncate_chars(&strip_highlights(&doc.snippet), MAX_DOCUMENT_CHARS);

        info!(doc_id = %params.doc_id, index, "mcp document fetched");

        Ok(Json(DocumentToolResult {
            url: self.document_url(&doc.doc_id, index),
            doc_id: doc.doc_id,
            index: index.to_owned(),
            display_path: doc.display_path,
            content,
            content_truncated,
            metadata: rest::clean_metadata(&doc.metadata),
            attachments,
            extracted_from: extracted_from?,
            thread_size: thread.len(),
        }))
    }

    /// Read the other messages in a document's email thread, oldest first.
    ///
    /// Only meaningful for email documents; anything else returns an empty
    /// list. The requested document itself is excluded — you already have it.
    #[tool(annotations(title = "Get email thread", read_only_hint = true))]
    pub async fn get_email_thread(
        &self,
        Parameters(params): Parameters<DocumentParams>,
        extensions: rmcp::model::Extensions,
    ) -> Result<Json<ThreadToolResult>, ErrorData> {
        let user = authenticate(&self.state, &extensions).await?;
        let index = params.index.unwrap_or_default();
        let index = rest::resolve_index(&index, &self.state.config.server.default_index);
        rest::check_index_access(&self.state, user.as_ref(), index)
            .await
            .map_err(to_mcp_error)?;

        let doc = self
            .state
            .backend
            .get_document(index, &params.doc_id)
            .await
            .map_err(|e| to_mcp_error(e.into()))?
            .ok_or_else(|| {
                ErrorData::resource_not_found(
                    format!("No document '{}' in index '{index}'", params.doc_id),
                    None,
                )
            })?;

        let thread = rest::build_thread(&self.state, index, &params.doc_id, &doc)
            .await
            .map_err(to_mcp_error)?;

        Ok(Json(ThreadToolResult {
            index: index.to_owned(),
            messages: thread
                .into_iter()
                .map(|m| ThreadMessage {
                    url: self.document_url(&m.doc_id, index),
                    doc_id: m.doc_id,
                    display_path: m.display_path,
                    subject: m.subject,
                    sender: m.sender,
                    date: m.date,
                    snippet: strip_highlights(&m.snippet),
                })
                .collect(),
        }))
    }

    /// Discover the facet values available for filtering.
    ///
    /// Returns each facet's most common values with document counts, so you
    /// can pick exact `filters` values for `search_documents` instead of
    /// guessing. Pass a `query` to see how the corpus breaks down within a
    /// result set rather than across the whole index.
    #[tool(annotations(title = "List facets", read_only_hint = true))]
    pub async fn list_facets(
        &self,
        Parameters(params): Parameters<FacetParams>,
        extensions: rmcp::model::Extensions,
    ) -> Result<Json<FacetToolResult>, ErrorData> {
        let user = authenticate(&self.state, &extensions).await?;
        let indices = self
            .resolve_and_authorize(user.as_ref(), params.indices.as_ref())
            .await?;

        let limit = params.limit.unwrap_or(DEFAULT_FACET_VALUES);
        if limit == 0 || limit > MAX_FACET_VALUES {
            return Err(ErrorData::invalid_params(
                format!("limit must be between 1 and {MAX_FACET_VALUES}"),
                None,
            ));
        }

        let filters: FilterMap = params.filters.unwrap_or_default();
        validate_filters(&filters)?;
        let query = params.query.unwrap_or_else(|| "*".to_owned());

        let (total, facet_map) = self
            .state
            .backend
            .count(&indices, Some(&query), &filters)
            .await
            .map_err(|e| to_mcp_error(e.into()))?;

        Ok(Json(FacetToolResult {
            total,
            facets: build_facets(&facet_map, limit),
        }))
    }

    /// Fetch the original file behind a document.
    ///
    /// Images come back inline so you can look at them. Everything else — PDFs,
    /// Office files, archives — is described rather than dumped: use
    /// `get_document` for the extracted text, or the returned URL to download
    /// the raw bytes yourself.
    #[tool(annotations(title = "Fetch document file", read_only_hint = true))]
    pub async fn fetch_document_file(
        &self,
        Parameters(params): Parameters<FetchFileParams>,
        extensions: rmcp::model::Extensions,
    ) -> Result<CallToolResult, ErrorData> {
        use base64ct::{Base64, Encoding as _};

        let user = authenticate(&self.state, &extensions).await?;
        let index = params.index.clone().unwrap_or_default();
        let index = rest::resolve_index(&index, &self.state.config.server.default_index).to_owned();

        let (doc, path) =
            rest::fetch_document_file(&self.state, user.as_ref(), &params.doc_id, &index)
                .await
                .map_err(to_mcp_error)?;

        let raw_ct = rest::meta_str(&doc.metadata, "content_type");
        let content_type = raw_ct.split(';').next().unwrap_or("").trim().to_lowercase();
        let base = self.state.config.server.base_url.trim_end_matches('/');
        let download_url = format!("{base}/api/documents/{}/download?index={index}", doc.doc_id);

        let size = tokio::fs::metadata(&path).await.map_or(0, |m| m.len());

        // Only raster images are worth returning inline; SVG is script-bearing
        // and the rest are better read through their extracted text.
        let inlineable = matches!(
            content_type.as_str(),
            "image/jpeg" | "image/png" | "image/gif" | "image/webp" | "image/bmp"
        );

        if inlineable && size <= MAX_INLINE_FILE_BYTES {
            let bytes = tokio::fs::read(&path).await.map_err(|e| {
                tracing::error!(error = %e, "failed to read source file");
                ErrorData::internal_error("Failed to read source file", None)
            })?;
            info!(doc_id = %doc.doc_id, %content_type, "mcp file fetched inline");
            return Ok(CallToolResult::success(vec![ContentBlock::image(
                Base64::encode_string(&bytes),
                content_type,
            )]));
        }

        let reason = if inlineable {
            format!("the image is {size} bytes, over the {MAX_INLINE_FILE_BYTES} byte inline limit")
        } else {
            format!("'{content_type}' is not an inline-viewable image type")
        };

        Ok(CallToolResult::success(vec![ContentBlock::text(format!(
            "Not returned inline because {reason}.\n\
             Path: {}\nContent type: {content_type}\nSize: {size} bytes\n\
             Download: {download_url}\n\
             For the text of this document, call get_document with doc_id '{}'.",
            doc.display_path, doc.doc_id,
        ))]))
    }
}
