//! `OpenSearch` search backend: [`OpenSearchBackend`] and trait implementations.

mod meta;
mod parse;
mod query;
mod settings;

use std::collections::HashSet;
use std::sync::Arc;

use futures::Future;
use futures::stream::{self, BoxStream, StreamExt as _};
use opensearch::OpenSearch;
use opensearch::http::request::JsonBody;
use opensearch::http::transport::Transport;
use opensearch::indices::{
    IndicesCreateParts, IndicesDeleteParts, IndicesExistsParts, IndicesGetMappingParts,
};
use opensearch::{BulkParts, CountParts, GetParts, MgetParts, SearchParts};
use serde_json::{Value, json};
use tracing::instrument;

use crate::config::OpenSearchConfig;
use crate::extraction::RecordErrorFn;
use crate::ingest::sink::{BatchSink, ExistenceChecker};
use crate::models::Document;
use crate::search::backend::SearchBackend;
use crate::search::types::{
    BatchIndexResult, FacetMap, FilterMap, SearchError, SearchRequest, SearchResult, SortSpec,
};

use meta::build_doc_body;
use parse::{parse_hit, parse_hits};
use query::{
    build_facet_aggs, build_filter_clauses, build_highlight, build_knn_query, build_sort_clause,
    build_text_query, parse_facets,
};
use settings::{META_FIELD_TYPES, build_index_body};

// ---------------------------------------------------------------------------
// Per-query size limits
// ---------------------------------------------------------------------------

const ATTACHMENTS_SEARCH_LIMIT: usize = 200;
const THREAD_SEARCH_LIMIT: usize = 100;

// ---------------------------------------------------------------------------
// Stream helpers
// ---------------------------------------------------------------------------

/// Drive `fut` to completion and fan its `Vec<SearchResult>` out as a stream,
/// propagating any error as a single stream item.
fn results_stream<'a, F>(fut: F) -> BoxStream<'a, Result<SearchResult, SearchError>>
where
    F: Future<Output = Result<Vec<SearchResult>, SearchError>> + Send + 'a,
{
    stream::once(fut)
        .flat_map(|r| match r {
            Ok(hits) => stream::iter(hits.into_iter().map(Ok)).boxed(),
            Err(e) => stream::once(async move { Err(e) }).boxed(),
        })
        .boxed()
}

// ---------------------------------------------------------------------------
// Backend struct
// ---------------------------------------------------------------------------

#[allow(clippy::doc_markdown)]
/// OpenSearch implementation of [`SearchBackend`], [`BatchSink`], and
/// [`ExistenceChecker`].
pub struct OpenSearchBackend {
    client: OpenSearch,
    max_highlight_offset: u64,
}

#[allow(clippy::doc_markdown)]
/// Name of the search pipeline registered on every aum OpenSearch index to
/// perform RRF fusion between the BM25 and k-NN sub-queries of a hybrid query.
const RRF_PIPELINE: &str = "aum-rrf-pipeline";

impl OpenSearchBackend {
    /// Create a new backend from config.
    ///
    /// # Errors
    /// Returns an error if the transport cannot be built (invalid URL).
    pub fn new(config: &OpenSearchConfig) -> Result<Self, SearchError> {
        let transport = Transport::single_node(&config.url).map_err(SearchError::OpenSearch)?;
        let client = OpenSearch::new(transport);
        Ok(Self {
            client,
            max_highlight_offset: config.max_highlight_offset,
        })
    }
}

// ---------------------------------------------------------------------------
// SearchBackend impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl SearchBackend for OpenSearchBackend {
    #[instrument(skip(self), fields(index))]
    async fn initialize(
        &self,
        index: &str,
        vector_dimension: Option<u32>,
    ) -> Result<(), SearchError> {
        initialize_index(
            &self.client,
            index,
            vector_dimension,
            self.max_highlight_offset,
        )
        .await
    }

    #[instrument(skip(self, docs), fields(index, doc_count = docs.len()))]
    async fn index_batch(
        &self,
        index: &str,
        docs: &[(String, Document)],
    ) -> Result<BatchIndexResult, SearchError> {
        if docs.is_empty() {
            return Ok(BatchIndexResult::default());
        }

        let doc_count = docs.len() as u64;

        // Build NDJSON bulk body: alternating action + source lines.
        let mut body: Vec<JsonBody<Value>> = Vec::with_capacity(docs.len() * 2);
        for (doc_id, document) in docs {
            let (id, source) = build_doc_body(doc_id, document);
            body.push(JsonBody::new(json!({ "index": { "_id": id } })));
            body.push(JsonBody::new(source));
        }

        let resp = self
            .client
            .bulk(BulkParts::Index(index))
            .body(body)
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        let resp_body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;

        let (indexed, failed) = parse_bulk_response(&resp_body, doc_count);
        Ok(BatchIndexResult {
            indexed,
            failed,
            truncations: vec![],
        })
    }

    fn search_text<'a>(
        &'a self,
        request: SearchRequest<'a>,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        results_stream(async move {
            execute_text_search(
                &self.client,
                request.indices,
                request.query,
                request.limit,
                request.offset,
                request.filters,
                request.sort.as_ref(),
                request.include_facets,
                self.max_highlight_offset,
            )
            .await
        })
    }

    fn search_hybrid<'a>(
        &'a self,
        request: SearchRequest<'a>,
        vector: &'a [f32],
        // Ignored: RRF fusion is performed by the search pipeline (rank_constant
        // fixed at 60), not by a tunable weighted blend.
        _semantic_ratio: f32,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        results_stream(async move {
            execute_hybrid_search(
                &self.client,
                request.indices,
                request.query,
                vector,
                request.limit,
                request.offset,
                request.filters,
                request.sort.as_ref(),
                request.include_facets,
                self.max_highlight_offset,
            )
            .await
        })
    }

    #[instrument(skip(self, filters), fields(indices = ?indices))]
    async fn count(
        &self,
        indices: &[String],
        query: Option<&str>,
        filters: &FilterMap,
    ) -> Result<(u64, FacetMap), SearchError> {
        let filter_clauses = build_filter_clauses(filters);
        let q = query.unwrap_or("");

        let text_query = build_text_query(q, &filter_clauses);

        let body = json!({
            "query": text_query,
            "size": 0,
            "aggs": build_facet_aggs(),
        });

        let index_names: Vec<&str> = indices.iter().map(String::as_str).collect();
        let resp = self
            .client
            .search(SearchParts::Index(&index_names))
            .body(body)
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        let resp_body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;

        let total = resp_body
            .get("hits")
            .and_then(|h| h.get("total"))
            .and_then(|t| t.get("value"))
            .and_then(Value::as_u64)
            .unwrap_or(0);

        let facets = parse_facets(&resp_body);
        Ok((total, facets))
    }

    #[instrument(skip(self), fields(index, doc_id))]
    async fn get_document(
        &self,
        index: &str,
        doc_id: &str,
    ) -> Result<Option<SearchResult>, SearchError> {
        let resp = self
            .client
            .get(GetParts::IndexId(index, doc_id))
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        if resp.status_code() == 404 {
            return Ok(None);
        }

        let body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;

        // Reframe the get response as a search hit for parse_hit.
        let hit = json!({
            "_id":     body.get("_id"),
            "_index":  body.get("_index"),
            "_score":  1.0,
            "_source": body.get("_source"),
        });

        Ok(parse_hit(&hit, index))
    }

    #[instrument(skip(self), fields(index, display_path))]
    async fn find_by_display_path(
        &self,
        index: &str,
        display_path: &str,
    ) -> Result<Option<SearchResult>, SearchError> {
        let body = json!({
            "query": { "term": { "display_path": display_path } },
            "size": 1,
        });
        let resp = self
            .client
            .search(SearchParts::Index(&[index]))
            .body(body)
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;
        let json: Value = resp.json().await.map_err(SearchError::OpenSearch)?;
        let (results, _) = parse_hits(&json);
        Ok(results.into_iter().next())
    }

    #[instrument(skip(self), fields(index))]
    async fn delete_index(&self, index: &str) -> Result<(), SearchError> {
        let resp = self
            .client
            .indices()
            .delete(IndicesDeleteParts::Index(&[index]))
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        if resp.status_code() == 404 {
            tracing::info!(index, "opensearch index not found, nothing to delete");
            return Ok(());
        }

        resp.error_for_status_code()
            .map_err(SearchError::OpenSearch)?;
        tracing::info!(index, "deleted opensearch index");
        Ok(())
    }

    #[instrument(skip(self), fields(index))]
    async fn doc_count(&self, index: &str) -> Result<u64, SearchError> {
        let resp = self
            .client
            .count(CountParts::Index(&[index]))
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        let body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;

        Ok(body.get("count").and_then(Value::as_u64).unwrap_or(0))
    }

    fn find_attachments<'a>(
        &'a self,
        index: &'a str,
        display_path: &'a str,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        let body = json!({
            "query": { "term": { "extracted_from": display_path } },
            "size": ATTACHMENTS_SEARCH_LIMIT,
        });
        results_stream(async move { search_raw(&self.client, &[index], body).await })
    }

    fn find_thread<'a>(
        &'a self,
        index: &'a str,
        message_id: Option<&'a str>,
        in_reply_to: Option<&'a str>,
        references: &'a [String],
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        let all_ids: Vec<Value> = [message_id, in_reply_to]
            .into_iter()
            .flatten()
            .chain(references.iter().map(String::as_str))
            .map(|s| Value::String(s.to_owned()))
            .collect();

        if all_ids.is_empty() {
            return stream::empty().boxed();
        }

        let body = json!({
            "query": {
                "bool": {
                    "should": [
                        { "terms": { "meta.message_id":  &all_ids } },
                        { "terms": { "meta.in_reply_to": &all_ids } },
                        { "terms": { "meta.references":  &all_ids } },
                    ],
                    "minimum_should_match": 1
                }
            },
            "size": THREAD_SEARCH_LIMIT,
        });

        results_stream(async move { search_raw(&self.client, &[index], body).await })
    }

    #[instrument(skip(self))]
    async fn list_indices(&self) -> Result<Vec<String>, SearchError> {
        let resp = self
            .client
            .indices()
            .get(opensearch::indices::IndicesGetParts::Index(&["*"]))
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        let body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;

        let mut names: Vec<String> = body
            .as_object()
            .map(|obj| {
                obj.keys()
                    .filter(|k| !k.starts_with('.'))
                    .cloned()
                    .collect()
            })
            .unwrap_or_default();
        names.sort();
        Ok(names)
    }

    #[instrument(skip(self), fields(index))]
    async fn count_unembedded(&self, index: &str) -> Result<u64, SearchError> {
        let resp = self
            .client
            .count(CountParts::Index(&[index]))
            .body(json!({ "query": { "term": { "has_embeddings": false } } }))
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        let body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;

        Ok(body.get("count").and_then(Value::as_u64).unwrap_or(0))
    }

    fn scroll_unembedded(
        &self,
        index: &str,
        batch_size: usize,
    ) -> BoxStream<'static, Result<Vec<SearchResult>, SearchError>> {
        let client = self.client.clone();
        let index = index.to_owned();

        // Cursor-based pagination using `search_after` on `_id`.  This is safe
        // to use when backend writes overlap with fetches because the cursor
        // advances past already-seen documents regardless of their
        // `has_embeddings` state.
        let stream = stream::unfold((None::<String>, false), move |(cursor, done)| {
            let client = client.clone();
            let index = index.clone();
            async move {
                if done {
                    return None;
                }
                let result =
                    fetch_unembedded_cursor(&client, &index, batch_size, cursor.as_deref()).await;
                match result {
                    Err(e) => Some((Err(e), (cursor, true))),
                    Ok(hits) => {
                        let exhausted = hits.is_empty();
                        let new_cursor = hits.last().map(|h| h.doc_id.clone());
                        Some((Ok(hits), (new_cursor, exhausted)))
                    }
                }
            }
        });

        stream.boxed()
    }

    #[instrument(skip(self, updates), fields(index, update_count = updates.len()))]
    async fn update_embeddings(
        &self,
        index: &str,
        updates: &[(String, Vec<Vec<f32>>)],
    ) -> Result<u64, SearchError> {
        if updates.is_empty() {
            return Ok(0);
        }

        let mut body: Vec<JsonBody<Value>> = Vec::with_capacity(updates.len() * 2);
        for (doc_id, chunk_vectors) in updates {
            let chunks: Vec<Value> = chunk_vectors
                .iter()
                .map(|vec| json!({ "embedding": vec }))
                .collect();
            body.push(JsonBody::new(json!({ "update": { "_id": doc_id } })));
            body.push(JsonBody::new(
                json!({ "doc": { "chunks": chunks, "has_embeddings": true } }),
            ));
        }

        let resp = self
            .client
            .bulk(BulkParts::Index(index))
            .body(body)
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        let resp_body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;

        let failures = count_bulk_failures(&resp_body);

        Ok(failures)
    }

    fn scroll_documents(
        &self,
        index: &str,
        doc_ids: &[String],
        batch_size: usize,
    ) -> BoxStream<'static, Result<Vec<SearchResult>, SearchError>> {
        if doc_ids.is_empty() {
            return futures::stream::empty().boxed();
        }
        let client = self.client.clone();
        let index = index.to_owned();
        let doc_ids: Arc<[String]> = Arc::from(doc_ids);

        let stream = stream::unfold((0usize, false), move |(offset, done)| {
            let client = client.clone();
            let index = index.clone();
            let doc_ids = Arc::clone(&doc_ids);
            async move {
                if done {
                    return None;
                }
                let end = (offset + batch_size).min(doc_ids.len());
                let page_ids = &doc_ids[offset..end];
                let body = serde_json::json!({
                    "query": { "ids": { "values": page_ids } },
                    "size": batch_size,
                });
                let result = search_raw(&client, &[&index], body).await;
                let exhausted = end >= doc_ids.len();
                match result {
                    Err(e) => Some((Err(e), (offset, true))),
                    Ok(hits) => Some((Ok(hits), (end, exhausted))),
                }
            }
        });

        stream.boxed()
    }

    #[instrument(skip(self, doc_ids), fields(index, id_count = doc_ids.len()))]
    async fn get_existing_doc_ids(
        &self,
        index: &str,
        doc_ids: &[String],
    ) -> Result<HashSet<String>, SearchError> {
        if doc_ids.is_empty() {
            return Ok(HashSet::new());
        }

        let resp = self
            .client
            .mget(MgetParts::Index(index))
            .body(json!({ "ids": doc_ids }))
            .source("false")
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        let body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;

        let found: HashSet<String> = body
            .get("docs")
            .and_then(|v| v.as_array())
            .map(|docs| {
                docs.iter()
                    .filter(|doc| doc.get("found").and_then(Value::as_bool).unwrap_or(false))
                    .filter_map(|doc| doc.get("_id")?.as_str().map(str::to_owned))
                    .collect()
            })
            .unwrap_or_default();

        Ok(found)
    }

    #[instrument(skip(self), fields(index))]
    async fn clear_embeddings(&self, index: &str) -> Result<(), SearchError> {
        let body = json!({
            "query": { "term": { "has_embeddings": true } },
            "script": {
                "source": "ctx._source.has_embeddings = false; ctx._source.remove('chunks')",
                "lang": "painless"
            }
        });
        let resp = self
            .client
            .update_by_query(opensearch::UpdateByQueryParts::Index(&[index]))
            .body(body)
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;

        let status = resp.status_code();
        if !status.is_success() {
            let text = resp.text().await.unwrap_or_else(|_| "unknown".to_owned());
            return Err(SearchError::Internal(format!(
                "clear_embeddings failed: HTTP {status}: {text}"
            )));
        }

        let body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;
        let updated = body.get("updated").and_then(Value::as_u64).unwrap_or(0);
        tracing::info!(index, updated, "cleared embeddings from all documents");
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BatchSink impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl BatchSink for OpenSearchBackend {
    #[instrument(skip(self, batch, record_error), fields(index, batch_len = batch.len()))]
    async fn flush_batch(
        &self,
        index: &str,
        job_id: &str,
        batch: &[(String, Document)],
        record_error: &RecordErrorFn,
    ) -> (u64, u64) {
        match self.index_batch(index, batch).await {
            Ok(result) => (result.indexed, result.failed),
            Err(e) => {
                tracing::error!(job_id, error = %e, "opensearch batch indexing failed");
                for (id, _doc) in batch {
                    record_error(std::path::Path::new(id), "IndexError", &e.to_string());
                }
                (0, batch.len() as u64)
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ExistenceChecker impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl ExistenceChecker for OpenSearchBackend {
    #[instrument(skip(self, doc_ids), fields(index, id_count = doc_ids.len()))]
    async fn get_existing(&self, index: &str, doc_ids: &[String]) -> HashSet<String> {
        self.get_existing_doc_ids(index, doc_ids)
            .await
            .unwrap_or_else(|e| {
                tracing::warn!(
                    error = %e,
                    "failed to check existing doc IDs; assuming none exist"
                );
                HashSet::new()
            })
    }
}

// ---------------------------------------------------------------------------
// Index initialization
// ---------------------------------------------------------------------------

async fn initialize_index(
    client: &OpenSearch,
    name: &str,
    vector_dimension: Option<u32>,
    max_highlight_offset: u64,
) -> Result<(), SearchError> {
    ensure_rrf_pipeline(client).await?;

    // Check whether the index already exists.
    let exists_resp = client
        .indices()
        .exists(IndicesExistsParts::Index(&[name]))
        .send()
        .await
        .map_err(SearchError::OpenSearch)?;

    if exists_resp.status_code().is_success() {
        if mapping_matches(client, name).await {
            tracing::info!(
                index = name,
                "opensearch index already exists with correct mapping"
            );
            return Ok(());
        }
        tracing::warn!(
            index = name,
            "opensearch index has stale mapping, recreating"
        );
        let del = client
            .indices()
            .delete(IndicesDeleteParts::Index(&[name]))
            .send()
            .await
            .map_err(SearchError::OpenSearch)?;
        del.error_for_status_code()
            .map_err(SearchError::OpenSearch)?;
    }

    let body = build_index_body(vector_dimension, max_highlight_offset);
    let resp = client
        .indices()
        .create(IndicesCreateParts::Index(name))
        .body(body)
        .send()
        .await
        .map_err(SearchError::OpenSearch)?;

    resp.error_for_status_code()
        .map_err(SearchError::OpenSearch)?;

    tracing::info!(
        index = name,
        vector = vector_dimension.is_some(),
        "created opensearch index"
    );
    Ok(())
}

#[allow(clippy::doc_markdown)]
/// Register (or overwrite) the cluster-wide search pipeline that applies
/// Reciprocal Rank Fusion to hybrid query results. Idempotent — `PUT` replaces
/// any existing pipeline with the same name.
///
/// This uses OpenSearch 2.19's native RRF normalization technique so that
/// hybrid search works out of the box without client-side fusion or paid
/// features.
async fn ensure_rrf_pipeline(client: &OpenSearch) -> Result<(), SearchError> {
    let body = json!({
        "description": "RRF fusion for aum hybrid search (BM25 + k-NN)",
        "phase_results_processors": [
            {
                "score-ranker-processor": {
                    "combination": {
                        "technique": "rrf"
                    }
                }
            }
        ]
    });

    let path = format!("/_search/pipeline/{RRF_PIPELINE}");
    let resp = client
        .send::<JsonBody<Value>, ()>(
            opensearch::http::Method::Put,
            &path,
            opensearch::http::headers::HeaderMap::new(),
            None,
            Some(JsonBody::new(body)),
            None,
        )
        .await
        .map_err(SearchError::OpenSearch)?;

    resp.error_for_status_code()
        .map_err(SearchError::OpenSearch)?;

    tracing::info!(pipeline = RRF_PIPELINE, "registered RRF search pipeline");
    Ok(())
}

/// Returns `true` if the existing index has the expected `meta.*` field types.
async fn mapping_matches(client: &OpenSearch, name: &str) -> bool {
    let resp = client
        .indices()
        .get_mapping(IndicesGetMappingParts::Index(&[name]))
        .send()
        .await;

    let Ok(resp) = resp else {
        return false;
    };

    let Ok(body) = resp.json::<Value>().await else {
        return false;
    };

    let meta_props = body
        .get(name)
        .and_then(|idx| idx.get("mappings"))
        .and_then(|m| m.get("properties"))
        .and_then(|p| p.get("meta"))
        .and_then(|m| m.get("properties"));

    let Some(meta_props) = meta_props else {
        return false;
    };

    for (field, expected_type) in META_FIELD_TYPES {
        let actual_type = meta_props
            .get(*field)
            .and_then(|f| f.get("type"))
            .and_then(|t| t.as_str());
        if actual_type != Some(expected_type) {
            tracing::warn!(
                field,
                expected = expected_type,
                actual = ?actual_type,
                "meta field type mismatch"
            );
            return false;
        }
    }
    true
}

// ---------------------------------------------------------------------------
// Search execution helpers
// ---------------------------------------------------------------------------

#[allow(clippy::too_many_arguments)]
async fn execute_text_search(
    client: &OpenSearch,
    indices: &[String],
    query: &str,
    limit: usize,
    offset: usize,
    filters: &FilterMap,
    sort: Option<&SortSpec>,
    include_facets: bool,
    max_highlight_offset: u64,
) -> Result<Vec<SearchResult>, SearchError> {
    let filter_clauses = build_filter_clauses(filters);
    let text_query = build_text_query(query, &filter_clauses);

    let mut body = json!({
        "query": text_query,
        "size":  limit,
        "from":  offset,
        "highlight": build_highlight(max_highlight_offset),
    });

    if let Some(clause) = sort.and_then(build_sort_clause) {
        body["sort"] = clause;
    }
    if include_facets {
        body["aggs"] = build_facet_aggs();
    }

    let index_names: Vec<&str> = indices.iter().map(String::as_str).collect();
    let (hits, _total) = send_search(client, &index_names, body).await?;
    Ok(hits)
}

#[allow(clippy::too_many_arguments)]
async fn execute_hybrid_search(
    client: &OpenSearch,
    indices: &[String],
    query: &str,
    vector: &[f32],
    limit: usize,
    offset: usize,
    filters: &FilterMap,
    sort: Option<&SortSpec>,
    include_facets: bool,
    max_highlight_offset: u64,
) -> Result<Vec<SearchResult>, SearchError> {
    let filter_clauses = build_filter_clauses(filters);
    let text_query = build_text_query(query, &filter_clauses);
    let knn_query = build_knn_query(vector, limit, &filter_clauses);
    let highlight = build_highlight(max_highlight_offset);

    let mut body = json!({
        "query": {
            "hybrid": {
                "queries": [text_query, knn_query]
            }
        },
        "size":      limit,
        "from":      offset,
        "highlight": highlight,
    });

    if let Some(clause) = sort.and_then(build_sort_clause) {
        body["sort"] = clause;
    }
    if include_facets {
        body["aggs"] = build_facet_aggs();
    }

    let index_names: Vec<&str> = indices.iter().map(String::as_str).collect();
    let path = format!("/{}/_search", index_names.join(","));
    let query = [("search_pipeline", RRF_PIPELINE)];
    let resp = client
        .send::<JsonBody<Value>, _>(
            opensearch::http::Method::Post,
            &path,
            opensearch::http::headers::HeaderMap::new(),
            Some(&query),
            Some(JsonBody::new(body)),
            None,
        )
        .await
        .map_err(SearchError::OpenSearch)?;

    let body = read_search_body(resp).await?;
    let (hits, _total) = parse_hits(&body);
    Ok(hits)
}

#[allow(clippy::doc_markdown)]
/// Send a raw JSON body to OpenSearch search and parse the response.
async fn send_search(
    client: &OpenSearch,
    indices: &[&str],
    body: Value,
) -> Result<(Vec<SearchResult>, u64), SearchError> {
    let resp = client
        .search(SearchParts::Index(indices))
        .body(body)
        .send()
        .await
        .map_err(SearchError::OpenSearch)?;

    let body = read_search_body(resp).await?;

    Ok(parse_hits(&body))
}

/// Read an `OpenSearch` search response body, surfacing non-2xx statuses as
/// errors instead of silently returning an error document that would parse
/// into zero hits.
async fn read_search_body(
    resp: opensearch::http::response::Response,
) -> Result<Value, SearchError> {
    let status = resp.status_code();
    let body: Value = resp.json().await.map_err(SearchError::OpenSearch)?;
    if !status.is_success() {
        let detail = body
            .get("error")
            .and_then(|e| e.get("reason"))
            .and_then(Value::as_str)
            .map_or_else(|| body.to_string(), str::to_owned);
        return Err(SearchError::Internal(format!(
            "opensearch search failed: HTTP {status}: {detail}"
        )));
    }
    Ok(body)
}

/// Send a raw search body and return `Vec<SearchResult>` (ignoring total).
async fn search_raw(
    client: &OpenSearch,
    indices: &[&str],
    body: Value,
) -> Result<Vec<SearchResult>, SearchError> {
    let (hits, _) = send_search(client, indices, body).await?;
    Ok(hits)
}

// ---------------------------------------------------------------------------
// Unembedded pagination
// ---------------------------------------------------------------------------

/// Fetch a page of unembedded documents using cursor-based `search_after`
/// pagination, sorted by `_id`.
async fn fetch_unembedded_cursor(
    client: &OpenSearch,
    index: &str,
    limit: usize,
    search_after: Option<&str>,
) -> Result<Vec<SearchResult>, SearchError> {
    let mut body = json!({
        "query": { "term": { "has_embeddings": false } },
        "size": limit,
        "sort": [{ "_id": "asc" }],
    });
    if let Some(cursor) = search_after {
        body["search_after"] = json!([cursor]);
    }
    search_raw(client, &[index], body).await
}

// ---------------------------------------------------------------------------
// Bulk response parsing
// ---------------------------------------------------------------------------

#[allow(clippy::doc_markdown)]
/// Count indexed / failed documents from an OpenSearch bulk response.
///
/// Returns `(indexed, failed)`.
fn parse_bulk_response(resp: &Value, total: u64) -> (u64, u64) {
    if !resp.get("errors").and_then(Value::as_bool).unwrap_or(false) {
        return (total, 0);
    }

    let failed = count_bulk_failures(resp);
    (total.saturating_sub(failed), failed)
}

#[allow(clippy::doc_markdown)]
/// Count failed items in an OpenSearch bulk response.
fn count_bulk_failures(resp: &Value) -> u64 {
    resp.get("items")
        .and_then(|v| v.as_array())
        .map_or(0, |items| {
            items
                .iter()
                .filter(|item| {
                    // Each item has exactly one key: "index", "update", "delete", or "create".
                    item.as_object()
                        .and_then(|o| o.values().next())
                        .and_then(|op| op.get("error"))
                        .is_some()
                })
                .count() as u64
        })
}
