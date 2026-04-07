//! Meilisearch search backend: [`MeilisearchBackend`] and trait implementations.

mod batching;
mod filter;
mod meta;
mod parse;
mod settings;

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, LazyLock};

use futures::stream::{self, BoxStream, StreamExt as _};
use meilisearch_sdk::client::Client;
use meilisearch_sdk::search::{SearchQuery, SearchResults, Selectors};
use meilisearch_sdk::settings::Settings;
use serde_json::{Value, json};
use tracing::instrument;

use crate::config::MeilisearchConfig;
use crate::extraction::RecordErrorFn;
use crate::ingest::sink::{BatchSink, ExistenceChecker};
use crate::models::Document;
use crate::search::backend::SearchBackend;
use crate::search::constants::{
    FACET_FIELDS, HIGHLIGHT_POST_TAG, HIGHLIGHT_PRE_TAG, REVERSE_FACET_FIELDS,
};
use crate::search::types::{
    BatchIndexResult, FacetMap, FilterMap, SearchError, SearchRequest, SearchResult, SortSpec,
};

use batching::{MAX_PAYLOAD_BYTES, split_by_payload_size};
use filter::build_filter_string;
use meta::build_doc_body;
use parse::{merge_facets, parse_hit};
use settings::{EMBEDDER_NAME, base_settings, embedder_settings, wait_for_task};

// ---------------------------------------------------------------------------
// Facet field names for search facets parameter
// ---------------------------------------------------------------------------

/// Meilisearch field names for each searchable facet, derived from [`FACET_FIELDS`].
static MEILI_FACET_FIELDS: LazyLock<Vec<&'static str>> =
    LazyLock::new(|| FACET_FIELDS.values().copied().collect());

// ---------------------------------------------------------------------------
// Backend struct
// ---------------------------------------------------------------------------

/// Meilisearch implementation of [`SearchBackend`], [`BatchSink`], and
/// [`ExistenceChecker`].
pub struct MeilisearchBackend {
    client: Client,
    semantic_ratio: f32,
    crop_length: u32,
}

impl MeilisearchBackend {
    /// Create a new backend from config.
    ///
    /// # Errors
    /// Returns an error if the Meilisearch client cannot be built.
    pub fn new(config: &MeilisearchConfig) -> Result<Self, SearchError> {
        let api_key = (!config.api_key.is_empty()).then_some(config.api_key.as_str());
        let client = Client::new(&config.url, api_key).map_err(SearchError::Meilisearch)?;
        Ok(Self {
            client,
            semantic_ratio: config.semantic_ratio,
            crop_length: config.crop_length,
        })
    }

    fn execute_search_stream<'a>(
        &'a self,
        params: SearchExecParams<'a>,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        stream::once(async move { execute_search(&self.client, params).await })
            .flat_map(|r| match r {
                Ok(hits) => stream::iter(hits.into_iter().map(Ok)).boxed(),
                Err(e) => stream::once(async move { Err(e) }).boxed(),
            })
            .boxed()
    }
}

// ---------------------------------------------------------------------------
// SearchBackend impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl SearchBackend for MeilisearchBackend {
    #[instrument(skip(self), fields(index))]
    async fn initialize(
        &self,
        index: &str,
        vector_dimension: Option<u32>,
    ) -> Result<(), SearchError> {
        initialize_index(&self.client, index, vector_dimension).await
    }

    #[instrument(skip(self, docs), fields(index, doc_count = docs.len()))]
    async fn index_batch(
        &self,
        index: &str,
        docs: &[(String, Document)],
    ) -> Result<BatchIndexResult, SearchError> {
        let bodies: Vec<Value> = docs
            .iter()
            .map(|(id, doc)| build_doc_body(id, doc))
            .collect();

        let (sub_batches, truncations) = split_by_payload_size(bodies, MAX_PAYLOAD_BYTES);

        let (indexed, failed) = index_sub_batches(&self.client, index, sub_batches).await?;
        Ok(BatchIndexResult {
            indexed,
            failed,
            truncations,
        })
    }

    fn search_text<'a>(
        &'a self,
        request: SearchRequest<'a>,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        self.execute_search_stream(SearchExecParams {
            indices: request.indices,
            query: request.query,
            vector: None,
            semantic_ratio: self.semantic_ratio,
            limit: request.limit,
            offset: request.offset,
            filters: request.filters,
            sort: request.sort,
            include_facets: request.include_facets,
            crop_length: self.crop_length,
        })
    }

    fn search_hybrid<'a>(
        &'a self,
        request: SearchRequest<'a>,
        vector: &'a [f32],
        semantic_ratio: f32,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        self.execute_search_stream(SearchExecParams {
            indices: request.indices,
            query: request.query,
            vector: Some(vector.to_vec()),
            semantic_ratio,
            limit: request.limit,
            offset: request.offset,
            filters: request.filters,
            sort: request.sort,
            include_facets: request.include_facets,
            crop_length: self.crop_length,
        })
    }

    #[instrument(skip(self, filters), fields(indices = ?indices))]
    async fn count(
        &self,
        indices: &[String],
        query: Option<&str>,
        filters: &FilterMap,
    ) -> Result<(u64, FacetMap), SearchError> {
        let filter = build_filter_string(filters);
        let mut total = 0u64;
        let mut facets = FacetMap::new();

        for name in indices {
            let idx = self.client.index(name);
            let mut q = SearchQuery::new(&idx);
            q.with_query(query.unwrap_or(""))
                .with_limit(0)
                .with_facets(Selectors::Some(&MEILI_FACET_FIELDS));
            if let Some(ref f) = filter {
                q.with_filter(f);
            }
            let resp: SearchResults<Value> = q.execute().await.map_err(SearchError::Meilisearch)?;
            total += resp.estimated_total_hits.unwrap_or(0) as u64;
            if let Some(dist) = resp.facet_distribution {
                facets = merge_facets(facets, convert_facet_distribution(&dist));
            }
        }
        Ok((total, facets))
    }

    #[instrument(skip(self), fields(index, doc_id))]
    async fn get_document(
        &self,
        index: &str,
        doc_id: &str,
    ) -> Result<Option<SearchResult>, SearchError> {
        let idx = self.client.index(index);
        match idx.get_document::<Value>(doc_id).await {
            Ok(doc) => Ok(parse_hit(&doc, index, None, None)),
            Err(meilisearch_sdk::errors::Error::Meilisearch(ref e))
                if e.error_code == meilisearch_sdk::errors::ErrorCode::DocumentNotFound =>
            {
                Ok(None)
            }
            Err(e) => Err(SearchError::Meilisearch(e)),
        }
    }

    #[instrument(skip(self), fields(index, display_path))]
    async fn find_by_display_path(
        &self,
        index: &str,
        display_path: &str,
    ) -> Result<Option<SearchResult>, SearchError> {
        let f = format!(
            "display_path = \"{}\"",
            filter::escape_filter_value(display_path)
        );
        let hits = fetch_filter_page(&self.client, index, &f, 1, 0).await?;
        Ok(hits.into_iter().next())
    }

    #[instrument(skip(self), fields(index))]
    async fn delete_index(&self, index: &str) -> Result<(), SearchError> {
        let task = self
            .client
            .index(index)
            .delete()
            .await
            .map_err(SearchError::Meilisearch)?;
        wait_for_task(task, &self.client).await
    }

    #[instrument(skip(self), fields(index))]
    async fn doc_count(&self, index: &str) -> Result<u64, SearchError> {
        let stats = self
            .client
            .index(index)
            .get_stats()
            .await
            .map_err(SearchError::Meilisearch)?;
        Ok(stats.number_of_documents as u64)
    }

    fn find_attachments<'a>(
        &'a self,
        index: &'a str,
        display_path: &'a str,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        let f = format!(
            "extracted_from = \"{}\"",
            filter::escape_filter_value(display_path)
        );
        filter_stream(&self.client, index, f)
    }

    fn find_thread<'a>(
        &'a self,
        index: &'a str,
        message_id: Option<&'a str>,
        in_reply_to: Option<&'a str>,
        references: &'a [String],
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        match build_thread_filter(message_id, in_reply_to, references) {
            Some(f) => filter_stream(&self.client, index, f),
            None => stream::empty().boxed(),
        }
    }

    #[instrument(skip(self))]
    async fn list_indices(&self) -> Result<Vec<String>, SearchError> {
        let result = self
            .client
            .list_all_indexes()
            .await
            .map_err(SearchError::Meilisearch)?;
        Ok(result.results.into_iter().map(|idx| idx.uid).collect())
    }

    #[instrument(skip(self), fields(index))]
    async fn count_unembedded(&self, index: &str) -> Result<u64, SearchError> {
        count_with_filter(&self.client, index, Some("has_embeddings = false")).await
    }

    fn scroll_unembedded(
        &self,
        index: &str,
        batch_size: usize,
    ) -> BoxStream<'static, Result<Vec<SearchResult>, SearchError>> {
        scroll_cursor(
            &self.client,
            index.to_owned(),
            "has_embeddings = false".to_owned(),
            batch_size,
        )
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
        let doc_ids: Arc<[String]> = doc_ids.to_vec().into();

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
                let ids_list = page_ids
                    .iter()
                    .map(|id| format!("\"{id}\""))
                    .collect::<Vec<_>>()
                    .join(", ");
                let filter = format!("id IN [{ids_list}]");
                let result = fetch_filter_page(&client, &index, &filter, batch_size, 0).await;
                let exhausted = end >= doc_ids.len();
                match result {
                    Err(e) => Some((Err(e), (offset, true))),
                    Ok(hits) => Some((Ok(hits), (end, exhausted))),
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
        let docs: Vec<Value> = updates
            .iter()
            .map(|(id, chunks)| {
                let mut vectors = serde_json::Map::new();
                vectors.insert(EMBEDDER_NAME.to_owned(), json!(chunks));
                json!({
                    "id": id,
                    "has_embeddings": true,
                    "_vectors": Value::Object(vectors),
                })
            })
            .collect();

        let task = self
            .client
            .index(index)
            .add_or_update(&docs, Some("id"))
            .await
            .map_err(SearchError::Meilisearch)?;
        wait_for_task(task, &self.client).await?;

        // Return the number of failed updates. Meilisearch tasks are all-or-nothing:
        // if wait_for_task succeeded above, all documents were updated successfully.
        Ok(0)
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
        let filter = build_id_filter(doc_ids);
        let idx = self.client.index(index);
        let mut q = SearchQuery::new(&idx);
        q.with_filter(&filter).with_limit(doc_ids.len());
        let resp: SearchResults<Value> = q.execute().await.map_err(SearchError::Meilisearch)?;
        let ids = resp
            .hits
            .iter()
            .filter_map(|h| h.result.get("id")?.as_str().map(str::to_owned))
            .collect();
        Ok(ids)
    }

    #[instrument(skip(self), fields(index))]
    async fn clear_embeddings(&self, index: &str) -> Result<(), SearchError> {
        use futures::TryStreamExt as _;

        const BATCH_SIZE: usize = 500;
        let mut stream = scroll_cursor(
            &self.client,
            index.to_owned(),
            "has_embeddings = true".to_owned(),
            BATCH_SIZE,
        );

        let idx = self.client.index(index);
        let mut total_cleared: u64 = 0;

        while let Some(batch) = stream.try_next().await? {
            let docs: Vec<Value> = batch
                .iter()
                .map(|r| {
                    json!({
                        "id": r.doc_id.clone(),
                        "has_embeddings": false,
                        "_vectors": { EMBEDDER_NAME: null },
                    })
                })
                .collect();

            if docs.is_empty() {
                continue;
            }

            let count = docs.len() as u64;
            let task = idx
                .add_or_update(&docs, Some("id"))
                .await
                .map_err(SearchError::Meilisearch)?;
            wait_for_task(task, &self.client).await?;
            total_cleared += count;
        }

        tracing::info!(
            index,
            total_cleared,
            "cleared embeddings from all documents"
        );
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// BatchSink impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl BatchSink for MeilisearchBackend {
    #[instrument(skip(self, batch, record_error), fields(index, batch_len = batch.len()))]
    async fn flush_batch(
        &self,
        index: &str,
        job_id: &str,
        batch: &[(String, Document)],
        record_error: &RecordErrorFn,
    ) -> (u64, u64) {
        match self.index_batch(index, batch).await {
            Ok(result) => {
                for trunc in &result.truncations {
                    tracing::warn!(
                        doc_id = %trunc.doc_id,
                        original_chars = trunc.original_chars,
                        truncated_chars = trunc.truncated_chars,
                        "document content truncated for indexing"
                    );
                    record_error(
                        std::path::Path::new(&trunc.doc_id),
                        "ContentTruncated",
                        &format!(
                            "content truncated from {} to {} chars to fit payload limit",
                            trunc.original_chars, trunc.truncated_chars,
                        ),
                    );
                }
                (result.indexed, result.failed)
            }
            Err(e) => {
                tracing::error!(job_id, error = %e, "batch indexing failed");
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
impl ExistenceChecker for MeilisearchBackend {
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
    client: &Client,
    name: &str,
    vector_dimension: Option<u32>,
) -> Result<(), SearchError> {
    let idx = client.index(name);
    let task = idx
        .set_settings(&base_settings())
        .await
        .map_err(SearchError::Meilisearch)?;
    wait_for_task(task, client).await?;

    if let Some(dim) = vector_dimension {
        let task = idx
            .set_settings(&Settings::new().with_embedders(embedder_settings(dim)))
            .await
            .map_err(SearchError::Meilisearch)?;
        wait_for_task(task, client).await?;
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Batch indexing
// ---------------------------------------------------------------------------

async fn index_sub_batches(
    client: &Client,
    index: &str,
    sub_batches: Vec<Vec<Value>>,
) -> Result<(u64, u64), SearchError> {
    let mut indexed = 0u64;
    let mut failed = 0u64;

    let idx = client.index(index);
    let num_sub_batches = sub_batches.len();
    for (i, batch) in sub_batches.iter().enumerate() {
        tracing::info!(
            index,
            sub_batch = i + 1,
            total_sub_batches = num_sub_batches,
            docs = batch.len(),
            "indexing batch"
        );
        let task = idx
            .add_or_replace(batch, Some("id"))
            .await
            .map_err(SearchError::Meilisearch)?;
        match wait_for_task(task, client).await {
            Ok(()) => indexed += batch.len() as u64,
            Err(SearchError::TaskFailed { .. }) => failed += batch.len() as u64,
            Err(e) => return Err(e),
        }
    }
    Ok((indexed, failed))
}

// ---------------------------------------------------------------------------
// Search execution
// ---------------------------------------------------------------------------

/// Parameters for an `execute_search` call.
struct SearchExecParams<'a> {
    indices: &'a [String],
    query: &'a str,
    vector: Option<Vec<f32>>,
    semantic_ratio: f32,
    limit: usize,
    offset: usize,
    filters: &'a FilterMap,
    sort: Option<SortSpec>,
    include_facets: bool,
    crop_length: u32,
}

/// Parameters for a `search_one_index` call (filter and sort already resolved to strings).
struct IndexSearchParams<'a> {
    query: &'a str,
    vector: Option<&'a [f32]>,
    semantic_ratio: f32,
    limit: usize,
    offset: usize,
    filter: Option<&'a str>,
    sort: Option<&'a str>,
    include_facets: bool,
    crop_length: u32,
}

async fn execute_search(
    client: &Client,
    params: SearchExecParams<'_>,
) -> Result<Vec<SearchResult>, SearchError> {
    let filter = build_filter_string(params.filters);
    let sort_str = params.sort.as_ref().map(build_sort_expr);
    let mut all_results: Vec<SearchResult> = Vec::new();

    for name in params.indices {
        let idx_params = IndexSearchParams {
            query: params.query,
            vector: params.vector.as_deref(),
            semantic_ratio: params.semantic_ratio,
            limit: params.limit,
            offset: params.offset,
            filter: filter.as_deref(),
            sort: sort_str.as_deref(),
            include_facets: params.include_facets,
            crop_length: params.crop_length,
        };
        let hits = search_one_index(client, name, idx_params).await?;
        all_results.extend(hits);
    }

    if params.indices.len() > 1 && sort_str.is_none() {
        all_results.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    Ok(all_results)
}

async fn search_one_index(
    client: &Client,
    name: &str,
    params: IndexSearchParams<'_>,
) -> Result<Vec<SearchResult>, SearchError> {
    // Hoist sort array so that its elements live for the full scope of `q` below.
    let sort_arr: [&str; 1];
    let sort_slice: &[&str] = if let Some(s) = params.sort {
        sort_arr = [s];
        &sort_arr
    } else {
        &[]
    };

    let idx = client.index(name);
    let mut q = SearchQuery::new(&idx);
    q.with_query(params.query)
        .with_limit(params.limit)
        .with_offset(params.offset)
        .with_attributes_to_highlight(Selectors::Some(&["display_path", "content"]))
        .with_highlight_pre_tag(HIGHLIGHT_PRE_TAG)
        .with_highlight_post_tag(HIGHLIGHT_POST_TAG)
        .with_attributes_to_crop(Selectors::Some(&[("content", None)]))
        .with_crop_length(params.crop_length as usize)
        .with_show_ranking_score(true);

    if let Some(f) = params.filter {
        q.with_filter(f);
    }
    if !sort_slice.is_empty() {
        q.with_sort(sort_slice);
    }
    if params.include_facets {
        q.with_facets(Selectors::Some(&MEILI_FACET_FIELDS));
    }
    if let Some(v) = params.vector {
        q.with_vector(v)
            .with_hybrid(EMBEDDER_NAME, params.semantic_ratio);
    }

    let resp: SearchResults<Value> = q.execute().await.map_err(SearchError::Meilisearch)?;
    let hits: Vec<SearchResult> = resp
        .hits
        .iter()
        .filter_map(|h| {
            parse_hit(
                &h.result,
                name,
                h.ranking_score,
                h.formatted_result.as_ref(),
            )
        })
        .collect();
    Ok(hits)
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

fn build_sort_expr(sort: &SortSpec) -> String {
    let dir = if sort.descending { "desc" } else { "asc" };
    format!("{}:{}", sort.field, dir)
}

fn build_id_filter(doc_ids: &[String]) -> String {
    let ids: Vec<String> = doc_ids
        .iter()
        .map(|id| format!("\"{}\"", filter::escape_filter_value(id)))
        .collect();
    format!("id IN [{}]", ids.join(", "))
}

fn build_thread_filter(
    message_id: Option<&str>,
    in_reply_to: Option<&str>,
    references: &[String],
) -> Option<String> {
    let mut parts: Vec<String> = Vec::new();

    if let Some(mid) = message_id {
        parts.push(format!(
            "meta_message_id = \"{}\"",
            filter::escape_filter_value(mid)
        ));
    }
    if let Some(irt) = in_reply_to {
        parts.push(format!(
            "meta_in_reply_to = \"{}\"",
            filter::escape_filter_value(irt)
        ));
    }
    for r in references {
        parts.push(format!(
            "meta_message_id = \"{}\"",
            filter::escape_filter_value(r)
        ));
        parts.push(format!(
            "meta_references = \"{}\"",
            filter::escape_filter_value(r)
        ));
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" OR "))
    }
}

// ---------------------------------------------------------------------------
// Facet conversion
// ---------------------------------------------------------------------------

fn convert_facet_distribution(
    raw: &std::collections::HashMap<String, std::collections::HashMap<String, usize>>,
) -> FacetMap {
    raw.iter()
        .filter_map(|(field_name, counts)| {
            let label = *REVERSE_FACET_FIELDS.get(field_name.as_str())?;
            let dist: HashMap<String, u64> = counts
                .iter()
                .map(|(val, cnt)| (val.clone(), *cnt as u64))
                .collect();
            Some((label.to_owned(), dist))
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Filter-based scroll helpers
// ---------------------------------------------------------------------------

async fn count_with_filter(
    client: &Client,
    index: &str,
    filter: Option<&str>,
) -> Result<u64, SearchError> {
    let idx = client.index(index);
    let mut q = SearchQuery::new(&idx);
    q.with_query("").with_limit(0);
    if let Some(f) = filter {
        q.with_filter(f);
    }
    let resp: SearchResults<Value> = q.execute().await.map_err(SearchError::Meilisearch)?;
    Ok(resp.estimated_total_hits.unwrap_or(0) as u64)
}

fn filter_stream<'a>(
    client: &'a Client,
    index: &'a str,
    filter: String,
) -> BoxStream<'a, Result<SearchResult, SearchError>> {
    const PAGE_SIZE: usize = 100;
    let client = client.clone();
    let index = index.to_owned();

    let stream = stream::unfold((0usize, false), move |(offset, done)| {
        let filter = filter.clone();
        let client = client.clone();
        let index = index.clone();
        async move {
            if done {
                return None;
            }
            let result = fetch_filter_page(&client, &index, &filter, PAGE_SIZE, offset).await;
            match result {
                Err(e) => Some((vec![Err(e)], (offset, true))),
                Ok(hits) => {
                    let exhausted = hits.len() < PAGE_SIZE;
                    let items: Vec<_> = hits.into_iter().map(Ok).collect();
                    Some((items, (offset + PAGE_SIZE, exhausted)))
                }
            }
        }
    })
    .flat_map(stream::iter);

    stream.boxed()
}

/// Cursor-based scroll: sort by `id` ascending and advance past the last-seen
/// doc ID.  Unlike draining pagination, this is safe to use when writes overlap
/// with fetches because the cursor position is independent of the filter result
/// set.
fn scroll_cursor(
    client: &Client,
    index: String,
    base_filter: String,
    batch_size: usize,
) -> BoxStream<'static, Result<Vec<SearchResult>, SearchError>> {
    let client = client.clone();

    let stream = stream::unfold((None::<String>, false), move |(cursor, done)| {
        let base_filter = base_filter.clone();
        let client = client.clone();
        let index = index.clone();
        async move {
            if done {
                return None;
            }
            let result =
                fetch_cursor_page(&client, &index, &base_filter, cursor.as_deref(), batch_size)
                    .await;
            match result {
                Err(e) => Some((Err(e), (cursor, true))),
                Ok(hits) => {
                    let exhausted = hits.is_empty();
                    let new_cursor = hits.last().map(|h| h.doc_id.clone());
                    Some((Ok(hits), (new_cursor.or(cursor), exhausted)))
                }
            }
        }
    });

    stream.boxed()
}

async fn fetch_cursor_page(
    client: &Client,
    index: &str,
    base_filter: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<Vec<SearchResult>, SearchError> {
    let idx = client.index(index);
    let mut q = SearchQuery::new(&idx);

    let filter_owned;
    let filter: &str = if let Some(last_id) = cursor {
        filter_owned = format!("{base_filter} AND id > '{last_id}'");
        &filter_owned
    } else {
        base_filter
    };

    q.with_query("")
        .with_filter(filter)
        .with_sort(&["id:asc"])
        .with_limit(limit)
        .with_offset(0);
    let resp: SearchResults<Value> = q.execute().await.map_err(SearchError::Meilisearch)?;
    Ok(resp
        .hits
        .iter()
        .filter_map(|h| parse_hit(&h.result, index, None, None))
        .collect())
}

async fn fetch_filter_page(
    client: &Client,
    index: &str,
    filter: &str,
    limit: usize,
    offset: usize,
) -> Result<Vec<SearchResult>, SearchError> {
    let idx = client.index(index);
    let mut q = SearchQuery::new(&idx);
    q.with_query("")
        .with_filter(filter)
        .with_limit(limit)
        .with_offset(offset);
    let resp: SearchResults<Value> = q.execute().await.map_err(SearchError::Meilisearch)?;
    Ok(resp
        .hits
        .iter()
        .filter_map(|h| parse_hit(&h.result, index, None, None))
        .collect())
}
