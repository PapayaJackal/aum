//! Runtime search backend dispatch.
//!
//! [`AumBackend`] wraps whichever backend is selected in the config and
//! implements [`SearchBackend`], [`BatchSink`], and [`ExistenceChecker`] by
//! delegating to the underlying implementation.  This lets callers hold a
//! single, concrete value without resorting to multi-trait dynamic dispatch.

use std::collections::HashSet;

use futures::stream::BoxStream;

use crate::config::AumConfig;
use crate::ingest::sink::{BatchSink, ExistenceChecker, RecordErrorFn};
use crate::models::Document;
use crate::search::backend::SearchBackend;
use crate::search::types::{
    BatchIndexResult, FacetMap, FilterMap, SearchError, SearchRequest, SearchResult,
};

// ---------------------------------------------------------------------------
// AumBackend enum
// ---------------------------------------------------------------------------

/// A concrete, owned search backend that can be either Meilisearch or
/// Elasticsearch, selected at runtime based on [`AumConfig::search_backend`].
///
/// Implements [`SearchBackend`], [`BatchSink`], and [`ExistenceChecker`].
pub enum AumBackend {
    /// Meilisearch backend.
    #[cfg(feature = "meilisearch")]
    Meilisearch(super::MeilisearchBackend),
    /// Elasticsearch backend.
    #[cfg(feature = "elasticsearch")]
    Elasticsearch(super::ElasticsearchBackend),
    /// Fallback for builds where no feature was enabled.
    #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
    _None(std::convert::Infallible),
}

// ---------------------------------------------------------------------------
// Constructor
// ---------------------------------------------------------------------------

impl AumBackend {
    /// Create the backend selected by `config.search_backend`.
    ///
    /// # Errors
    ///
    /// Returns an error if the selected backend is not compiled into this
    /// binary, or if the client cannot be constructed (e.g. invalid URL).
    pub fn from_config(config: &AumConfig) -> Result<Self, SearchError> {
        use crate::config::SearchBackendType;

        #[allow(unreachable_patterns, unused_variables)]
        match config.search_backend {
            SearchBackendType::Meilisearch => {
                #[cfg(feature = "meilisearch")]
                return Ok(AumBackend::Meilisearch(super::MeilisearchBackend::new(
                    &config.meilisearch,
                )?));
                #[cfg(not(feature = "meilisearch"))]
                return Err(SearchError::BackendNotCompiled("meilisearch"));
            }
            SearchBackendType::Elasticsearch => {
                #[cfg(feature = "elasticsearch")]
                return Ok(AumBackend::Elasticsearch(super::ElasticsearchBackend::new(
                    &config.elasticsearch,
                )?));
                #[cfg(not(feature = "elasticsearch"))]
                return Err(SearchError::BackendNotCompiled("elasticsearch"));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SearchBackend impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl SearchBackend for AumBackend {
    async fn initialize(
        &self,
        index: &str,
        vector_dimension: Option<u32>,
    ) -> Result<(), SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.initialize(index, vector_dimension).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.initialize(index, vector_dimension).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn index_batch(
        &self,
        index: &str,
        docs: &[(String, Document)],
    ) -> Result<BatchIndexResult, SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.index_batch(index, docs).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.index_batch(index, docs).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    fn search_text<'a>(
        &'a self,
        request: SearchRequest<'a>,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.search_text(request),
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.search_text(request),
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    fn search_hybrid<'a>(
        &'a self,
        request: SearchRequest<'a>,
        vector: &'a [f32],
        semantic_ratio: f32,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.search_hybrid(request, vector, semantic_ratio),
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.search_hybrid(request, vector, semantic_ratio),
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn count(
        &self,
        indices: &[String],
        query: Option<&str>,
        filters: &FilterMap,
    ) -> Result<(u64, FacetMap), SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.count(indices, query, filters).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.count(indices, query, filters).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn get_document(
        &self,
        index: &str,
        doc_id: &str,
    ) -> Result<Option<SearchResult>, SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.get_document(index, doc_id).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.get_document(index, doc_id).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn find_by_display_path(
        &self,
        index: &str,
        display_path: &str,
    ) -> Result<Option<SearchResult>, SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.find_by_display_path(index, display_path).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.find_by_display_path(index, display_path).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn delete_index(&self, index: &str) -> Result<(), SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.delete_index(index).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.delete_index(index).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn doc_count(&self, index: &str) -> Result<u64, SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.doc_count(index).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.doc_count(index).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    fn find_attachments<'a>(
        &'a self,
        index: &'a str,
        display_path: &'a str,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.find_attachments(index, display_path),
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.find_attachments(index, display_path),
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    fn find_thread<'a>(
        &'a self,
        index: &'a str,
        message_id: Option<&'a str>,
        in_reply_to: Option<&'a str>,
        references: &'a [String],
    ) -> BoxStream<'a, Result<SearchResult, SearchError>> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.find_thread(index, message_id, in_reply_to, references),
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => {
                b.find_thread(index, message_id, in_reply_to, references)
            }
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn list_indices(&self) -> Result<Vec<String>, SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.list_indices().await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.list_indices().await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn count_unembedded(&self, index: &str) -> Result<u64, SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.count_unembedded(index).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.count_unembedded(index).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    fn scroll_unembedded(
        &self,
        index: &str,
        batch_size: usize,
    ) -> BoxStream<'static, Result<Vec<SearchResult>, SearchError>> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.scroll_unembedded(index, batch_size),
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.scroll_unembedded(index, batch_size),
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn update_embeddings(
        &self,
        index: &str,
        updates: &[(String, Vec<Vec<f32>>)],
    ) -> Result<u64, SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.update_embeddings(index, updates).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.update_embeddings(index, updates).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    fn scroll_documents(
        &self,
        index: &str,
        doc_ids: &[String],
        batch_size: usize,
    ) -> BoxStream<'static, Result<Vec<SearchResult>, SearchError>> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.scroll_documents(index, doc_ids, batch_size),
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.scroll_documents(index, doc_ids, batch_size),
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn get_existing_doc_ids(
        &self,
        index: &str,
        doc_ids: &[String],
    ) -> Result<HashSet<String>, SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.get_existing_doc_ids(index, doc_ids).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.get_existing_doc_ids(index, doc_ids).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }

    async fn clear_embeddings(&self, index: &str) -> Result<(), SearchError> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.clear_embeddings(index).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.clear_embeddings(index).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }
}

// ---------------------------------------------------------------------------
// BatchSink impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl BatchSink for AumBackend {
    async fn flush_batch(
        &self,
        index: &str,
        job_id: &str,
        batch: &[(String, Document)],
        record_error: &RecordErrorFn,
    ) -> (u64, u64) {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.flush_batch(index, job_id, batch, record_error).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.flush_batch(index, job_id, batch, record_error).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }
}

// ---------------------------------------------------------------------------
// ExistenceChecker impl
// ---------------------------------------------------------------------------

#[async_trait::async_trait]
impl ExistenceChecker for AumBackend {
    async fn get_existing(&self, index: &str, doc_ids: &[String]) -> HashSet<String> {
        match self {
            #[cfg(feature = "meilisearch")]
            AumBackend::Meilisearch(b) => b.get_existing(index, doc_ids).await,
            #[cfg(feature = "elasticsearch")]
            AumBackend::Elasticsearch(b) => b.get_existing(index, doc_ids).await,
            #[cfg(not(any(feature = "meilisearch", feature = "elasticsearch")))]
            AumBackend::_None(n) => match *n {},
        }
    }
}
