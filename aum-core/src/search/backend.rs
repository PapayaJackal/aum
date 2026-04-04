//! Abstract search backend trait.

use std::collections::HashSet;

use futures::stream::BoxStream;

use crate::models::Document;
use crate::search::types::{
    BatchIndexResult, FacetMap, FilterMap, SearchError, SearchRequest, SearchResult,
};

// ---------------------------------------------------------------------------
// SearchBackend trait
// ---------------------------------------------------------------------------

/// Defines the interface that every search backend must implement.
///
/// Implementations wrap a specific search engine (Meilisearch, Elasticsearch,
/// etc.) and translate the generic operations below into engine-specific calls.
#[async_trait::async_trait]
pub trait SearchBackend: Send + Sync {
    /// Create or update the search index with the required field settings.
    ///
    /// Must be called before indexing any documents. Pass `vector_dimension`
    /// when enabling hybrid/semantic search.
    async fn initialize(
        &self,
        index: &str,
        vector_dimension: Option<u32>,
    ) -> Result<(), SearchError>;

    /// Index a batch of `(doc_id, Document)` pairs into the given index.
    ///
    /// Documents are split into sub-batches if needed to respect the backend's
    /// payload limits. Returns a [`BatchIndexResult`] describing successes,
    /// failures, and any content truncations.
    async fn index_batch(
        &self,
        index: &str,
        docs: &[(String, Document)],
    ) -> Result<BatchIndexResult, SearchError>;

    /// Full-text keyword search across the indices in `request.indices`.
    fn search_text<'a>(
        &'a self,
        request: SearchRequest<'a>,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>>;

    /// Hybrid keyword + vector search across the indices in `request.indices`.
    fn search_hybrid<'a>(
        &'a self,
        request: SearchRequest<'a>,
        vector: &'a [f32],
        semantic_ratio: f32,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>>;

    /// Return the total hit count and facet distributions for a query without
    /// fetching document bodies.
    async fn count(
        &self,
        indices: &[String],
        query: Option<&str>,
        filters: &FilterMap,
    ) -> Result<(u64, FacetMap), SearchError>;

    /// Fetch a single document by its ID from the given index.
    async fn get_document(
        &self,
        index: &str,
        doc_id: &str,
    ) -> Result<Option<SearchResult>, SearchError>;

    /// Delete the given index.
    async fn delete_index(&self, index: &str) -> Result<(), SearchError>;

    /// Return the total number of indexed documents in the given index.
    async fn doc_count(&self, index: &str) -> Result<u64, SearchError>;

    /// Find all documents extracted from the given container display path.
    fn find_attachments<'a>(
        &'a self,
        index: &'a str,
        display_path: &'a str,
    ) -> BoxStream<'a, Result<SearchResult, SearchError>>;

    /// Find all documents belonging to the same email thread.
    fn find_thread<'a>(
        &'a self,
        index: &'a str,
        message_id: Option<&'a str>,
        in_reply_to: Option<&'a str>,
        references: &'a [String],
    ) -> BoxStream<'a, Result<SearchResult, SearchError>>;

    /// List the names of all available indices on the search backend.
    async fn list_indices(&self) -> Result<Vec<String>, SearchError>;

    /// Count documents that do not yet have embedding vectors.
    async fn count_unembedded(&self, index: &str) -> Result<u64, SearchError>;

    /// Stream batches of documents that do not yet have embedding vectors.
    ///
    /// The returned stream is `'static` — implementations must clone/own all
    /// state so the stream can be sent to a spawned task.
    fn scroll_unembedded(
        &self,
        index: &str,
        batch_size: usize,
    ) -> BoxStream<'static, Result<Vec<SearchResult>, SearchError>>;

    /// Bulk-update embedding vectors for a set of document IDs.
    ///
    /// `updates` is a list of `(doc_id, chunks)` pairs where each chunk is a
    /// flat embedding vector.
    ///
    /// Returns the number of documents that **failed** to update.
    async fn update_embeddings(
        &self,
        index: &str,
        updates: &[(String, Vec<Vec<f32>>)],
    ) -> Result<u64, SearchError>;

    /// Stream batches of documents by their IDs.
    ///
    /// Used by embed retry to re-embed specific documents that failed previously.
    /// Each returned batch contains up to `batch_size` results with their `snippet`
    /// field populated with the full document content.
    /// The returned stream is `'static` — implementations must clone/own all
    /// state so the stream can be sent to a spawned task.
    fn scroll_documents(
        &self,
        index: &str,
        doc_ids: &[String],
        batch_size: usize,
    ) -> BoxStream<'static, Result<Vec<SearchResult>, SearchError>>;

    /// Return the subset of `doc_ids` that already exist in the given index.
    ///
    /// Used by resume mode to avoid re-indexing already-processed files.
    async fn get_existing_doc_ids(
        &self,
        index: &str,
        doc_ids: &[String],
    ) -> Result<HashSet<String>, SearchError>;
}
