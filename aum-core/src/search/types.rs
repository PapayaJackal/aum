//! Core types for the search layer.

use std::collections::HashMap;

// ---------------------------------------------------------------------------
// Search result
// ---------------------------------------------------------------------------

/// A single document returned from a search query.
#[derive(Debug, Clone)]
pub struct SearchResult {
    /// Unique document identifier (hash of source path).
    pub doc_id: String,
    /// Absolute path to the original file on disk.
    pub source_path: String,
    /// Human-readable relative path shown in the UI.
    pub display_path: String,
    /// Display path with search-term highlights (may contain HTML tags).
    pub display_path_highlighted: String,
    /// Relevance score from the search backend (higher is better).
    pub score: f64,
    /// Short excerpt from the document content with highlights applied.
    pub snippet: String,
    /// Display path of the container document this was extracted from, if any.
    pub extracted_from: String,
    /// Curated metadata key→value map (values are strings or string arrays).
    pub metadata: HashMap<String, serde_json::Value>,
    /// Name of the index this result came from.
    pub index: String,
}

// ---------------------------------------------------------------------------
// Batch indexing result
// ---------------------------------------------------------------------------

/// Outcome of a batch index operation.
#[derive(Debug, Default)]
pub struct BatchIndexResult {
    /// Number of documents successfully indexed.
    pub indexed: u64,
    /// Number of documents that failed to index.
    pub failed: u64,
    /// Documents whose `content` field was truncated to fit the payload limit.
    pub truncations: Vec<TruncationRecord>,
}

/// Record of a single document whose content was truncated before indexing.
#[derive(Debug, Clone)]
pub struct TruncationRecord {
    /// Document ID.
    pub doc_id: String,
    /// Original content length in characters before truncation.
    pub original_chars: usize,
    /// Truncated content length in characters after truncation.
    pub truncated_chars: usize,
}

// ---------------------------------------------------------------------------
// Search request
// ---------------------------------------------------------------------------

/// Common parameters for a keyword or hybrid search query.
///
/// Pass this to [`SearchBackend::search_text`] or [`SearchBackend::search_hybrid`].
pub struct SearchRequest<'a> {
    /// Indices (datasets) to search. Must be non-empty.
    pub indices: &'a [String],
    /// Query string for keyword matching.
    pub query: &'a str,
    /// Maximum number of results to return.
    pub limit: usize,
    /// Number of results to skip (for pagination).
    pub offset: usize,
    /// Active facet filters to apply.
    pub filters: &'a FilterMap,
    /// Optional sort field and direction.
    pub sort: Option<SortSpec>,
    /// Whether to include facet distribution counts in the response.
    pub include_facets: bool,
}

// ---------------------------------------------------------------------------
// Sort specification
// ---------------------------------------------------------------------------

/// A single-field sort specification for search queries.
#[derive(Debug, Clone)]
pub struct SortSpec {
    /// Indexed field name to sort on (e.g. `"meta_created_year"`).
    pub field: String,
    /// Sort direction: `true` = descending, `false` = ascending.
    pub descending: bool,
}

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// Facet filter map: display label → list of accepted values.
///
/// Keys are human-readable facet labels (e.g. `"File Type"`).
/// Values are the accepted values for that facet.
pub type FilterMap = HashMap<String, Vec<String>>;

/// Facet distribution: facet label → value → document count.
pub type FacetMap = HashMap<String, HashMap<String, u64>>;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors originating from the search backend.
#[derive(Debug, thiserror::Error)]
pub enum SearchError {
    /// The Meilisearch SDK returned an error.
    #[cfg(feature = "meilisearch")]
    #[error("meilisearch error: {0}")]
    Meilisearch(#[from] meilisearch_sdk::errors::Error),
    /// The Elasticsearch client returned an error.
    #[cfg(feature = "elasticsearch")]
    #[error("elasticsearch error: {0}")]
    Elasticsearch(elasticsearch::Error),
    /// JSON serialisation or deserialisation failed.
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    /// The requested backend was not compiled into this binary.
    #[error("backend '{0}' is not compiled into this binary; rebuild with --features {0}")]
    BackendNotCompiled(&'static str),
    /// A Meilisearch task did not complete within the allowed timeout.
    #[cfg(feature = "meilisearch")]
    #[error("task timed out")]
    TaskTimeout,
    /// A Meilisearch task completed with a failure status.
    #[cfg(feature = "meilisearch")]
    #[error("task failed: {error}")]
    TaskFailed {
        /// Error message from the failed task.
        error: String,
    },
}
