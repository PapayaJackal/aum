//! Request and response schemas for the MCP tool surface.
//!
//! These are deliberately separate from the REST DTOs in [`crate::dto`]: agents
//! pay for every token they read, so the shapes here are trimmed (no HTML
//! highlight markup, curated metadata on list results) and carry deep links
//! back into the web UI.

use std::collections::HashMap;

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Numeric schema helpers
// ---------------------------------------------------------------------------

// schemars stamps Rust integer widths with `format: "uint"` / `"uint64"`,
// which are not registered JSON Schema formats. Strict clients log a warning
// for every occurrence each time they load the tool list, so express the same
// constraint with a plain lower bound instead.

/// JSON Schema for a required count: a non-negative integer.
fn count_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({ "type": "integer", "minimum": 0 })
}

/// JSON Schema for an optional count: a non-negative integer or null.
fn optional_count_schema(_: &mut SchemaGenerator) -> Schema {
    json_schema!({ "type": ["integer", "null"], "minimum": 0 })
}

// ---------------------------------------------------------------------------
// Shared parameter fragments
// ---------------------------------------------------------------------------

/// Search type discriminator for the `search_documents` tool.
#[derive(Debug, Clone, Copy, Default, Deserialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum McpSearchType {
    /// Keyword (BM25) search only.
    #[default]
    Text,
    /// Blend of keyword and vector similarity. Requires an embedded index.
    Hybrid,
}

/// Parameters for `search_documents`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct SearchParams {
    /// Search query. Use `*` to match everything (useful with filters).
    pub query: String,
    /// Index names to search. Defaults to the server's default index.
    #[serde(default)]
    pub indices: Option<Vec<String>>,
    /// `text` for keyword search, `hybrid` to blend in vector similarity.
    #[serde(default)]
    pub search_type: McpSearchType,
    /// Maximum results to return (1-50). Defaults to 10.
    #[serde(default)]
    #[schemars(schema_with = "optional_count_schema")]
    pub limit: Option<usize>,
    /// Number of results to skip, for paging through `total`. Defaults to 0.
    #[serde(default)]
    #[schemars(schema_with = "optional_count_schema")]
    pub offset: Option<usize>,
    /// Facet filters as label to accepted values, e.g.
    /// `{"File Type": ["application/pdf"], "Created": ["2021"]}`.
    /// Call `list_facets` to discover valid labels and values.
    #[serde(default)]
    pub filters: Option<HashMap<String, Vec<String>>>,
    /// Weight of vector similarity in hybrid search, 0.0-1.0. Defaults to 0.5.
    #[serde(default)]
    pub semantic_ratio: Option<f32>,
    /// Sort order: `date:asc`, `date:desc`, `size:asc`, or `size:desc`.
    /// Omit to sort by relevance.
    #[serde(default)]
    pub sort: Option<String>,
}

/// Parameters for tools that address a single document.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct DocumentParams {
    /// Document ID, as returned by `search_documents`.
    pub doc_id: String,
    /// Index the document lives in. Defaults to the server's default index.
    #[serde(default)]
    pub index: Option<String>,
}

/// Parameters for `list_facets`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct FacetParams {
    /// Index names to aggregate over. Defaults to the server's default index.
    #[serde(default)]
    pub indices: Option<Vec<String>>,
    /// Restrict the facet counts to documents matching this query.
    /// Defaults to `*` (the whole corpus).
    #[serde(default)]
    pub query: Option<String>,
    /// Already-applied filters, to see how the remaining facets narrow down.
    #[serde(default)]
    pub filters: Option<HashMap<String, Vec<String>>>,
    /// Maximum values to list per facet (1-200). Defaults to 20.
    #[serde(default)]
    #[schemars(schema_with = "optional_count_schema")]
    pub limit: Option<usize>,
}

/// Parameters for `fetch_document_file`.
#[derive(Debug, Deserialize, JsonSchema)]
pub struct FetchFileParams {
    /// Document ID, as returned by `search_documents`.
    pub doc_id: String,
    /// Index the document lives in. Defaults to the server's default index.
    #[serde(default)]
    pub index: Option<String>,
}

// ---------------------------------------------------------------------------
// Responses
// ---------------------------------------------------------------------------

/// A single search hit.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SearchHit {
    /// Document ID. Pass to `get_document` for the full text.
    pub doc_id: String,
    /// Index this hit came from.
    pub index: String,
    /// Human-readable path within the corpus.
    pub display_path: String,
    /// Relevance score; higher is more relevant.
    pub score: f64,
    /// Plain-text excerpt around the match.
    pub snippet: String,
    /// Curated metadata: content type, author, dates, email headers.
    pub metadata: HashMap<String, serde_json::Value>,
    /// Deep link to this document in the aum web UI.
    pub url: String,
}

/// Result of `search_documents`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct SearchToolResult {
    /// Hits for this page, most relevant first.
    pub results: Vec<SearchHit>,
    /// Total number of matching documents across all pages.
    #[schemars(schema_with = "count_schema")]
    pub total: u64,
    /// Offset to pass in the next call, or `null` when this was the last page.
    #[schemars(schema_with = "optional_count_schema")]
    pub next_offset: Option<usize>,
    /// Facet value counts for the matched set, when available.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<Vec<Facet>>,
}

/// A facet and its most common values within the matched set.
#[derive(Debug, Serialize, JsonSchema)]
pub struct Facet {
    /// Facet label, usable as a key in the `filters` parameter.
    pub label: String,
    /// Values ordered by document count, descending.
    pub values: Vec<FacetValue>,
    /// Number of distinct values omitted by the `limit`.
    #[schemars(schema_with = "count_schema")]
    pub truncated: usize,
}

/// One value of a facet, with its document count.
#[derive(Debug, Serialize, JsonSchema)]
pub struct FacetValue {
    /// The value, usable in the `filters` parameter.
    pub value: String,
    /// Number of matching documents carrying this value.
    #[schemars(schema_with = "count_schema")]
    pub count: u64,
}

/// Result of `list_facets`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct FacetToolResult {
    /// Number of documents the counts were computed over.
    #[schemars(schema_with = "count_schema")]
    pub total: u64,
    /// Available facets, in the canonical display order.
    pub facets: Vec<Facet>,
}

/// A reference to another document.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DocumentRef {
    /// Document ID.
    pub doc_id: String,
    /// Human-readable path within the corpus.
    pub display_path: String,
}

/// Result of `get_document`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct DocumentToolResult {
    /// Document ID.
    pub doc_id: String,
    /// Index the document lives in.
    pub index: String,
    /// Human-readable path within the corpus.
    pub display_path: String,
    /// Extracted plain-text content.
    pub content: String,
    /// Whether `content` was cut short; fetch fewer documents at a time or
    /// use `search_documents` snippets to locate the relevant passage.
    pub content_truncated: bool,
    /// Full document metadata.
    pub metadata: HashMap<String, serde_json::Value>,
    /// Files extracted out of this document (email attachments, archive members).
    pub attachments: Vec<DocumentRef>,
    /// The container this document was extracted from, if any.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extracted_from: Option<DocumentRef>,
    /// Number of other messages in this document's email thread.
    /// Call `get_email_thread` to read them.
    #[schemars(schema_with = "count_schema")]
    pub thread_size: usize,
    /// Deep link to this document in the aum web UI.
    pub url: String,
}

/// One message in a reconstructed email thread.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ThreadMessage {
    /// Document ID of the message.
    pub doc_id: String,
    /// Human-readable path within the corpus.
    pub display_path: String,
    /// Subject line.
    pub subject: String,
    /// Sender address.
    pub sender: String,
    /// Send date, as stored in the index.
    pub date: String,
    /// Opening excerpt of the message body.
    pub snippet: String,
    /// Deep link to this message in the aum web UI.
    pub url: String,
}

/// Result of `get_email_thread`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct ThreadToolResult {
    /// Index the thread lives in.
    pub index: String,
    /// Sibling messages, oldest first. Excludes the requested document.
    pub messages: Vec<ThreadMessage>,
}

/// An index the caller may search.
#[derive(Debug, Serialize, JsonSchema)]
pub struct IndexEntry {
    /// Index name, usable in the `indices` parameter of other tools.
    pub name: String,
    /// Whether the index has embeddings, enabling `hybrid` search.
    pub has_embeddings: bool,
    /// Whether this is the index used when none is specified.
    pub is_default: bool,
}

/// Result of `list_indices`.
#[derive(Debug, Serialize, JsonSchema)]
pub struct IndicesToolResult {
    /// Indices the caller has access to.
    pub indices: Vec<IndexEntry>,
}
