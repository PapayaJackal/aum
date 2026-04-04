//! Request and response types for the API.

use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

// ---------------------------------------------------------------------------
// Search
// ---------------------------------------------------------------------------

/// A single search result.
#[derive(Serialize, ToSchema)]
pub struct SearchResultResponse {
    /// Document ID.
    pub doc_id: String,
    /// Human-readable display path.
    pub display_path: String,
    /// Display path with highlighted search terms.
    pub display_path_highlighted: String,
    /// Relevance score.
    pub score: f64,
    /// Highlighted text snippet.
    pub snippet: String,
    /// Document metadata.
    pub metadata: HashMap<String, serde_json::Value>,
    /// Index this result came from.
    pub index: String,
}

/// Paginated search response.
#[derive(Serialize, ToSchema)]
pub struct SearchResponse {
    /// Search results for the current page.
    pub results: Vec<SearchResultResponse>,
    /// Total number of matching documents.
    pub total: u64,
    /// Facet distributions (only on first page or when filters are applied).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub facets: Option<HashMap<String, Vec<String>>>,
}

/// Search type discriminator.
#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum SearchType {
    /// Full-text keyword search.
    #[default]
    Text,
    /// Hybrid semantic + keyword search.
    Hybrid,
}

impl std::fmt::Display for SearchType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Text => f.write_str("text"),
            Self::Hybrid => f.write_str("hybrid"),
        }
    }
}

/// Search query parameters.
#[derive(Deserialize)]
pub struct SearchParams {
    /// Search query string (required, min 1 character).
    pub q: String,
    /// Comma-separated index names (defaults to server default).
    #[serde(default)]
    pub index: String,
    /// Search type: "text" or "hybrid".
    #[serde(default, rename = "type")]
    pub search_type: SearchType,
    /// Results per page (1–200, default 20).
    #[serde(default = "default_limit")]
    pub limit: usize,
    /// Pagination offset (0–100000, default 0).
    #[serde(default)]
    pub offset: usize,
    /// JSON-encoded filter map.
    #[serde(default)]
    pub filters: Option<String>,
    /// Semantic ratio for hybrid search (0.0–1.0).
    #[serde(default)]
    pub semantic_ratio: Option<f32>,
    /// Sort field and direction (e.g. "date:asc", "size:desc").
    #[serde(default)]
    pub sort: Option<String>,
}

const fn default_limit() -> usize {
    20
}

// ---------------------------------------------------------------------------
// Documents
// ---------------------------------------------------------------------------

/// An attachment reference.
#[derive(Serialize, ToSchema)]
pub struct AttachmentResponse {
    /// Document ID of the attachment.
    pub doc_id: String,
    /// Display path of the attachment.
    pub display_path: String,
}

/// Reference to the container document this was extracted from.
#[derive(Serialize, ToSchema)]
pub struct ExtractedFromResponse {
    /// Document ID of the parent.
    pub doc_id: String,
    /// Display path of the parent.
    pub display_path: String,
}

/// A message in an email thread.
#[derive(Serialize, ToSchema)]
pub struct ThreadMessageResponse {
    /// Document ID of the thread message.
    pub doc_id: String,
    /// Display path.
    pub display_path: String,
    /// Email subject.
    pub subject: String,
    /// Email sender.
    pub sender: String,
    /// Date string.
    pub date: String,
    /// Short text excerpt.
    pub snippet: String,
}

/// Full document detail response.
#[derive(Serialize, ToSchema)]
pub struct DocumentResponse {
    /// Document ID.
    pub doc_id: String,
    /// Display path.
    pub display_path: String,
    /// Full document text content.
    pub content: String,
    /// Document metadata.
    pub metadata: HashMap<String, serde_json::Value>,
    /// Documents extracted from this container (e.g. files within an archive).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub attachments: Vec<AttachmentResponse>,
    /// Parent container this document was extracted from.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extracted_from: Option<ExtractedFromResponse>,
    /// Other messages in the same email thread.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub thread: Vec<ThreadMessageResponse>,
}

/// Document query parameters (shared by detail, download, and preview).
#[derive(Deserialize)]
pub struct DocumentParams {
    /// Index name (defaults to server default).
    #[serde(default)]
    pub index: String,
}

// ---------------------------------------------------------------------------
// Indices
// ---------------------------------------------------------------------------

/// Information about a single search index.
#[derive(Serialize, ToSchema)]
pub struct IndexInfo {
    /// Index name.
    pub name: String,
    /// Whether embeddings have been generated for this index.
    pub has_embeddings: bool,
}

/// Response listing all available indices.
#[derive(Serialize, ToSchema)]
pub struct IndicesResponse {
    /// Available search indices.
    pub indices: Vec<IndexInfo>,
}

// ---------------------------------------------------------------------------
// Auth
// ---------------------------------------------------------------------------

/// Login request body.
#[derive(Deserialize, ToSchema)]
pub struct LoginRequest {
    /// Username.
    pub username: String,
    /// Password.
    pub password: String,
}

/// Session token response (returned after successful login or invitation redemption).
#[derive(Serialize, ToSchema)]
pub struct SessionTokenResponse {
    /// Opaque session token.
    pub session_token: String,
    /// Token type (always "bearer").
    pub token_type: String,
}

/// Invitation validation response.
#[derive(Serialize, ToSchema)]
pub struct InviteValidationResponse {
    /// Username assigned to this invitation.
    pub username: String,
    /// Whether the invitation is valid.
    pub valid: bool,
}

/// Request body for redeeming an invitation.
#[derive(Deserialize, ToSchema)]
pub struct RedeemInviteRequest {
    /// Password for the new account.
    pub password: String,
}

/// Available auth providers and mode information.
#[derive(Serialize, ToSchema)]
pub struct ProvidersResponse {
    /// List of available OAuth provider names (empty for now).
    pub providers: Vec<String>,
    /// Whether anonymous access is allowed.
    pub public_mode: bool,
}
