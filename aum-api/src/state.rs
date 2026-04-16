//! Shared application state for the Axum web server.

use std::sync::Arc;

use aum_core::auth::AuthService;
use aum_core::config::AumConfig;
use aum_core::db::SqlxIndexEmbeddingRepository;
use aum_core::search::AumBackend;

/// Shared state available to all request handlers via `State<AppState>`.
#[derive(Clone)]
pub struct AppState {
    /// Resolved server configuration.
    pub config: Arc<AumConfig>,
    /// Authentication and authorization service.
    pub auth: AuthService,
    #[allow(clippy::doc_markdown)]
    /// Search backend (Meilisearch or OpenSearch).
    pub backend: Arc<AumBackend>,
    /// Embedding model metadata repository.
    pub embeddings_repo: Arc<SqlxIndexEmbeddingRepository>,
}
