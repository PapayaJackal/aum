//! Error type for the embedding pipeline.

/// Errors that can occur during embedding.
#[derive(Debug, thiserror::Error)]
pub enum EmbedError {
    /// An HTTP request to the embedding backend failed.
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),

    /// The backend returned an empty embeddings list.
    #[error("backend returned empty embeddings")]
    EmptyResponse,

    /// Pulling the Ollama model failed.
    #[error("model pull failed: {0}")]
    PullFailed(String),
}
