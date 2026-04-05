//! Ollama embedding backend.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;
use tracing::{info, warn};

use crate::config::EmbeddingsConfig;

use super::backend::Embedder;
use super::error::EmbedError;

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

/// Computes embeddings using a locally-running Ollama server.
pub struct OllamaEmbedder {
    model: String,
    base_url: String,
    /// Updated atomically if the backend returns a different dimension.
    dimension: AtomicU32,
    context_length: u32,
    query_prefix: String,
    client: reqwest::Client,
}

// ---------------------------------------------------------------------------
// HTTP payload types
// ---------------------------------------------------------------------------

#[derive(Serialize)]
struct EmbedRequest<'a> {
    model: &'a str,
    input: &'a [String],
    options: EmbedOptions,
    dimensions: u32,
}

#[derive(Serialize)]
struct EmbedOptions {
    num_ctx: u32,
}

#[derive(Deserialize)]
struct EmbedResponse {
    embeddings: Vec<Vec<f32>>,
}

#[derive(Serialize)]
struct PullRequest<'a> {
    name: &'a str,
    stream: bool,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl OllamaEmbedder {
    /// Create a new `OllamaEmbedder` from the embeddings config and a server URL.
    ///
    /// # Panics
    ///
    /// Panics if the HTTP client cannot be constructed (should not happen with
    /// the default TLS configuration).
    #[expect(clippy::expect_used, reason = "default reqwest builder is infallible")]
    pub fn new(config: &EmbeddingsConfig, base_url: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(600))
            .build()
            .expect("reqwest client builder should not fail with these settings");

        let base_url = base_url.trim_end_matches('/').to_owned();
        info!(model = %config.model, base_url = %base_url, num_ctx = config.context_length, "ollama embedder configured");

        Self {
            model: config.model.clone(),
            base_url,
            dimension: AtomicU32::new(config.dimension),
            context_length: config.context_length,
            query_prefix: config.query_prefix.clone(),
            client,
        }
    }

    /// Pull the model on the Ollama server if it is not already available.
    ///
    /// # Errors
    ///
    /// Returns [`EmbedError::PullFailed`] if the HTTP request fails or the
    /// server returns a non-success status.
    pub async fn ensure_model(&self) -> Result<(), EmbedError> {
        info!(model = %self.model, "ensuring ollama model is available");
        let url = format!("{}/api/pull", self.base_url);
        let body = PullRequest {
            name: &self.model,
            stream: false,
        };
        let resp = self.client.post(&url).json(&body).send().await?;
        if !resp.status().is_success() {
            let status = resp.status();
            let text = resp.text().await.unwrap_or_default();
            return Err(EmbedError::PullFailed(format!("HTTP {status}: {text}")));
        }
        info!(model = %self.model, "ollama model ready");
        Ok(())
    }

    async fn embed_raw(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let start = Instant::now();
        let url = format!("{}/api/embed", self.base_url);
        let dimension = self.dimension.load(Ordering::Relaxed);
        let body = EmbedRequest {
            model: &self.model,
            input: texts,
            options: EmbedOptions {
                num_ctx: self.context_length,
            },
            dimensions: dimension,
        };

        let resp = self.client.post(&url).json(&body).send().await?;
        resp.error_for_status_ref()?;
        let data: EmbedResponse = resp.json().await?;

        if data.embeddings.is_empty() {
            return Err(EmbedError::EmptyResponse);
        }

        // Detect and adapt to dimension mismatches.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "embedding dimensions are always small"
        )]
        let actual = data.embeddings[0].len() as u32;
        let expected = self.dimension.load(Ordering::Relaxed);
        if actual != expected {
            warn!(
                expected,
                actual, "ollama embedding dimension mismatch — updating"
            );
            self.dimension.store(actual, Ordering::Relaxed);
        }

        tracing::debug!(
            count = texts.len(),
            elapsed_ms = start.elapsed().as_millis(),
            "ollama embedded batch"
        );
        Ok(data.embeddings)
    }
}

#[async_trait::async_trait]
impl Embedder for OllamaEmbedder {
    fn dimension(&self) -> u32 {
        self.dimension.load(Ordering::Relaxed)
    }

    async fn embed_documents(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.embed_raw(&texts).await
    }

    async fn embed_query(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let prefixed = format!("{}{}", self.query_prefix, text);
        let mut vecs = self.embed_raw(&[prefixed]).await?;
        Ok(vecs.remove(0))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::config::EmbeddingsConfig;

    use super::*;

    fn make_config(base_url: &str) -> EmbeddingsConfig {
        EmbeddingsConfig {
            ollama_url: base_url.to_owned(),
            model: "test-model".to_owned(),
            dimension: 3,
            context_length: 512,
            query_prefix: "query: ".to_owned(),
            ..Default::default()
        }
    }

    fn embed_response(vecs: &[Vec<f32>]) -> serde_json::Value {
        serde_json::json!({ "embeddings": vecs })
    }

    #[tokio::test]
    async fn embed_documents_returns_vectors_as_is() -> anyhow::Result<()> {
        // Ollama's api/embed already returns L2-normalized vectors, so we
        // pass them through without additional normalization.
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(embed_response(&[vec![0.6, 0.8, 0.0]])),
            )
            .mount(&server)
            .await;

        let cfg = make_config(&server.uri());
        let embedder = OllamaEmbedder::new(&cfg, &server.uri());
        let result = embedder.embed_documents(vec!["hello".to_owned()]).await?;
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], vec![0.6, 0.8, 0.0]);
        Ok(())
    }

    #[tokio::test]
    async fn embed_query_prepends_prefix() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(embed_response(&[vec![1.0, 0.0, 0.0]])),
            )
            .mount(&server)
            .await;

        let cfg = make_config(&server.uri());
        let embedder = OllamaEmbedder::new(&cfg, &server.uri());
        let result = embedder.embed_query("search term").await?;
        assert_eq!(result.len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn dimension_mismatch_updates_stored_value() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        // Return 4-dimensional vectors even though config says 3.
        Mock::given(method("POST"))
            .and(path("/api/embed"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(embed_response(&[vec![1.0, 0.0, 0.0, 0.0]])),
            )
            .mount(&server)
            .await;

        let cfg = make_config(&server.uri());
        let embedder = OllamaEmbedder::new(&cfg, &server.uri());
        assert_eq!(embedder.dimension(), 3);
        embedder.embed_documents(vec!["text".to_owned()]).await?;
        assert_eq!(embedder.dimension(), 4);
        Ok(())
    }

    #[tokio::test]
    async fn ensure_model_calls_pull_endpoint() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/pull"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({"status":"success"})),
            )
            .mount(&server)
            .await;

        let cfg = make_config(&server.uri());
        let embedder = OllamaEmbedder::new(&cfg, &server.uri());
        embedder.ensure_model().await?;
        Ok(())
    }

    #[tokio::test]
    async fn ensure_model_returns_error_on_failure() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/api/pull"))
            .respond_with(ResponseTemplate::new(500).set_body_string("internal error"))
            .mount(&server)
            .await;

        let cfg = make_config(&server.uri());
        let embedder = OllamaEmbedder::new(&cfg, &server.uri());
        let result = embedder.ensure_model().await;
        assert!(result.is_err());
        Ok(())
    }
}
