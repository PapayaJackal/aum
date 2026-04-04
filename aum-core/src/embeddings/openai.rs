//! OpenAI-compatible embedding backend.
//!
//! Works with `OpenAI`, Azure `OpenAI`, and any provider implementing the
//! `POST /v1/embeddings` endpoint (e.g. `vLLM`, `LiteLLM`, Together).

use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::config::EmbeddingsConfig;

use super::backend::{Embedder, l2_normalize_batch};
use super::error::EmbedError;

// ---------------------------------------------------------------------------
// Structs
// ---------------------------------------------------------------------------

/// Computes embeddings using an OpenAI-compatible REST API.
pub struct OpenAiEmbedder {
    model: String,
    api_url: String,
    /// Updated atomically if the backend returns a different dimension.
    dimension: AtomicU32,
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
}

#[derive(Deserialize)]
struct EmbedResponse {
    data: Vec<EmbedItem>,
}

#[derive(Deserialize)]
struct EmbedItem {
    index: usize,
    embedding: Vec<f32>,
}

// ---------------------------------------------------------------------------
// Implementation
// ---------------------------------------------------------------------------

impl OpenAiEmbedder {
    /// Create a new `OpenAiEmbedder` from the embeddings config and a server URL.
    ///
    /// # Panics
    ///
    /// Panics if the API key contains non-ASCII characters that cannot be used
    /// in an HTTP header, or if the HTTP client cannot be constructed.
    #[expect(
        clippy::expect_used,
        reason = "default reqwest builder and ASCII header values are infallible"
    )]
    pub fn new(config: &EmbeddingsConfig, api_url: &str) -> Self {
        let mut headers = reqwest::header::HeaderMap::new();
        if !config.api_key.is_empty() {
            let value = format!("Bearer {}", config.api_key)
                .parse()
                .expect("API key should be a valid header value");
            headers.insert(reqwest::header::AUTHORIZATION, value);
        }

        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(120))
            .default_headers(headers)
            .build()
            .expect("reqwest client builder should not fail with these settings");

        let api_url = api_url.trim_end_matches('/').to_owned();
        info!(model = %config.model, api_url = %api_url, "openai embedder configured");

        Self {
            model: config.model.clone(),
            api_url,
            dimension: AtomicU32::new(config.dimension),
            query_prefix: config.query_prefix.clone(),
            client,
        }
    }

    async fn embed_raw(&self, texts: &[String]) -> Result<Vec<Vec<f32>>, EmbedError> {
        let start = Instant::now();
        metrics::counter!("aum_embedding_requests_total", "backend" => "openai").increment(1);

        let url = format!("{}/v1/embeddings", self.api_url);
        let body = EmbedRequest {
            model: &self.model,
            input: texts,
        };
        let resp = self.client.post(&url).json(&body).send().await?;
        resp.error_for_status_ref()?;
        let mut data: EmbedResponse = resp.json().await?;

        metrics::histogram!("aum_embedding_duration_seconds", "backend" => "openai")
            .record(start.elapsed().as_secs_f64());

        if data.data.is_empty() {
            return Err(EmbedError::EmptyResponse);
        }

        // Sort by index to guarantee the order matches the input.
        data.data.sort_unstable_by_key(|item| item.index);
        let mut embeddings: Vec<Vec<f32>> =
            data.data.into_iter().map(|item| item.embedding).collect();

        // Detect and adapt to dimension mismatches.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "embedding dimensions are always small"
        )]
        let actual = embeddings[0].len() as u32;
        let expected = self.dimension.load(Ordering::Relaxed);
        if actual != expected {
            warn!(
                expected,
                actual, "openai embedding dimension mismatch — updating"
            );
            self.dimension.store(actual, Ordering::Relaxed);
        }

        tracing::debug!(
            count = texts.len(),
            elapsed_ms = start.elapsed().as_millis(),
            "openai embedded batch"
        );

        // Normalise in-place before returning.
        l2_normalize_batch(&mut embeddings);
        Ok(embeddings)
    }
}

#[async_trait::async_trait]
impl Embedder for OpenAiEmbedder {
    fn dimension(&self) -> u32 {
        self.dimension.load(Ordering::Relaxed)
    }

    async fn embed_documents(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, EmbedError> {
        self.embed_raw(&texts).await
    }

    async fn embed_query(&self, text: &str) -> Result<Vec<f32>, EmbedError> {
        let prefixed = format!("{}{}", self.query_prefix, text);
        let mut vecs = self.embed_raw(&[prefixed]).await?;
        // `embed_raw` already L2-normalises the batch; no second pass needed.
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

    fn make_config(api_url: &str) -> EmbeddingsConfig {
        EmbeddingsConfig {
            api_url: api_url.to_owned(),
            model: "text-embedding-3-small".to_owned(),
            dimension: 3,
            query_prefix: "query: ".to_owned(),
            ..Default::default()
        }
    }

    fn embed_response(vecs: &[Vec<f32>]) -> serde_json::Value {
        let data: Vec<serde_json::Value> = vecs
            .iter()
            .enumerate()
            .map(|(i, v)| serde_json::json!({"index": i, "embedding": v}))
            .collect();
        serde_json::json!({"data": data, "model": "test", "usage": {}})
    }

    #[tokio::test]
    async fn embed_documents_returns_normalised_vectors() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/embeddings"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(embed_response(&[vec![3.0, 4.0, 0.0]])),
            )
            .mount(&server)
            .await;

        let cfg = make_config(&server.uri());
        let embedder = OpenAiEmbedder::new(&cfg, &server.uri());
        let result = embedder.embed_documents(vec!["hello".to_owned()]).await?;
        assert_eq!(result.len(), 1);
        let norm: f32 = result[0].iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
        Ok(())
    }

    #[tokio::test]
    async fn items_sorted_by_index() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        // Return items in reverse order.
        let body = serde_json::json!({
            "data": [
                {"index": 1, "embedding": [0.0, 1.0, 0.0]},
                {"index": 0, "embedding": [1.0, 0.0, 0.0]},
            ]
        });
        Mock::given(method("POST"))
            .and(path("/v1/embeddings"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&server)
            .await;

        let cfg = make_config(&server.uri());
        let embedder = OpenAiEmbedder::new(&cfg, &server.uri());
        let texts = vec!["first".to_owned(), "second".to_owned()];
        let result = embedder.embed_documents(texts).await?;
        // index 0 should be first
        assert!((result[0][0] - 1.0).abs() < f32::EPSILON);
        assert!((result[1][1] - 1.0).abs() < f32::EPSILON);
        Ok(())
    }

    #[tokio::test]
    async fn dimension_mismatch_updates_stored_value() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/embeddings"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(embed_response(&[vec![1.0, 0.0, 0.0, 0.0]])),
            )
            .mount(&server)
            .await;

        let cfg = make_config(&server.uri());
        let embedder = OpenAiEmbedder::new(&cfg, &server.uri());
        assert_eq!(embedder.dimension(), 3);
        embedder.embed_documents(vec!["text".to_owned()]).await?;
        assert_eq!(embedder.dimension(), 4);
        Ok(())
    }
}
