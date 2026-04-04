//! [`Embedder`] trait and L2-normalization helpers.

use super::error::EmbedError;

// ---------------------------------------------------------------------------
// Trait
// ---------------------------------------------------------------------------

/// Common interface for embedding backends.
///
/// Implementors wrap a specific backend (Ollama, OpenAI-compatible) and handle
/// HTTP communication, L2 normalization, and dimension tracking.
#[async_trait::async_trait]
pub trait Embedder: Send + Sync {
    /// Dimension of the embedding vectors produced by this backend.
    ///
    /// May be updated after the first request if the backend returns a
    /// different dimension than the one configured.
    fn dimension(&self) -> u32;

    /// Embed a batch of document texts.
    ///
    /// Takes ownership of `texts` so the resulting future can be sent across
    /// task boundaries without lifetime complications. No query prefix is
    /// added. Results are L2-normalised.
    async fn embed_documents(&self, texts: Vec<String>) -> Result<Vec<Vec<f32>>, EmbedError>;

    /// Embed a single query string.
    ///
    /// The implementation should prepend the configured query prefix before
    /// sending to the backend. Result is L2-normalised.
    async fn embed_query(&self, text: &str) -> Result<Vec<f32>, EmbedError>;
}

// ---------------------------------------------------------------------------
// Normalization helpers
// ---------------------------------------------------------------------------

/// L2-normalize *v* in-place.
///
/// If the vector has zero magnitude it is left unchanged.
pub fn l2_normalize(v: &mut [f32]) {
    let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
    if norm > 0.0 {
        for x in v.iter_mut() {
            *x /= norm;
        }
    }
}

/// L2-normalize every vector in *batch* in-place.
pub fn l2_normalize_batch(batch: &mut [Vec<f32>]) {
    for v in batch.iter_mut() {
        l2_normalize(v);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn l2_normalize_unit_vector() {
        let mut v = vec![3.0_f32, 4.0];
        l2_normalize(&mut v);
        let norm: f32 = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-6);
    }

    #[test]
    fn l2_normalize_zero_vector_unchanged() {
        let mut v = vec![0.0_f32, 0.0];
        l2_normalize(&mut v);
        assert_eq!(v, vec![0.0, 0.0]);
    }
}
