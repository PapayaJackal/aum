//! Traits for consuming document batches and checking document existence.
//!
//! The ingest pipeline produces batches of `(doc_id, Document)` pairs and
//! pushes them through a [`BatchSink`].  In phase 4 the search backend will
//! implement this trait; until then [`NullSink`] can be used for testing.
//!
//! [`ExistenceChecker`] is used by resume mode to skip already-indexed files.

use std::collections::HashSet;

use crate::models::Document;

pub use crate::extraction::RecordErrorFn;

/// A consumer of document batches produced by the ingest pipeline.
///
/// Implementations might write to a search backend, dump to disk, or simply
/// count documents (useful for testing).
#[async_trait::async_trait]
pub trait BatchSink: Send + Sync {
    /// Process a batch of `(doc_id, Document)` pairs.
    ///
    /// Returns `(indexed_count, failed_count)`.  Individual document failures
    /// should be recorded via `record_error` rather than failing the whole
    /// batch.
    async fn flush_batch(
        &self,
        job_id: &str,
        batch: &[(String, Document)],
        record_error: &RecordErrorFn,
    ) -> (u64, u64);
}

/// A no-op sink that accepts every document without writing anywhere.
///
/// Useful for testing and for running the pipeline before the search backend
/// is ported.
pub struct NullSink;

#[async_trait::async_trait]
impl BatchSink for NullSink {
    async fn flush_batch(
        &self,
        _job_id: &str,
        batch: &[(String, Document)],
        _record_error: &RecordErrorFn,
    ) -> (u64, u64) {
        (batch.len() as u64, 0)
    }
}

/// Checks which document IDs already exist in the search backend.
///
/// Used by resume mode to skip files whose primary document is already
/// indexed.
#[async_trait::async_trait]
pub trait ExistenceChecker: Send + Sync {
    /// Return the subset of `doc_ids` that already exist.
    async fn get_existing(&self, doc_ids: &[String]) -> HashSet<String>;
}

/// A no-op checker that reports nothing as existing.
pub struct NoOpChecker;

#[async_trait::async_trait]
impl ExistenceChecker for NoOpChecker {
    async fn get_existing(&self, _doc_ids: &[String]) -> HashSet<String> {
        HashSet::new()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    use super::*;

    fn dummy_batch(n: usize) -> Vec<(String, Document)> {
        (0..n)
            .map(|i| {
                (
                    format!("doc_{i}"),
                    Document {
                        source_path: PathBuf::from(format!("/tmp/file_{i}.txt")),
                        content: String::new(),
                        metadata: HashMap::new(),
                    },
                )
            })
            .collect()
    }

    #[tokio::test]
    async fn null_sink_returns_batch_length() {
        let batch = dummy_batch(5);
        let record_error: RecordErrorFn = Arc::new(|_, _, _| {});
        let (indexed, failed) = NullSink.flush_batch("j1", &batch, &record_error).await;
        assert_eq!(indexed, 5);
        assert_eq!(failed, 0);
    }

    #[tokio::test]
    async fn no_op_checker_returns_empty() {
        let ids = vec!["a".to_owned(), "b".to_owned()];
        let existing = NoOpChecker.get_existing(&ids).await;
        assert!(existing.is_empty());
    }
}
