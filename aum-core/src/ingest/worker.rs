//! Extraction worker tasks for the ingest pipeline.
//!
//! Workers pull file paths from a shared channel, acquire a pool slot, and
//! call [`Extractor::extract`] to produce documents.  Results are sent to the
//! batcher via a separate channel.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use futures::TryStreamExt as _;
use tokio::sync::{Mutex, mpsc};
use tracing::{debug, warn};

use crate::db::JobTracker;
use crate::extraction::Extractor;
use crate::models::Document;
use crate::pool::InstancePool;

// ---------------------------------------------------------------------------
// Worker result types
// ---------------------------------------------------------------------------

/// Successful extraction of a single file.
pub struct ExtractedFile {
    /// The file that was extracted.
    pub file_path: PathBuf,
    /// Canonicalised version of `file_path`, computed once per extraction to
    /// avoid repeated `canonicalize()` syscalls downstream.
    pub canonical_path: PathBuf,
    /// Documents produced (container + embedded parts).
    pub documents: Vec<Document>,
    /// Wall-clock time spent extracting, in seconds.
    pub extraction_secs: f64,
    /// Number of documents with empty content.
    pub empty_count: u64,
}

/// Result of processing a single file in a worker.
pub enum WorkerResult {
    /// Extraction succeeded (possibly with sub-errors recorded separately).
    Success(ExtractedFile),
    /// Extraction failed fatally for this file.
    Failure {
        /// The file that failed.
        file_path: PathBuf,
        /// Machine-readable error category.
        error_type: String,
        /// Human-readable error message.
        message: String,
    },
}

// ---------------------------------------------------------------------------
// Worker spawning
// ---------------------------------------------------------------------------

/// Spawn `max_workers` extraction worker tasks.
///
/// Each worker loops: receive a path from the shared `rx`, acquire a pool
/// slot via [`InstancePool::run_stream`], collect documents, and send the
/// result to `result_tx`.
///
/// Workers exit when `rx` is closed (all paths consumed) or `result_tx` is
/// dropped (pipeline shutting down).  `in_flight` is incremented while a
/// worker is actively extracting a file and decremented immediately after.
pub fn spawn_workers<E: Extractor + 'static>(
    pool: &Arc<InstancePool<E>>,
    rx: &Arc<Mutex<mpsc::Receiver<PathBuf>>>,
    result_tx: &mpsc::Sender<WorkerResult>,
    tracker: &JobTracker,
    job_id: &str,
    max_workers: u32,
    in_flight: &Arc<AtomicU64>,
) -> Vec<tokio::task::JoinHandle<()>> {
    (0..max_workers)
        .map(|worker_id| {
            let pool = pool.clone();
            let rx = rx.clone();
            let result_tx = result_tx.clone();
            let tracker = tracker.clone();
            let job_id = job_id.to_owned();
            let in_flight = in_flight.clone();

            tokio::spawn(async move {
                worker_loop(
                    worker_id, &pool, &rx, &result_tx, &tracker, &job_id, &in_flight,
                )
                .await;
            })
        })
        .collect()
}

/// Main loop for a single worker task.
async fn worker_loop<E: Extractor + 'static>(
    worker_id: u32,
    pool: &InstancePool<E>,
    rx: &Mutex<mpsc::Receiver<PathBuf>>,
    result_tx: &mpsc::Sender<WorkerResult>,
    tracker: &JobTracker,
    job_id: &str,
    in_flight: &AtomicU64,
) {
    let record_error = super::make_record_error_fn(tracker.clone(), job_id.to_owned());

    loop {
        let path = {
            let mut guard = rx.lock().await;
            match guard.recv().await {
                Some(p) => p,
                None => break, // channel closed — all paths consumed
            }
        };

        in_flight.fetch_add(1, Ordering::Relaxed);
        let result = extract_one(pool, &path, &record_error).await;
        in_flight.fetch_sub(1, Ordering::Relaxed);

        if result_tx.send(result).await.is_err() {
            debug!(worker_id, "result channel closed, worker exiting");
            break;
        }
    }

    debug!(worker_id, "worker exiting");
}

/// Extract a single file via the pool, returning a [`WorkerResult`].
async fn extract_one<E: Extractor + 'static>(
    pool: &InstancePool<E>,
    file_path: &Path,
    record_error: &crate::extraction::RecordErrorFn,
) -> WorkerResult {
    let start = Instant::now();

    let stream = pool.run_stream(|extractor| extractor.extract(file_path, Some(record_error)));

    let result = collect_extraction_stream(stream, file_path).await;
    let elapsed = start.elapsed().as_secs_f64();
    metrics::histogram!("aum_ingest_extraction_seconds").record(elapsed);

    match result {
        Ok((documents, empty_count)) => {
            let canonical = file_path
                .canonicalize()
                .unwrap_or_else(|_| file_path.to_owned());
            WorkerResult::Success(ExtractedFile {
                file_path: file_path.to_owned(),
                canonical_path: canonical,
                documents,
                extraction_secs: elapsed,
                empty_count,
            })
        }
        Err((error_type, message)) => {
            warn!(path = %file_path.display(), error_type, elapsed_secs = elapsed, "extraction failed");
            WorkerResult::Failure {
                file_path: file_path.to_owned(),
                error_type,
                message,
            }
        }
    }
}

/// Collect all documents from an extraction stream, counting empty ones.
///
/// Returns `Ok((docs, empty_count))` on success, or `Err((error_type, message))`
/// if the stream yields a fatal error.
async fn collect_extraction_stream(
    stream: futures::stream::BoxStream<'_, Result<Document, crate::extraction::ExtractionError>>,
    file_path: &Path,
) -> Result<(Vec<Document>, u64), (String, String)> {
    let mut documents = Vec::new();
    let mut empty_count: u64 = 0;

    let result: Result<(), _> = stream
        .try_for_each(|doc| {
            if doc.content.is_empty() {
                empty_count += 1;
            }
            documents.push(doc);
            async { Ok(()) }
        })
        .await;

    match result {
        Ok(()) => Ok((documents, empty_count)),
        Err(e) => {
            debug!(
                path = %file_path.display(),
                error = %e,
                "extraction stream error"
            );
            Err((e.error_type().to_owned(), e.to_string()))
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use futures::stream::BoxStream;
    use tokio::sync::{Mutex, mpsc};

    use super::*;
    use crate::extraction::{ExtractionError, Extractor, RecordErrorFn};
    use crate::ingest::test_helpers::{make_pool, make_tracker};
    use crate::models::{Document, JobType};

    // -- Mock extractor that returns a fixed document -----------------------

    struct MockExtractor {
        content: String,
    }

    impl Extractor for MockExtractor {
        fn extract<'a>(
            &'a self,
            file_path: &'a Path,
            _record_error: Option<&'a RecordErrorFn>,
        ) -> BoxStream<'a, Result<Document, ExtractionError>> {
            let doc = Document {
                source_path: file_path.to_owned(),
                content: self.content.clone(),
                metadata: HashMap::new(),
            };
            Box::pin(futures::stream::once(async move { Ok(doc) }))
        }

        fn supports(&self, _mime_type: &str) -> bool {
            true
        }
    }

    // -- Mock extractor that always fails -----------------------------------

    struct FailingExtractor;

    impl Extractor for FailingExtractor {
        fn extract<'a>(
            &'a self,
            file_path: &'a Path,
            _record_error: Option<&'a RecordErrorFn>,
        ) -> BoxStream<'a, Result<Document, ExtractionError>> {
            let err = ExtractionError::Io {
                path: file_path.to_owned(),
                source: std::io::Error::other("mock failure"),
            };
            Box::pin(futures::stream::once(async move { Err(err) }))
        }

        fn supports(&self, _mime_type: &str) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn worker_extracts_successfully() -> anyhow::Result<()> {
        let pool = make_pool(MockExtractor {
            content: "hello".to_owned(),
        })?;
        let tracker = make_tracker().await?;
        tracker
            .create_job("wj1", Path::new("/src"), "aum", JobType::Ingest, 0)
            .await?;

        let (path_tx, path_rx) = mpsc::channel(10);
        let (result_tx, mut result_rx) = mpsc::channel(10);
        let path_rx = Arc::new(Mutex::new(path_rx));

        let in_flight = Arc::new(AtomicU64::new(0));
        let handles = spawn_workers(&pool, &path_rx, &result_tx, &tracker, "wj1", 2, &in_flight);

        path_tx.send(PathBuf::from("/tmp/test.txt")).await?;
        drop(path_tx);

        let result = result_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("no result"))?;
        match result {
            WorkerResult::Success(ef) => {
                assert_eq!(ef.documents.len(), 1);
                assert_eq!(ef.documents[0].content, "hello");
                assert_eq!(ef.empty_count, 0);
            }
            WorkerResult::Failure { .. } => panic!("expected success"),
        }

        // Workers should exit after channel closes.
        for h in handles {
            h.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn worker_reports_failure() -> anyhow::Result<()> {
        let pool = make_pool(FailingExtractor)?;
        let tracker = make_tracker().await?;
        tracker
            .create_job("wj2", Path::new("/src"), "aum", JobType::Ingest, 0)
            .await?;

        let (path_tx, path_rx) = mpsc::channel(10);
        let (result_tx, mut result_rx) = mpsc::channel(10);
        let path_rx = Arc::new(Mutex::new(path_rx));

        let in_flight = Arc::new(AtomicU64::new(0));
        let handles = spawn_workers(&pool, &path_rx, &result_tx, &tracker, "wj2", 1, &in_flight);

        path_tx.send(PathBuf::from("/tmp/bad.pdf")).await?;
        drop(path_tx);

        let result = result_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("no result"))?;
        match result {
            WorkerResult::Failure {
                error_type,
                message,
                ..
            } => {
                assert!(!error_type.is_empty());
                assert!(!message.is_empty());
            }
            WorkerResult::Success(_) => panic!("expected failure"),
        }

        for h in handles {
            h.await?;
        }
        Ok(())
    }

    #[tokio::test]
    async fn worker_counts_empty_documents() -> anyhow::Result<()> {
        let pool = make_pool(MockExtractor {
            content: String::new(), // empty content
        })?;
        let tracker = make_tracker().await?;
        tracker
            .create_job("wj3", Path::new("/src"), "aum", JobType::Ingest, 0)
            .await?;

        let (path_tx, path_rx) = mpsc::channel(10);
        let (result_tx, mut result_rx) = mpsc::channel(10);
        let path_rx = Arc::new(Mutex::new(path_rx));

        let in_flight = Arc::new(AtomicU64::new(0));
        let handles = spawn_workers(&pool, &path_rx, &result_tx, &tracker, "wj3", 1, &in_flight);

        path_tx.send(PathBuf::from("/tmp/empty.txt")).await?;
        drop(path_tx);

        let result = result_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("no result"))?;
        match result {
            WorkerResult::Success(ef) => {
                assert_eq!(ef.empty_count, 1);
            }
            WorkerResult::Failure { .. } => panic!("expected success"),
        }

        for h in handles {
            h.await?;
        }
        Ok(())
    }
}
