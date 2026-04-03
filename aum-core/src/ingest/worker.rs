//! Extraction worker tasks for the ingest pipeline.
//!
//! A single dispatcher task reads file paths from the shared channel and
//! spawns an extraction task for each one.  A [`tokio::sync::Semaphore`]
//! caps the number of concurrent extractions at `max_workers`, so a new
//! extraction begins as soon as *any* in-flight extraction finishes — no
//! batch boundaries, no head-of-line blocking.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use futures::TryStreamExt as _;
use tokio::sync::mpsc;
use tracing::{debug, warn};

use crate::db::JobTracker;
use crate::extraction::Extractor;
use crate::models::Document;
use crate::pool::InstancePool;

// ---------------------------------------------------------------------------
// In-flight tracking
// ---------------------------------------------------------------------------

/// Shared state tracking which files are currently in extraction.
///
/// Cheap to clone: the inner `Arc` bumps a reference count.
#[derive(Clone, Default)]
pub struct InFlightState {
    paths: Arc<std::sync::Mutex<Vec<String>>>,
}

impl InFlightState {
    /// Register `path` as in-flight.
    pub fn add_path(&self, path: &str) {
        self.paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(path.to_owned());
    }

    /// Deregister `path` (removes the first matching entry).
    pub fn remove_path(&self, path: &str) {
        self.paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|p| p != path);
    }

    /// Returns `(count, paths_clone)` in a single lock acquisition.
    pub fn snapshot(&self) -> (u64, Vec<String>) {
        let guard = self
            .paths
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (guard.len() as u64, guard.clone())
    }
}

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

/// Spawn a dispatcher task that keeps up to `max_workers` extractions
/// running concurrently.
///
/// The dispatcher reads paths from `rx` and, for each one, acquires a
/// semaphore permit before spawning a new extraction task.  Because the
/// semaphore is acquired *before* spawning, at most `max_workers` tasks
/// run at any time, and a new extraction begins as soon as any permit is
/// released — no batch boundaries.
///
/// The returned [`JoinHandle`] resolves once every path has been consumed
/// and every spawned extraction task has completed.
pub fn spawn_dispatcher<E: Extractor + 'static>(
    pool: Arc<InstancePool<E>>,
    mut rx: mpsc::Receiver<PathBuf>,
    result_tx: mpsc::Sender<WorkerResult>,
    tracker: JobTracker,
    job_id: &str,
    max_workers: u32,
    in_flight: InFlightState,
) -> tokio::task::JoinHandle<()> {
    let job_id = job_id.to_owned();

    tokio::spawn(async move {
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_workers as usize));
        let record_error = super::make_record_error_fn(tracker.clone(), job_id.clone());

        while let Some(path) = rx.recv().await {
            // Wait until a slot is free — this is where backpressure happens.
            // The semaphore is never closed, so `acquire_owned` always succeeds.
            let Ok(permit) = Arc::clone(&semaphore).acquire_owned().await else {
                break;
            };

            let pool = Arc::clone(&pool);
            let result_tx = result_tx.clone();
            let in_flight = in_flight.clone();
            let record_error = Arc::clone(&record_error);

            tokio::spawn(async move {
                let path_str = path.display().to_string();
                in_flight.add_path(&path_str);

                let result = extract_one(&pool, &path, &record_error).await;

                in_flight.remove_path(&path_str);

                // Release the permit so the dispatcher can spawn the next task.
                drop(permit);

                if result_tx.send(result).await.is_err() {
                    debug!(path = %path_str, "result channel closed, dropping result");
                }
            });
        }

        // All paths consumed — wait for remaining in-flight extractions.
        // Acquiring all permits guarantees every spawned task has finished
        // and released its permit.
        let _ = semaphore.acquire_many(max_workers).await;

        debug!("dispatcher exiting, all extractions complete");
    })
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
    use std::sync::atomic::{AtomicU32, Ordering};

    use futures::stream::BoxStream;
    use tokio::sync::mpsc;

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

    // -- Slow extractor for concurrency testing ----------------------------

    struct SlowExtractor {
        /// Tracks the peak number of concurrent extractions.
        peak_concurrent: Arc<AtomicU32>,
        /// Current number of concurrent extractions.
        current: Arc<AtomicU32>,
    }

    impl Extractor for SlowExtractor {
        fn extract<'a>(
            &'a self,
            file_path: &'a Path,
            _record_error: Option<&'a RecordErrorFn>,
        ) -> BoxStream<'a, Result<Document, ExtractionError>> {
            let peak = Arc::clone(&self.peak_concurrent);
            let current = Arc::clone(&self.current);
            let path = file_path.to_owned();

            Box::pin(async_stream::try_stream! {
                let val = current.fetch_add(1, Ordering::SeqCst) + 1;
                peak.fetch_max(val, Ordering::SeqCst);

                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                current.fetch_sub(1, Ordering::SeqCst);

                yield Document {
                    source_path: path,
                    content: "slow".to_owned(),
                    metadata: HashMap::new(),
                };
            })
        }

        fn supports(&self, _mime_type: &str) -> bool {
            true
        }
    }

    #[tokio::test]
    async fn dispatcher_extracts_successfully() -> anyhow::Result<()> {
        let pool = make_pool(MockExtractor {
            content: "hello".to_owned(),
        })?;
        let tracker = make_tracker().await?;
        tracker
            .create_job("wj1", Path::new("/src"), "aum", JobType::Ingest, 0)
            .await?;

        let (path_tx, path_rx) = mpsc::channel(10);
        let (result_tx, mut result_rx) = mpsc::channel(10);

        let in_flight = InFlightState::default();
        let handle = spawn_dispatcher(pool, path_rx, result_tx, tracker, "wj1", 2, in_flight);

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

        handle.await?;
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_reports_failure() -> anyhow::Result<()> {
        let pool = make_pool(FailingExtractor)?;
        let tracker = make_tracker().await?;
        tracker
            .create_job("wj2", Path::new("/src"), "aum", JobType::Ingest, 0)
            .await?;

        let (path_tx, path_rx) = mpsc::channel(10);
        let (result_tx, mut result_rx) = mpsc::channel(10);

        let in_flight = InFlightState::default();
        let handle = spawn_dispatcher(pool, path_rx, result_tx, tracker, "wj2", 1, in_flight);

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

        handle.await?;
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_counts_empty_documents() -> anyhow::Result<()> {
        let pool = make_pool(MockExtractor {
            content: String::new(),
        })?;
        let tracker = make_tracker().await?;
        tracker
            .create_job("wj3", Path::new("/src"), "aum", JobType::Ingest, 0)
            .await?;

        let (path_tx, path_rx) = mpsc::channel(10);
        let (result_tx, mut result_rx) = mpsc::channel(10);

        let in_flight = InFlightState::default();
        let handle = spawn_dispatcher(pool, path_rx, result_tx, tracker, "wj3", 1, in_flight);

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

        handle.await?;
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_respects_concurrency_limit() -> anyhow::Result<()> {
        let peak = Arc::new(AtomicU32::new(0));
        let current = Arc::new(AtomicU32::new(0));
        let pool = make_pool(SlowExtractor {
            peak_concurrent: Arc::clone(&peak),
            current: Arc::clone(&current),
        })?;
        let tracker = make_tracker().await?;
        tracker
            .create_job("wj4", Path::new("/src"), "aum", JobType::Ingest, 0)
            .await?;

        let max_workers = 3u32;
        let num_files = 10;

        let (path_tx, path_rx) = mpsc::channel(num_files);
        let (result_tx, mut result_rx) = mpsc::channel(num_files);

        let in_flight = InFlightState::default();
        let handle = spawn_dispatcher(
            pool,
            path_rx,
            result_tx,
            tracker,
            "wj4",
            max_workers,
            in_flight,
        );

        for i in 0..num_files {
            path_tx
                .send(PathBuf::from(format!("/tmp/file{i}.txt")))
                .await?;
        }
        drop(path_tx);

        let mut results = 0;
        while result_rx.recv().await.is_some() {
            results += 1;
        }

        handle.await?;

        assert_eq!(results, num_files);
        assert!(
            peak.load(Ordering::SeqCst) <= max_workers,
            "peak concurrency {} exceeded max_workers {}",
            peak.load(Ordering::SeqCst),
            max_workers,
        );
        assert!(
            peak.load(Ordering::SeqCst) >= 2,
            "expected at least 2 concurrent extractions, got {}",
            peak.load(Ordering::SeqCst),
        );
        Ok(())
    }
}
