//! Extraction worker tasks for the ingest pipeline.
//!
//! A single dispatcher task reads file paths from the shared channel and
//! spawns an extraction task for each one.  A [`tokio::sync::Semaphore`]
//! caps the number of concurrent extractions at `max_workers`, so a new
//! extraction begins as soon as *any* in-flight extraction finishes — no
//! batch boundaries, no head-of-line blocking.

use futures::StreamExt as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;
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
/// Cheap to clone: the inner `Arc` bumps a reference count.  Uses an atomic
/// counter (always updated) plus an optional [`HashSet`] of paths (populated
/// only when `debug` is true) to avoid mutex overhead on hot paths in
/// production.
#[derive(Clone)]
pub struct InFlightState {
    debug: bool,
    count: Arc<AtomicU64>,
    paths: Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
}

impl InFlightState {
    /// Create a new `InFlightState`.  When `debug` is true, path strings are
    /// tracked and returned by [`snapshot`](Self::snapshot); otherwise only
    /// the count is maintained.
    #[must_use]
    pub fn new(debug: bool) -> Self {
        Self {
            debug,
            count: Arc::new(AtomicU64::new(0)),
            paths: Arc::default(),
        }
    }

    /// Register `path` as in-flight.
    pub fn add_path(&self, path: &str) {
        self.count.fetch_add(1, Ordering::Relaxed);
        if self.debug {
            self.paths
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(path.to_owned());
        }
    }

    /// Deregister `path`.
    pub fn remove_path(&self, path: &str) {
        self.count.fetch_sub(1, Ordering::Relaxed);
        if self.debug {
            self.paths
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .remove(path);
        }
    }

    /// Returns `(count, paths_clone)`.  The count is always accurate; the
    /// path vec is empty unless constructed with `debug = true`.
    pub fn snapshot(&self) -> (u64, Vec<String>) {
        let count = self.count.load(Ordering::Relaxed);
        let paths = if self.debug {
            self.paths
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        (count, paths)
    }
}

// ---------------------------------------------------------------------------
// Worker result types
// ---------------------------------------------------------------------------

/// A single document extracted from a file, sent individually through the
/// result channel so that large archives don't buffer all documents in memory.
pub struct ExtractedDocument {
    /// Canonicalised source path, computed once per extraction to avoid
    /// repeated `canonicalize()` syscalls downstream.
    pub canonical_path: PathBuf,
    /// Zero-based index of this document within the source file.
    pub doc_index: u64,
    /// The extracted document.
    pub doc: Document,
}

/// Signals that all documents from a file have been streamed.
pub struct FileComplete {
    /// The source file path.
    pub file_path: PathBuf,
    /// Total number of documents produced by this file.
    pub total_doc_count: u64,
    /// Wall-clock time spent extracting, in seconds.
    pub extraction_secs: f64,
    /// Number of documents with empty content.
    pub empty_count: u64,
}

/// Messages sent from worker tasks to the batcher.
pub enum WorkerResult {
    /// A single extracted document.
    Document(ExtractedDocument),
    /// All documents for a file have been sent.
    FileComplete(FileComplete),
    /// Extraction failed fatally (no documents produced).
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

                stream_file(&pool, &path, &record_error, &result_tx).await;

                in_flight.remove_path(&path_str);

                // Release the permit so the dispatcher can spawn the next task.
                drop(permit);
            });
        }

        // All paths consumed — wait for remaining in-flight extractions.
        // Acquiring all permits guarantees every spawned task has finished
        // and released its permit.
        let _ = semaphore.acquire_many(max_workers).await;

        debug!("dispatcher exiting, all extractions complete");
    })
}

/// Stream documents from a single file extraction through the result channel.
///
/// Each document is sent individually so that large archives (ZIP, PST, etc.)
/// don't buffer all embedded documents in memory at once.  A [`FileComplete`]
/// message is sent after all documents, or a [`Failure`](WorkerResult::Failure)
/// if the very first stream item is an error.
async fn stream_file<E: Extractor + 'static>(
    pool: &InstancePool<E>,
    file_path: &Path,
    record_error: &crate::extraction::RecordErrorFn,
    result_tx: &mpsc::Sender<WorkerResult>,
) {
    let start = Instant::now();
    let canonical = file_path
        .canonicalize()
        .unwrap_or_else(|_| file_path.to_owned());

    // In clustered mode (multiple Tika instances) the pool retries the
    // extraction on a different instance when the first one fails before
    // producing any documents.  The max_retries is capped internally to
    // the number of alternative instances available.
    #[allow(clippy::cast_possible_truncation)]
    let instance_retries = pool.len().saturating_sub(1) as u32;
    let stream = pool.run_stream_with_retry(instance_retries, |extractor| {
        extractor.extract(file_path, Some(record_error))
    });
    tokio::pin!(stream);

    let mut doc_index: u64 = 0;
    let mut empty_count: u64 = 0;

    let mut fatal_error: Option<crate::extraction::ExtractionError> = None;

    while let Some(item) = stream.next().await {
        match item {
            Ok(doc) => {
                if doc.content.is_empty() {
                    empty_count += 1;
                }
                let msg = WorkerResult::Document(ExtractedDocument {
                    canonical_path: canonical.clone(),
                    doc_index,
                    doc,
                });
                doc_index += 1;
                if result_tx.send(msg).await.is_err() {
                    debug!(path = %file_path.display(), "result channel closed");
                    return;
                }
            }
            Err(e) => {
                fatal_error = Some(e);
                break;
            }
        }
    }

    let elapsed = start.elapsed().as_secs_f64();
    if let Some(e) = fatal_error {
        if doc_index == 0 {
            // No documents produced — report as a fatal failure.
            warn!(
                path = %file_path.display(),
                error_type = e.error_type(),
                elapsed_secs = elapsed,
                "extraction failed"
            );
            let _ = result_tx
                .send(WorkerResult::Failure {
                    file_path: file_path.to_owned(),
                    error_type: e.error_type().to_owned(),
                    message: e.to_string(),
                })
                .await;
        } else {
            // Partial success — docs already sent, just log and stop.
            debug!(
                path = %file_path.display(),
                docs_sent = doc_index,
                error = %e,
                "extraction stream error after partial success"
            );
            let _ = result_tx
                .send(WorkerResult::FileComplete(FileComplete {
                    file_path: file_path.to_owned(),
                    total_doc_count: doc_index,
                    extraction_secs: elapsed,
                    empty_count,
                }))
                .await;
        }
    } else {
        let _ = result_tx
            .send(WorkerResult::FileComplete(FileComplete {
                file_path: file_path.to_owned(),
                total_doc_count: doc_index,
                extraction_secs: elapsed,
                empty_count,
            }))
            .await;
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

    fn variant_name(r: &WorkerResult) -> &'static str {
        match r {
            WorkerResult::Document(_) => "Document",
            WorkerResult::FileComplete(_) => "FileComplete",
            WorkerResult::Failure { .. } => "Failure",
        }
    }

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
            .create_job("wj1", Path::new("/src"), "aum", JobType::Ingest, 0, None)
            .await?;

        let (path_tx, path_rx) = mpsc::channel(10);
        let (result_tx, mut result_rx) = mpsc::channel(10);

        let in_flight = InFlightState::new(false);
        let handle = spawn_dispatcher(pool, path_rx, result_tx, tracker, "wj1", 2, in_flight);

        path_tx.send(PathBuf::from("/tmp/test.txt")).await?;
        drop(path_tx);

        // First message: the document itself.
        let result = result_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("no result"))?;
        match result {
            WorkerResult::Document(ed) => {
                assert_eq!(ed.doc.content, "hello");
                assert_eq!(ed.doc_index, 0);
            }
            other => panic!("expected Document, got {}", variant_name(&other)),
        }

        // Second message: file-complete stats.
        let result = result_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("no file-complete"))?;
        match result {
            WorkerResult::FileComplete(fc) => {
                assert_eq!(fc.empty_count, 0);
            }
            other => panic!("expected FileComplete, got {}", variant_name(&other)),
        }

        handle.await?;
        Ok(())
    }

    #[tokio::test]
    async fn dispatcher_reports_failure() -> anyhow::Result<()> {
        let pool = make_pool(FailingExtractor)?;
        let tracker = make_tracker().await?;
        tracker
            .create_job("wj2", Path::new("/src"), "aum", JobType::Ingest, 0, None)
            .await?;

        let (path_tx, path_rx) = mpsc::channel(10);
        let (result_tx, mut result_rx) = mpsc::channel(10);

        let in_flight = InFlightState::new(false);
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
            other => panic!("expected Failure, got {}", variant_name(&other)),
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
            .create_job("wj3", Path::new("/src"), "aum", JobType::Ingest, 0, None)
            .await?;

        let (path_tx, path_rx) = mpsc::channel(10);
        let (result_tx, mut result_rx) = mpsc::channel(10);

        let in_flight = InFlightState::new(false);
        let handle = spawn_dispatcher(pool, path_rx, result_tx, tracker, "wj3", 1, in_flight);

        path_tx.send(PathBuf::from("/tmp/empty.txt")).await?;
        drop(path_tx);

        // Document message (empty content).
        let result = result_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("no result"))?;
        assert!(matches!(result, WorkerResult::Document(_)));

        // FileComplete with empty_count = 1.
        let result = result_rx
            .recv()
            .await
            .ok_or_else(|| anyhow::anyhow!("no file-complete"))?;
        match result {
            WorkerResult::FileComplete(fc) => {
                assert_eq!(fc.empty_count, 1);
            }
            other => panic!("expected FileComplete, got {}", variant_name(&other)),
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
            .create_job("wj4", Path::new("/src"), "aum", JobType::Ingest, 0, None)
            .await?;

        let max_workers = 3u32;
        let num_files = 10;

        let (path_tx, path_rx) = mpsc::channel(num_files);
        let (result_tx, mut result_rx) = mpsc::channel(num_files);

        let in_flight = InFlightState::new(false);
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

        let mut file_completes = 0usize;
        while let Some(msg) = result_rx.recv().await {
            if matches!(msg, WorkerResult::FileComplete(_)) {
                file_completes += 1;
            }
        }

        handle.await?;

        assert_eq!(file_completes, num_files);
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
