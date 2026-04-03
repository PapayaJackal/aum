//! Ingest pipeline orchestrator.
//!
//! [`IngestPipeline`] ties together directory walking, concurrent extraction,
//! batching, and progress tracking into a single async operation.  Three entry
//! points cover the common workflows:
//!
//! - [`run`](IngestPipeline::run) — full ingest of a directory
//! - [`run_resume`](IngestPipeline::run_resume) — resume an interrupted ingest,
//!   skipping already-indexed documents
//! - [`run_retry`](IngestPipeline::run_retry) — retry specific failed files

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Instant;

use tokio::sync::{Mutex, mpsc};
use tracing::{debug, info, warn};

use crate::db::JobTracker;
use crate::extraction::Extractor;
use crate::models::{IngestJob, JobProgress, JobStatus, JobType};
use crate::pool::InstancePool;

use super::progress::{IngestSnapshot, ProgressTx};

use super::display_path::set_display_path;
use super::doc_id::file_doc_id;
use super::error::IngestPipelineError;
use crate::extraction::RecordErrorFn;

use super::sink::{BatchSink, ExistenceChecker};
use super::walker::{self, PATH_CHANNEL_CAPACITY};
use super::worker::{self, WorkerResult};

use crate::names::generate_name;

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// Orchestrates concurrent document extraction and batched indexing.
pub struct IngestPipeline<E: Extractor + 'static, S: BatchSink> {
    pool: Arc<InstancePool<E>>,
    sink: Arc<S>,
    tracker: JobTracker,
    index_name: String,
    batch_size: u32,
    max_workers: u32,
    /// Optional progress channel.  When set, the pipeline emits an
    /// [`IngestSnapshot`] after every processed file so callers can render a
    /// live display.
    progress_tx: Option<ProgressTx>,
}

impl<E: Extractor + 'static, S: BatchSink> IngestPipeline<E, S> {
    /// Create a new pipeline.
    #[must_use]
    pub fn new(
        pool: Arc<InstancePool<E>>,
        sink: Arc<S>,
        tracker: JobTracker,
        index_name: String,
        batch_size: u32,
        max_workers: u32,
    ) -> Self {
        Self {
            pool,
            sink,
            tracker,
            index_name,
            batch_size,
            max_workers,
            progress_tx: None,
        }
    }

    /// Attach a progress channel.
    ///
    /// After every processed file the pipeline will send an [`IngestSnapshot`]
    /// via `tx`.  Use [`tokio::sync::watch::channel`] to create the pair;
    /// hold the receiver in a background render task.
    #[must_use]
    pub fn with_progress(mut self, tx: ProgressTx) -> Self {
        self.progress_tx = Some(tx);
        self
    }

    /// Run a full ingest on `source_dir`.
    ///
    /// # Errors
    ///
    /// Returns [`IngestPipelineError`] on source-directory or database failures.
    pub async fn run(&self, source_dir: &Path) -> Result<IngestJob, IngestPipelineError> {
        let source_dir = canonicalize_source(source_dir)?;
        let job_id = generate_name();
        info!(job_id, source_dir = %source_dir.display(), "starting ingest");

        self.run_pipeline(&job_id, &source_dir, PipelineMode::Walk)
            .await
    }

    /// Resume an interrupted ingest, skipping already-indexed documents.
    ///
    /// Re-walks `source_dir` but filters out files whose primary document ID
    /// already exists according to `checker`.
    ///
    /// # Errors
    ///
    /// Returns [`IngestPipelineError`] on source-directory or database failures.
    pub async fn run_resume(
        &self,
        source_dir: &Path,
        checker: Arc<dyn ExistenceChecker>,
    ) -> Result<IngestJob, IngestPipelineError> {
        let source_dir = canonicalize_source(source_dir)?;
        let job_id = generate_name();
        info!(job_id, source_dir = %source_dir.display(), "resuming ingest");

        self.run_pipeline(
            &job_id,
            &source_dir,
            PipelineMode::Resume(checker, self.index_name.clone()),
        )
        .await
    }

    /// Retry specific file paths that failed in a previous ingest.
    ///
    /// # Errors
    ///
    /// Returns [`IngestPipelineError`] on source-directory or database failures.
    pub async fn run_retry(
        &self,
        file_paths: Vec<PathBuf>,
        source_dir: &Path,
    ) -> Result<IngestJob, IngestPipelineError> {
        let source_dir = canonicalize_source(source_dir)?;
        let job_id = generate_name();
        info!(
            job_id,
            source_dir = %source_dir.display(),
            file_count = file_paths.len(),
            "retrying failed files"
        );

        self.run_pipeline(&job_id, &source_dir, PipelineMode::Retry(file_paths))
            .await
    }
}

// ---------------------------------------------------------------------------
// Internal pipeline mode and shared counters
// ---------------------------------------------------------------------------

enum PipelineMode {
    /// Walk the source directory for all files.
    Walk,
    /// Walk but filter out already-indexed files.
    Resume(Arc<dyn ExistenceChecker>, String),
    /// Process explicit file paths.
    Retry(Vec<PathBuf>),
}

/// Atomic counters shared across the source task, workers, and batcher.
struct PipelineCounters {
    /// Total files discovered by the walker.
    discovered: Arc<AtomicU64>,
    /// Files skipped in resume mode (already indexed).
    skip_count: Arc<AtomicU64>,
    /// Set to `true` (with `Release` ordering) once the walker finishes.
    scan_done: Arc<AtomicBool>,
    /// Number of workers currently executing an extraction.
    in_flight: Arc<AtomicU64>,
}

impl PipelineCounters {
    fn new() -> Self {
        Self {
            discovered: Arc::new(AtomicU64::new(0)),
            skip_count: Arc::new(AtomicU64::new(0)),
            scan_done: Arc::new(AtomicBool::new(false)),
            in_flight: Arc::new(AtomicU64::new(0)),
        }
    }
}

// ---------------------------------------------------------------------------
// Core pipeline implementation
// ---------------------------------------------------------------------------

impl<E: Extractor + 'static, S: BatchSink> IngestPipeline<E, S> {
    /// The shared core of `run`, `run_resume`, and `run_retry`.
    async fn run_pipeline(
        &self,
        job_id: &str,
        source_dir: &Path,
        mode: PipelineMode,
    ) -> Result<IngestJob, IngestPipelineError> {
        metrics::gauge!("aum_ingest_jobs_active").increment(1.0);

        let job = self
            .tracker
            .create_job(job_id, source_dir, &self.index_name, JobType::Ingest, 0)
            .await?;

        let result = self.execute_pipeline(job_id, source_dir, mode).await;

        let final_status = match &result {
            Ok(_) => JobStatus::Completed,
            Err(IngestPipelineError::Cancelled) => JobStatus::Interrupted,
            Err(_) => JobStatus::Failed,
        };

        if let Err(e) = self.tracker.complete_job(job_id, final_status).await {
            warn!(job_id, error = %e, "failed to mark job as complete");
        }

        metrics::gauge!("aum_ingest_jobs_active").decrement(1.0);

        let progress = result?;
        let job = self.tracker.get_job(job_id, false).await?.unwrap_or(job);
        info!(
            job_id,
            extracted = progress.extracted,
            processed = progress.processed,
            failed = progress.failed,
            empty = progress.empty,
            skipped = progress.skipped,
            "ingest complete"
        );
        Ok(job)
    }

    /// Execute the pipeline stages: spawn walker, workers, and run the batcher.
    async fn execute_pipeline(
        &self,
        job_id: &str,
        source_dir: &Path,
        mode: PipelineMode,
    ) -> Result<JobProgress, IngestPipelineError> {
        let counters = PipelineCounters::new();

        let (path_tx, path_rx) = mpsc::channel(PATH_CHANNEL_CAPACITY);

        let result_channel_cap = (self.batch_size as usize).saturating_mul(2).max(16);
        let (result_tx, result_rx) = mpsc::channel(result_channel_cap);

        let source_handle = Self::spawn_source_task(mode, source_dir, path_tx, &counters);

        let path_rx = Arc::new(Mutex::new(path_rx));
        let worker_handles = worker::spawn_workers(
            &self.pool,
            &path_rx,
            &result_tx,
            &self.tracker,
            job_id,
            self.max_workers,
            &counters.in_flight,
        );

        // Drop our copy so `result_rx` closes when all workers finish.
        drop(result_tx);

        let progress = self
            .batcher_loop(job_id, source_dir, result_rx, &counters)
            .await?;

        for handle in worker_handles {
            if let Err(e) = handle.await {
                warn!(error = %e, "worker task panicked");
            }
        }
        if let Err(e) = source_handle.await {
            warn!(error = %e, "source task panicked");
        }

        Ok(progress)
    }

    /// Spawn the file-source task based on the pipeline mode.
    ///
    /// Sets `counters.scan_done` to `true` (with `Release` ordering) after all
    /// paths have been produced.
    fn spawn_source_task(
        mode: PipelineMode,
        source_dir: &Path,
        path_tx: mpsc::Sender<PathBuf>,
        counters: &PipelineCounters,
    ) -> tokio::task::JoinHandle<()> {
        let source_dir = source_dir.to_owned();
        let discovered = counters.discovered.clone();
        let skip_count = counters.skip_count.clone();
        let scan_done = counters.scan_done.clone();

        tokio::spawn(async move {
            match mode {
                PipelineMode::Walk => {
                    if let Err(e) = walker::walk_directory(&source_dir, &path_tx, &discovered).await
                    {
                        warn!(error = %e, "directory walk failed");
                    }
                }
                PipelineMode::Resume(checker, index) => {
                    let (walk_tx, walk_rx) = mpsc::channel(PATH_CHANNEL_CAPACITY);

                    let filter_handle = tokio::spawn(async move {
                        walker::filter_existing(walk_rx, path_tx, checker, index, skip_count).await;
                    });

                    if let Err(e) = walker::walk_directory(&source_dir, &walk_tx, &discovered).await
                    {
                        warn!(error = %e, "directory walk failed");
                    }
                    drop(walk_tx);
                    let _ = filter_handle.await;
                }
                PipelineMode::Retry(paths) => {
                    walker::feed_paths(paths, &path_tx, &discovered).await;
                }
            }
            scan_done.store(true, Ordering::Release);
        })
    }
}

// ---------------------------------------------------------------------------
// Batcher loop
// ---------------------------------------------------------------------------

impl<E: Extractor + 'static, S: BatchSink> IngestPipeline<E, S> {
    /// Receive extraction results, batch them, and flush to the sink.
    async fn batcher_loop(
        &self,
        job_id: &str,
        source_dir: &Path,
        mut result_rx: mpsc::Receiver<WorkerResult>,
        counters: &PipelineCounters,
    ) -> Result<JobProgress, IngestPipelineError> {
        let mut progress = JobProgress::default();
        let mut batch: Vec<(String, crate::models::Document)> =
            Vec::with_capacity(self.batch_size as usize);
        let mut last_total_update: u64 = 0;
        let mut extraction_secs_total: f64 = 0.0;

        let record_error = super::make_record_error_fn(self.tracker.clone(), job_id.to_owned());

        while let Some(result) = result_rx.recv().await {
            match result {
                WorkerResult::Success(ef) => {
                    extraction_secs_total += ef.extraction_secs;
                    Self::handle_extracted_file(ef, source_dir, &mut batch, &mut progress);
                }
                WorkerResult::Failure {
                    file_path,
                    error_type,
                    message,
                } => {
                    progress.failed += 1;
                    metrics::counter!("aum_ingest_docs_total", "status" => "failed").increment(1);
                    if let Err(e) = self
                        .tracker
                        .record_error(job_id, &file_path, &error_type, &message)
                        .await
                    {
                        warn!(error = %e, "failed to record extraction error");
                    }
                }
            }

            if batch.len() >= self.batch_size as usize {
                self.flush_batch(job_id, &mut batch, &mut progress, &record_error)
                    .await?;
            }

            // Periodically sync the walker's discovered count to the tracker.
            let current_discovered = counters.discovered.load(Ordering::Relaxed);
            if current_discovered != last_total_update {
                last_total_update = current_discovered;
                progress.skipped = saturating_i64(counters.skip_count.load(Ordering::Relaxed));
                let _ = self
                    .tracker
                    .update_total_files(job_id, saturating_i64(current_discovered))
                    .await;
            }

            emit_snapshot(
                self.progress_tx.as_ref(),
                &progress,
                counters,
                extraction_secs_total,
            );
        }

        if !batch.is_empty() {
            self.flush_batch(job_id, &mut batch, &mut progress, &record_error)
                .await?;
        }

        progress.skipped = saturating_i64(counters.skip_count.load(Ordering::Relaxed));
        let _ = self.tracker.update_progress(job_id, &progress).await;
        let _ = self
            .tracker
            .update_total_files(
                job_id,
                saturating_i64(counters.discovered.load(Ordering::Relaxed)),
            )
            .await;

        // Final snapshot so the renderer always sees the completed state.
        emit_snapshot(
            self.progress_tx.as_ref(),
            &progress,
            counters,
            extraction_secs_total,
        );

        Ok(progress)
    }

    /// Process a successful extraction: generate doc IDs, set display paths,
    /// and append to the batch buffer.
    fn handle_extracted_file(
        ef: worker::ExtractedFile,
        source_dir: &Path,
        batch: &mut Vec<(String, crate::models::Document)>,
        progress: &mut JobProgress,
    ) {
        let canonical = ef
            .file_path
            .canonicalize()
            .unwrap_or_else(|_| ef.file_path.clone());

        progress.extracted += 1;
        progress.empty += saturating_i64(ef.empty_count);

        if ef.empty_count > 0 {
            metrics::counter!("aum_ingest_docs_total", "status" => "empty")
                .increment(ef.empty_count);
        }

        for (i, mut doc) in ef.documents.into_iter().enumerate() {
            let doc_id = file_doc_id(&canonical, i as u64);
            set_display_path(&mut doc, source_dir);
            batch.push((doc_id, doc));
        }
    }

    /// Flush the batch to the sink and update progress.
    async fn flush_batch(
        &self,
        job_id: &str,
        batch: &mut Vec<(String, crate::models::Document)>,
        progress: &mut JobProgress,
        record_error: &RecordErrorFn,
    ) -> Result<(), IngestPipelineError> {
        let start = Instant::now();

        let (indexed, failed) = self
            .sink
            .flush_batch(&self.index_name, job_id, batch, record_error)
            .await;

        let elapsed = start.elapsed().as_secs_f64();
        metrics::histogram!("aum_ingest_batch_flush_seconds").record(elapsed);
        metrics::counter!("aum_ingest_docs_total", "status" => "indexed").increment(indexed);
        if failed > 0 {
            metrics::counter!("aum_ingest_docs_total", "status" => "failed").increment(failed);
        }

        progress.processed += saturating_i64(indexed);
        progress.failed += saturating_i64(failed);
        batch.clear();

        debug!(
            job_id,
            indexed,
            failed,
            elapsed_secs = elapsed,
            "batch flushed"
        );

        let _ = self.tracker.update_progress(job_id, progress).await;

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Saturating conversion from u64 to i64.  Counts that exceed `i64::MAX` are
/// clamped — in practice ingest counters never reach this limit.
fn saturating_i64(n: u64) -> i64 {
    i64::try_from(n).unwrap_or(i64::MAX)
}

/// Send a progress snapshot over the watch channel if one is attached.
///
/// `send_replace` never blocks and silently succeeds when there are no
/// receivers, so this is always safe to call from the batcher loop.
fn emit_snapshot(
    tx: Option<&ProgressTx>,
    progress: &JobProgress,
    counters: &PipelineCounters,
    total_extraction_secs: f64,
) {
    let Some(tx) = tx else { return };
    tx.send_replace(IngestSnapshot {
        discovered: counters.discovered.load(Ordering::Relaxed),
        scan_complete: counters.scan_done.load(Ordering::Acquire),
        in_flight: counters.in_flight.load(Ordering::Relaxed),
        extracted: progress.extracted.cast_unsigned(),
        indexed: progress.processed.cast_unsigned(),
        skipped: progress.skipped.cast_unsigned(),
        empty: progress.empty.cast_unsigned(),
        failed: progress.failed.cast_unsigned(),
        total_extraction_secs,
    });
}

/// Canonicalise the source directory, returning an error if it does not exist.
fn canonicalize_source(source_dir: &Path) -> Result<PathBuf, IngestPipelineError> {
    if !source_dir.is_dir() {
        return Err(IngestPipelineError::SourceNotFound(source_dir.to_owned()));
    }
    source_dir
        .canonicalize()
        .map_err(|source| IngestPipelineError::Canonicalize {
            path: source_dir.to_owned(),
            source,
        })
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use futures::stream::BoxStream;

    use super::*;
    use crate::extraction::{AUM_DISPLAY_PATH_KEY, ExtractionError, Extractor, RecordErrorFn};
    use crate::ingest::sink::{ExistenceChecker, NullSink};
    use crate::ingest::test_helpers::{TestChecker, make_pool, make_temp_tree, make_tracker};
    use crate::models::{Document, MetadataValue};

    // -- Mock extractors ----------------------------------------------------

    struct FixedExtractor {
        content: String,
        doc_count: usize,
    }

    impl Extractor for FixedExtractor {
        fn extract<'a>(
            &'a self,
            file_path: &'a Path,
            _record_error: Option<&'a RecordErrorFn>,
        ) -> BoxStream<'a, Result<Document, ExtractionError>> {
            let docs: Vec<Result<Document, ExtractionError>> = (0..self.doc_count)
                .map(|i| {
                    Ok(Document {
                        source_path: file_path.to_owned(),
                        content: format!("{}_{i}", self.content),
                        metadata: HashMap::new(),
                    })
                })
                .collect();
            Box::pin(futures::stream::iter(docs))
        }

        fn supports(&self, _mime_type: &str) -> bool {
            true
        }
    }

    struct FailExtractor;

    impl Extractor for FailExtractor {
        fn extract<'a>(
            &'a self,
            file_path: &'a Path,
            _record_error: Option<&'a RecordErrorFn>,
        ) -> BoxStream<'a, Result<Document, ExtractionError>> {
            let err = ExtractionError::Io {
                path: file_path.to_owned(),
                source: std::io::Error::other("test failure"),
            };
            Box::pin(futures::stream::once(async move { Err(err) }))
        }

        fn supports(&self, _mime_type: &str) -> bool {
            true
        }
    }

    struct EmptyExtractor;

    impl Extractor for EmptyExtractor {
        fn extract<'a>(
            &'a self,
            file_path: &'a Path,
            _record_error: Option<&'a RecordErrorFn>,
        ) -> BoxStream<'a, Result<Document, ExtractionError>> {
            let doc = Document {
                source_path: file_path.to_owned(),
                content: String::new(),
                metadata: HashMap::new(),
            };
            Box::pin(futures::stream::once(async move { Ok(doc) }))
        }

        fn supports(&self, _mime_type: &str) -> bool {
            true
        }
    }

    // -- Counting sink ------------------------------------------------------

    struct CountingSink {
        indexed: AtomicU64,
    }

    impl CountingSink {
        fn new() -> Self {
            Self {
                indexed: AtomicU64::new(0),
            }
        }

        fn indexed(&self) -> u64 {
            self.indexed.load(Ordering::Relaxed)
        }
    }

    #[async_trait::async_trait]
    impl BatchSink for CountingSink {
        async fn flush_batch(
            &self,
            _index: &str,
            _job_id: &str,
            batch: &[(String, Document)],
            _record_error: &RecordErrorFn,
        ) -> (u64, u64) {
            let count = batch.len() as u64;
            self.indexed.fetch_add(count, Ordering::Relaxed);
            (count, 0)
        }
    }

    // -- Flush counter sink -------------------------------------------------

    struct FlushCounter {
        count: Arc<AtomicU64>,
    }

    #[async_trait::async_trait]
    impl BatchSink for FlushCounter {
        async fn flush_batch(
            &self,
            _index: &str,
            _job_id: &str,
            batch: &[(String, Document)],
            _record_error: &RecordErrorFn,
        ) -> (u64, u64) {
            self.count.fetch_add(1, Ordering::Relaxed);
            (batch.len() as u64, 0)
        }
    }

    // -- Capture sink (doc_id, content) -------------------------------------

    struct CaptureContentSink {
        docs: Arc<Mutex<Vec<(String, String)>>>,
    }

    #[async_trait::async_trait]
    impl BatchSink for CaptureContentSink {
        async fn flush_batch(
            &self,
            _index: &str,
            _job_id: &str,
            batch: &[(String, Document)],
            _record_error: &RecordErrorFn,
        ) -> (u64, u64) {
            let mut guard = self.docs.lock().await;
            for (id, doc) in batch {
                guard.push((id.clone(), doc.content.clone()));
            }
            (batch.len() as u64, 0)
        }
    }

    // -- Capture sink (metadata) --------------------------------------------

    struct CaptureMetadataSink {
        docs: Arc<Mutex<Vec<HashMap<String, MetadataValue>>>>,
    }

    #[async_trait::async_trait]
    impl BatchSink for CaptureMetadataSink {
        async fn flush_batch(
            &self,
            _index: &str,
            _job_id: &str,
            batch: &[(String, Document)],
            _record_error: &RecordErrorFn,
        ) -> (u64, u64) {
            let mut guard = self.docs.lock().await;
            for (_, doc) in batch {
                guard.push(doc.metadata.clone());
            }
            (batch.len() as u64, 0)
        }
    }

    // -- Single-doc extractor that returns content = "text" -----------------

    struct MetaExtractor;

    impl Extractor for MetaExtractor {
        fn extract<'a>(
            &'a self,
            file_path: &'a Path,
            _record_error: Option<&'a RecordErrorFn>,
        ) -> BoxStream<'a, Result<Document, ExtractionError>> {
            let doc = Document {
                source_path: file_path.to_owned(),
                content: "text".to_owned(),
                metadata: HashMap::new(),
            };
            Box::pin(futures::stream::once(async move { Ok(doc) }))
        }

        fn supports(&self, _mime_type: &str) -> bool {
            true
        }
    }

    // -- Tests --------------------------------------------------------------

    #[tokio::test]
    async fn happy_path_ingest() -> anyhow::Result<()> {
        let dir = make_temp_tree(&["a.txt", "sub/b.txt", "sub/c.txt"])?;
        let pool = make_pool(FixedExtractor {
            content: "text".to_owned(),
            doc_count: 1,
        })?;
        let sink = Arc::new(CountingSink::new());
        let tracker = make_tracker().await?;

        let pipeline = IngestPipeline::new(
            pool,
            sink.clone(),
            tracker.clone(),
            "test".to_owned(),
            50,
            2,
        );

        let job = pipeline.run(dir.path()).await?;

        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.total_files, 3);
        assert_eq!(job.extracted, 3);
        assert_eq!(job.processed, 3);
        assert_eq!(job.failed, 0);
        assert_eq!(sink.indexed(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn extraction_failure_recorded() -> anyhow::Result<()> {
        let dir = make_temp_tree(&["bad.pdf"])?;
        let pool = make_pool(FailExtractor)?;
        let sink = Arc::new(NullSink);
        let tracker = make_tracker().await?;

        let pipeline = IngestPipeline::new(pool, sink, tracker.clone(), "test".to_owned(), 50, 1);

        let job = pipeline.run(dir.path()).await?;

        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.failed, 1);
        assert_eq!(job.processed, 0);
        Ok(())
    }

    #[tokio::test]
    async fn empty_extraction_counted() -> anyhow::Result<()> {
        let dir = make_temp_tree(&["empty.txt"])?;
        let pool = make_pool(EmptyExtractor)?;
        let sink = Arc::new(CountingSink::new());
        let tracker = make_tracker().await?;

        let pipeline = IngestPipeline::new(
            pool,
            sink.clone(),
            tracker.clone(),
            "test".to_owned(),
            50,
            1,
        );

        let job = pipeline.run(dir.path()).await?;

        assert_eq!(job.empty, 1);
        assert_eq!(job.processed, 1); // still indexed, just empty
        assert_eq!(sink.indexed(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn batch_size_respected() -> anyhow::Result<()> {
        // 5 files with batch_size=2 should produce 3 flushes (2, 2, 1).
        let dir = make_temp_tree(&["a.txt", "b.txt", "c.txt", "d.txt", "e.txt"])?;
        let pool = make_pool(FixedExtractor {
            content: "x".to_owned(),
            doc_count: 1,
        })?;

        let flush_count = Arc::new(AtomicU64::new(0));
        let sink = Arc::new(FlushCounter {
            count: flush_count.clone(),
        });
        let tracker = make_tracker().await?;

        let pipeline = IngestPipeline::new(pool, sink, tracker.clone(), "test".to_owned(), 2, 1);

        let job = pipeline.run(dir.path()).await?;

        assert_eq!(job.processed, 5);
        assert!(flush_count.load(Ordering::Relaxed) >= 2); // at least 2 flushes
        Ok(())
    }

    #[tokio::test]
    async fn retry_mode_processes_specific_files() -> anyhow::Result<()> {
        let dir = make_temp_tree(&["a.txt", "b.txt", "c.txt"])?;
        let pool = make_pool(FixedExtractor {
            content: "retry".to_owned(),
            doc_count: 1,
        })?;
        let sink = Arc::new(CountingSink::new());
        let tracker = make_tracker().await?;

        let pipeline = IngestPipeline::new(
            pool,
            sink.clone(),
            tracker.clone(),
            "test".to_owned(),
            50,
            2,
        );

        // Only retry a.txt and c.txt.
        let paths = vec![dir.path().join("a.txt"), dir.path().join("c.txt")];
        let job = pipeline.run_retry(paths, dir.path()).await?;

        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.total_files, 2);
        assert_eq!(job.processed, 2);
        assert_eq!(sink.indexed(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn resume_mode_skips_existing() -> anyhow::Result<()> {
        use anyhow::Context as _;

        let dir = make_temp_tree(&["a.txt", "b.txt", "c.txt"])?;

        // Pre-compute the doc ID for b.txt.
        let b_canonical = dir
            .path()
            .join("b.txt")
            .canonicalize()
            .context("canonicalize")?;
        let b_id = file_doc_id(&b_canonical, 0);

        let checker: Arc<dyn ExistenceChecker> =
            Arc::new(TestChecker(std::collections::HashSet::from([b_id])));

        let pool = make_pool(FixedExtractor {
            content: "resume".to_owned(),
            doc_count: 1,
        })?;
        let sink = Arc::new(CountingSink::new());
        let tracker = make_tracker().await?;

        let pipeline = IngestPipeline::new(
            pool,
            sink.clone(),
            tracker.clone(),
            "test".to_owned(),
            50,
            2,
        );

        let job = pipeline.run_resume(dir.path(), checker).await?;

        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.processed, 2, "b.txt should be skipped");
        assert_eq!(job.skipped, 1);
        assert_eq!(sink.indexed(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn nonexistent_source_dir_errors() -> anyhow::Result<()> {
        let pool = make_pool(FixedExtractor {
            content: "x".to_owned(),
            doc_count: 1,
        })?;
        let sink = Arc::new(NullSink);
        let tracker = make_tracker().await?;

        let pipeline = IngestPipeline::new(pool, sink, tracker, "test".to_owned(), 50, 1);

        let result = pipeline.run(Path::new("/nonexistent_dir_xyz")).await;
        assert!(result.is_err());
        Ok(())
    }

    #[tokio::test]
    async fn multi_doc_extraction_generates_unique_ids() -> anyhow::Result<()> {
        let dir = make_temp_tree(&["archive.zip"])?;
        let pool = make_pool(FixedExtractor {
            content: "part".to_owned(),
            doc_count: 3, // container + 2 embedded
        })?;

        let docs_received = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::new(CaptureContentSink {
            docs: docs_received.clone(),
        });
        let tracker = make_tracker().await?;

        let pipeline = IngestPipeline::new(pool, sink, tracker.clone(), "test".to_owned(), 50, 1);

        let job = pipeline.run(dir.path()).await?;

        assert_eq!(job.extracted, 1); // 1 file extracted
        assert_eq!(job.processed, 3); // 3 documents produced

        let docs = docs_received.lock().await;
        assert_eq!(docs.len(), 3);

        // All doc IDs should be unique.
        let ids: std::collections::HashSet<&str> = docs.iter().map(|(id, _)| id.as_str()).collect();
        assert_eq!(ids.len(), 3, "all doc IDs must be unique");
        Ok(())
    }

    #[tokio::test]
    async fn display_paths_set_on_documents() -> anyhow::Result<()> {
        let dir = make_temp_tree(&["sub/doc.txt"])?;

        let docs_received = Arc::new(Mutex::new(Vec::new()));

        let pool = make_pool(MetaExtractor)?;
        let sink = Arc::new(CaptureMetadataSink {
            docs: docs_received.clone(),
        });
        let tracker = make_tracker().await?;

        let pipeline = IngestPipeline::new(pool, sink, tracker.clone(), "test".to_owned(), 50, 1);

        pipeline.run(dir.path()).await?;

        let docs = docs_received.lock().await;
        assert_eq!(docs.len(), 1);
        assert_eq!(
            docs[0].get(AUM_DISPLAY_PATH_KEY),
            Some(&MetadataValue::Single("sub/doc.txt".to_owned()))
        );
        Ok(())
    }
}
