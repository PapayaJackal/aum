//! Embed pipeline orchestrator.
//!
//! [`EmbedPipeline`] streams un-embedded (or specific) documents from the
//! search backend, chunks their content, embeds the chunks in parallel using
//! an [`InstancePool`] of [`Embedder`]s, and writes the resulting vectors back
//! to the backend.  Progress is emitted over a [`tokio::sync::watch`] channel
//! so callers can render a live display.
//!
//! The pipeline is structured as three channel-connected stages (matching the
//! ingest pipeline's architecture):
//!
//! 1. **Scroll source** — a spawned task drains the scroll stream, chunks each
//!    document's content, and sends `(doc_id, chunks, display_path)` items
//!    through an [`mpsc`] channel.
//! 2. **Embed dispatcher** — a spawned task reads from the channel and spawns
//!    one embed task per document.  A [`Semaphore`] caps concurrency so that a
//!    new embed starts as soon as *any* in-flight embed completes — no batch
//!    boundaries, no head-of-line blocking.
//! 3. **Batcher/writer** — the main task receives embed results through a
//!    second [`mpsc`] channel, accumulates them into write batches, and spawns
//!    background flush tasks (limited by a flush semaphore) so that writing
//!    never blocks embedding.

use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::db::{DbError, JobTracker};
use crate::models::{IngestJob, JobProgress, JobStatus, JobType};
use crate::names::generate_name;
use crate::pool::InstancePool;
use crate::search::{AumBackend, SearchBackend as _, SearchError};

use crate::ingest::InFlightState;

use super::backend::Embedder;
use super::chunking::chunk_text;

// ---------------------------------------------------------------------------
// Progress snapshot
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of embed pipeline counters.
#[derive(Debug, Clone, Default)]
pub struct EmbedSnapshot {
    /// Total documents to embed.
    pub total: u64,
    /// Documents successfully embedded so far.
    pub embedded: u64,
    /// Documents that failed to embed.
    pub failed: u64,
    /// Number of documents currently being embedded.
    pub in_flight: u64,
    /// Display paths currently in flight (populated only when `debug` is enabled).
    pub in_flight_paths: Vec<String>,
}

/// Sender half of the embed progress watch channel.
pub type EmbedProgressTx = tokio::sync::watch::Sender<EmbedSnapshot>;

// ---------------------------------------------------------------------------
// Error
// ---------------------------------------------------------------------------

/// Errors that can occur during an embed pipeline run.
#[derive(Debug, thiserror::Error)]
pub enum EmbedPipelineError {
    /// A database operation failed.
    #[error("database error: {0}")]
    Db(#[from] DbError),

    /// A search backend operation failed.
    #[error("search error: {0}")]
    Search(#[from] SearchError),
}

// ---------------------------------------------------------------------------
// Internal message types
// ---------------------------------------------------------------------------

/// A document ready for embedding, sent from the scroll source to the
/// embed dispatcher.
struct EmbedItem {
    doc_id: String,
    chunks: Vec<String>,
    display_path: String,
}

/// Result of a single document's embedding, sent from the embed dispatcher
/// to the batcher.
enum EmbedResult {
    Ok {
        doc_id: String,
        vectors: Vec<Vec<f32>>,
    },
    /// Error already recorded in the dispatcher; batcher just needs to know
    /// it happened.
    Err,
}

/// Result of a background flush (`update_embeddings`).
struct FlushResult {
    failed: u64,
}

/// Maximum concurrent backend writes.  Matches the ingest pipeline's
/// `MAX_PENDING_FLUSHES`.
const MAX_PENDING_FLUSHES: usize = 2;

// ---------------------------------------------------------------------------
// Pipeline
// ---------------------------------------------------------------------------

/// Orchestrates streaming un-embedded documents, chunking, parallel embedding,
/// and updating the search backend with the resulting vectors.
pub struct EmbedPipeline {
    backend: Arc<AumBackend>,
    pool: Arc<InstancePool<Box<dyn Embedder>>>,
    tracker: JobTracker,
    index_name: Arc<str>,
    batch_size: usize,
    max_chunk_chars: usize,
    overlap_chars: usize,
    progress_tx: Option<EmbedProgressTx>,
    debug: bool,
}

impl EmbedPipeline {
    /// Create a new pipeline.
    ///
    /// * `max_chunk_chars` — maximum characters per chunk (`context_length * 4`)
    /// * `overlap_chars` — overlap between adjacent chunks (`chunk_overlap`)
    #[must_use]
    pub fn new(
        backend: Arc<AumBackend>,
        pool: Arc<InstancePool<Box<dyn Embedder>>>,
        tracker: JobTracker,
        index_name: impl Into<Arc<str>>,
        batch_size: usize,
        max_chunk_chars: usize,
        overlap_chars: usize,
    ) -> Self {
        Self {
            backend,
            pool,
            tracker,
            index_name: index_name.into(),
            batch_size,
            max_chunk_chars,
            overlap_chars,
            progress_tx: None,
            debug: false,
        }
    }

    /// Attach a progress channel.
    #[must_use]
    pub fn with_progress(mut self, tx: EmbedProgressTx) -> Self {
        self.progress_tx = Some(tx);
        self
    }

    /// Enable debug mode: populate `in_flight_paths` in each [`EmbedSnapshot`].
    #[must_use]
    pub fn with_debug(mut self) -> Self {
        self.debug = true;
        self
    }

    /// Embed all documents that do not yet have embedding vectors.
    ///
    /// # Errors
    ///
    /// Returns [`EmbedPipelineError`] on database or search backend failures.
    pub async fn run(&self) -> Result<IngestJob, EmbedPipelineError> {
        let total = self.backend.count_unembedded(&self.index_name).await?;
        if total == 0 {
            info!(index = %self.index_name, "all documents already embedded");
        }

        let stream = self
            .backend
            .scroll_unembedded(&self.index_name, self.batch_size);
        self.run_stream(total, stream).await
    }

    /// Embed specific documents by their IDs (for retry after failures).
    ///
    /// # Errors
    ///
    /// Returns [`EmbedPipelineError`] on database or search backend failures.
    pub async fn run_for_doc_ids(
        &self,
        doc_ids: Vec<String>,
    ) -> Result<IngestJob, EmbedPipelineError> {
        let total = doc_ids.len() as u64;
        let stream = self
            .backend
            .scroll_documents(&self.index_name, &doc_ids, self.batch_size);
        self.run_stream(total, stream).await
    }

    /// Core pipeline: create the job record, run the three-stage pipeline,
    /// finalise the job.
    async fn run_stream(
        &self,
        total: u64,
        stream: impl futures::Stream<
            Item = Result<Vec<crate::search::types::SearchResult>, SearchError>,
        > + Unpin
        + Send
        + 'static,
    ) -> Result<IngestJob, EmbedPipelineError> {
        let job_id = generate_name();
        let index = &*self.index_name;

        info!(job_id, index, total, "starting embed job");
        #[expect(
            clippy::cast_possible_wrap,
            reason = "document counts do not exceed i64::MAX"
        )]
        let total_i64 = total as i64;
        let job = self
            .tracker
            .create_job(
                &job_id,
                Path::new("."),
                index,
                JobType::Embed,
                total_i64,
                None,
            )
            .await?;

        let result = self.execute_pipeline(&job_id, total, stream).await;

        let final_status = if result.is_err() {
            JobStatus::Failed
        } else {
            JobStatus::Completed
        };

        if let Err(ref e) = result {
            error!(job_id, error = %e, "embed job failed");
        }

        self.tracker.complete_job(&job_id, final_status).await?;

        let completed = self.tracker.get_job(&job_id, false).await?.unwrap_or(job);

        result.map(|()| completed)
    }

    /// Execute the three-stage pipeline: scroll source → embed dispatcher →
    /// batcher/writer.
    async fn execute_pipeline(
        &self,
        job_id: &str,
        total: u64,
        stream: impl futures::Stream<
            Item = Result<Vec<crate::search::types::SearchResult>, SearchError>,
        > + Unpin
        + Send
        + 'static,
    ) -> Result<(), EmbedPipelineError> {
        let embedded = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicU64::new(0));
        let in_flight_state = InFlightState::new(self.debug);

        // Stage 1 → Stage 2 channel: scroll source sends chunked docs to the
        // embed dispatcher.
        let (item_tx, item_rx) = mpsc::channel::<EmbedItem>(self.batch_size * 2);

        // Stage 2 → Stage 3 channel: embed dispatcher sends results to the
        // batcher/writer.
        let (result_tx, result_rx) = mpsc::channel::<EmbedResult>(self.batch_size * 2);

        // --- Stage 1: Scroll source ---
        let max_chunk_chars = self.max_chunk_chars;
        let overlap_chars = self.overlap_chars;
        let source_handle = tokio::spawn(async move {
            Self::scroll_source(stream, item_tx, max_chunk_chars, overlap_chars).await;
        });

        // --- Stage 2: Embed dispatcher ---
        let pool = Arc::clone(&self.pool);
        let tracker = self.tracker.clone();
        let job_id_owned = job_id.to_owned();
        let in_flight_clone = in_flight_state.clone();
        let failed_clone = Arc::clone(&failed);
        let dispatcher_handle = tokio::spawn(async move {
            Self::embed_dispatcher(
                pool,
                item_rx,
                result_tx,
                tracker,
                &job_id_owned,
                in_flight_clone,
                failed_clone,
            )
            .await;
        });

        // --- Stage 3: Batcher/writer (runs on the current task) ---
        let result = self
            .batcher_loop(
                job_id,
                total,
                result_rx,
                &embedded,
                &failed,
                &in_flight_state,
            )
            .await;

        // Wait for upstream stages.
        if let Err(e) = dispatcher_handle.await {
            warn!(error = %e, "embed dispatcher panicked");
        }
        if let Err(e) = source_handle.await {
            warn!(error = %e, "scroll source panicked");
        }

        result
    }

    // -----------------------------------------------------------------------
    // Stage 1: Scroll source
    // -----------------------------------------------------------------------

    /// Drain the scroll stream, chunk each document, and send items to the
    /// embed dispatcher through the channel.
    async fn scroll_source(
        mut stream: impl futures::Stream<
            Item = Result<Vec<crate::search::types::SearchResult>, SearchError>,
        > + Unpin,
        tx: mpsc::Sender<EmbedItem>,
        max_chunk_chars: usize,
        overlap_chars: usize,
    ) {
        use futures::StreamExt as _;

        while let Some(batch_result) = stream.next().await {
            let batch = match batch_result {
                Ok(b) => b,
                Err(e) => {
                    error!(error = %e, "scroll source error");
                    break;
                }
            };
            for doc in batch {
                let chunks = chunk_text(&doc.snippet, max_chunk_chars, overlap_chars);
                let item = EmbedItem {
                    doc_id: doc.doc_id,
                    chunks,
                    display_path: doc.display_path,
                };
                if tx.send(item).await.is_err() {
                    return; // downstream closed
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Stage 2: Embed dispatcher
    // -----------------------------------------------------------------------

    /// Read items from the channel and spawn one embed task per document.
    ///
    /// A semaphore caps concurrency so that a new embed starts as soon as any
    /// in-flight embed completes — no batch boundaries, no head-of-line
    /// blocking.
    async fn embed_dispatcher(
        pool: Arc<InstancePool<Box<dyn Embedder>>>,
        mut rx: mpsc::Receiver<EmbedItem>,
        result_tx: mpsc::Sender<EmbedResult>,
        tracker: JobTracker,
        job_id: &str,
        in_flight: InFlightState,
        embed_failed: Arc<AtomicU64>,
    ) {
        let max_workers = pool.total_concurrency() as usize;
        let semaphore = Arc::new(tokio::sync::Semaphore::new(max_workers));

        while let Some(item) = rx.recv().await {
            let Ok(permit) = Arc::clone(&semaphore).acquire_owned().await else {
                break;
            };

            let pool = Arc::clone(&pool);
            let result_tx = result_tx.clone();
            let in_flight = in_flight.clone();
            let tracker = tracker.clone();
            let job_id = job_id.to_owned();
            let embed_failed = Arc::clone(&embed_failed);

            tokio::spawn(async move {
                in_flight.add_path(&item.display_path);

                let outcome = pool.run_dyn(move |e| e.embed_documents(item.chunks)).await;

                let msg = match outcome {
                    Ok(vectors) => EmbedResult::Ok {
                        doc_id: item.doc_id,
                        vectors,
                    },
                    Err(e) => {
                        error!(doc_id = %item.doc_id, error = %e, "embedding failed");
                        embed_failed.fetch_add(1, Ordering::Relaxed);
                        if let Err(db_err) = tracker
                            .record_error(
                                &job_id,
                                Path::new(&item.doc_id),
                                "EmbeddingError",
                                &e.to_string(),
                            )
                            .await
                        {
                            warn!(error = %db_err, "failed to record embed error");
                        }
                        EmbedResult::Err
                    }
                };

                in_flight.remove_path(&item.display_path);

                let _ = result_tx.send(msg).await;
                drop(permit);
            });
        }

        // Wait for all in-flight embeds to finish.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "max_workers is bounded by pool config"
        )]
        let _ = semaphore.acquire_many(max_workers as u32).await;

        debug!("embed dispatcher exiting, all embeds complete");
    }

    // -----------------------------------------------------------------------
    // Stage 3: Batcher / writer
    // -----------------------------------------------------------------------

    /// Receive embed results, batch them, and flush to the backend.
    ///
    /// Flushes run in background tasks (bounded by a semaphore) so that
    /// writing never blocks embedding.
    async fn batcher_loop(
        &self,
        job_id: &str,
        total: u64,
        mut result_rx: mpsc::Receiver<EmbedResult>,
        embedded: &Arc<AtomicU64>,
        failed: &Arc<AtomicU64>,
        in_flight_state: &InFlightState,
    ) -> Result<(), EmbedPipelineError> {
        let job_start = Instant::now();
        let flush_sem = Arc::new(tokio::sync::Semaphore::new(MAX_PENDING_FLUSHES));
        let (flush_tx, mut flush_rx) = mpsc::channel::<FlushResult>(MAX_PENDING_FLUSHES);

        let mut batch: Vec<(String, Vec<Vec<f32>>)> = Vec::with_capacity(self.batch_size);
        let mut in_flight_flushes: usize = 0;

        while let Some(result) = result_rx.recv().await {
            match result {
                EmbedResult::Ok { doc_id, vectors } => {
                    batch.push((doc_id, vectors));
                    // Update immediately so the progress bar reflects each
                    // document as soon as its embedding completes, rather than
                    // waiting for the next batch flush.
                    embedded.fetch_add(1, Ordering::Relaxed);
                }
                EmbedResult::Err => {
                    // Error already recorded in the dispatcher.
                }
            }

            // Flush when the batch is full.
            if batch.len() >= self.batch_size {
                // Drain completed flushes (non-blocking).
                self.drain_flush_failures(&mut flush_rx, &mut in_flight_flushes, embedded, failed);

                let Ok(permit) = Arc::clone(&flush_sem).acquire_owned().await else {
                    unreachable!("flush semaphore closed unexpectedly");
                };
                let full_batch = std::mem::replace(&mut batch, Vec::with_capacity(self.batch_size));
                self.spawn_flush(full_batch, flush_tx.clone(), permit);
                in_flight_flushes += 1;
            }

            // Emit progress.
            self.emit_progress(total, embedded, failed, in_flight_state);
        }

        // Drain remaining completed flushes.
        self.drain_flush_failures(&mut flush_rx, &mut in_flight_flushes, embedded, failed);

        // Flush any remaining documents inline.
        if !batch.is_empty() {
            let result = Self::flush_inner(&self.backend, &self.index_name, &batch).await;
            Self::fold_flush_failures(&result, embedded, failed);
        }

        // Wait for in-flight background flushes.
        for _ in 0..in_flight_flushes {
            if let Some(r) = flush_rx.recv().await {
                Self::fold_flush_failures(&r, embedded, failed);
            }
        }

        // Persist final progress.
        let emb = embedded.load(Ordering::Relaxed);
        let fail = failed.load(Ordering::Relaxed);
        #[expect(
            clippy::cast_possible_wrap,
            reason = "document counts do not exceed i64::MAX"
        )]
        let progress = JobProgress {
            processed: emb as i64,
            failed: fail as i64,
            ..Default::default()
        };
        let _ = self.tracker.update_progress(job_id, &progress).await;

        let elapsed = job_start.elapsed().as_secs_f64();
        #[expect(clippy::cast_precision_loss, reason = "approximate rate display")]
        let rate = if elapsed > 0.0 {
            emb as f64 / elapsed
        } else {
            0.0
        };
        info!(
            job_id,
            index = %self.index_name,
            embedded = emb,
            failed = fail,
            total,
            rate = format!("{rate:.1} docs/s"),
            "embed job finished"
        );

        self.emit_progress(total, embedded, failed, in_flight_state);

        Ok(())
    }

    /// Spawn a background flush task.
    fn spawn_flush(
        &self,
        batch: Vec<(String, Vec<Vec<f32>>)>,
        result_tx: mpsc::Sender<FlushResult>,
        permit: tokio::sync::OwnedSemaphorePermit,
    ) {
        let backend = Arc::clone(&self.backend);
        let index_name = Arc::clone(&self.index_name);

        tokio::spawn(async move {
            let result = Self::flush_inner(&backend, &index_name, &batch).await;
            let _ = result_tx.send(result).await;
            drop(permit);
        });
    }

    /// Execute a single backend write.
    async fn flush_inner(
        backend: &AumBackend,
        index_name: &str,
        batch: &[(String, Vec<Vec<f32>>)],
    ) -> FlushResult {
        match backend.update_embeddings(index_name, batch).await {
            Ok(n_failed) => FlushResult { failed: n_failed },
            Err(e) => {
                error!(error = %e, "update_embeddings failed");
                FlushResult {
                    failed: batch.len() as u64,
                }
            }
        }
    }

    /// Non-blocking drain of completed flush results, adjusting counters for
    /// any write failures (embedded count was already incremented optimistically).
    #[expect(
        clippy::unused_self,
        reason = "method is logically part of the pipeline"
    )]
    fn drain_flush_failures(
        &self,
        rx: &mut mpsc::Receiver<FlushResult>,
        in_flight: &mut usize,
        embedded: &AtomicU64,
        failed: &AtomicU64,
    ) {
        while let Ok(result) = rx.try_recv() {
            Self::fold_flush_failures(&result, embedded, failed);
            *in_flight -= 1;
        }
    }

    /// Adjust counters for flush failures.  Since `embedded` is incremented
    /// optimistically when each document completes embedding, flush failures
    /// must subtract from `embedded` and add to `failed`.
    fn fold_flush_failures(result: &FlushResult, embedded: &AtomicU64, failed: &AtomicU64) {
        if result.failed > 0 {
            embedded.fetch_sub(result.failed, Ordering::Relaxed);
            failed.fetch_add(result.failed, Ordering::Relaxed);
        }
    }

    /// Emit a progress snapshot.
    fn emit_progress(
        &self,
        total: u64,
        embedded: &AtomicU64,
        failed: &AtomicU64,
        in_flight_state: &InFlightState,
    ) {
        let Some(ref tx) = self.progress_tx else {
            return;
        };
        let (in_flight_count, in_flight_paths) = in_flight_state.snapshot();
        tx.send_replace(EmbedSnapshot {
            total,
            embedded: embedded.load(Ordering::Relaxed),
            failed: failed.load(Ordering::Relaxed),
            in_flight: in_flight_count,
            in_flight_paths,
        });
    }
}
