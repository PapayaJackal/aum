//! Abstract repository traits for the aum database layer.
//!
//! These traits decouple business logic from the underlying database driver.
//! The concrete sqlx implementations live in the sibling `jobs`, `errors`, and
//! `embeddings` modules. Tests can supply in-memory or mock implementations
//! without touching real database state.

use std::path::{Path, PathBuf};

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::models::{
    EmbeddingModelInfo, ErrorFilter, IngestError, IngestJob, JobProgress, JobStatus, JobType,
    OcrSettings,
};

use super::error::DbResult;

// ---------------------------------------------------------------------------
// JobRepository
// ---------------------------------------------------------------------------

/// Persistent storage for [`IngestJob`] records.
#[async_trait]
pub trait JobRepository: Send + Sync {
    /// Insert a new job row and return the created record.
    ///
    /// `ocr_settings` records the effective OCR configuration used for
    /// extraction so that `aum retry` can detect when it changes.
    async fn create_job(
        &self,
        job_id: &str,
        source_dir: &Path,
        index_name: &str,
        job_type: JobType,
        total_files: i64,
        ocr_settings: Option<OcrSettings>,
    ) -> DbResult<IngestJob>;

    /// Atomically overwrite all progress counters for a job.
    async fn update_progress(&self, job_id: &str, progress: &JobProgress) -> DbResult<()>;

    /// Mark a job as terminal, setting `finished_at` to the current UTC time.
    async fn complete_job(&self, job_id: &str, status: JobStatus) -> DbResult<()>;

    /// Fetch a single job by ID.
    async fn get_job(&self, job_id: &str) -> DbResult<Option<IngestJob>>;

    /// Stream all jobs ordered by `created_at DESC`, optionally filtered by status.
    ///
    /// Uses a database cursor so the full result set is never held in memory.
    fn list_jobs(&self, status: Option<JobStatus>) -> BoxStream<'_, DbResult<IngestJob>>;

    /// Return the most recent running job for the given type and optional directory.
    ///
    /// Used to find a job that can be resumed after an interruption.
    async fn find_resumable_job(
        &self,
        source_dir: Option<&Path>,
        job_type: JobType,
    ) -> DbResult<Option<IngestJob>>;

    /// Update just the `total_files` count for a job.
    ///
    /// Called periodically by the file walker as new files are discovered,
    /// without overwriting the progress counters.
    async fn update_total_files(&self, job_id: &str, total_files: i64) -> DbResult<()>;

    /// Delete all jobs (and their errors) for a given index.
    ///
    /// Returns the number of job rows deleted.
    async fn clear_index(&self, index_name: &str) -> DbResult<u64>;

    /// Return the distinct source directories used by all ingest jobs for the
    /// given index.
    ///
    /// Used for path-containment checks when serving downloaded or previewed
    /// files to prevent symlink traversal outside the dataset directory.
    async fn get_source_dirs_for_index(&self, index_name: &str) -> DbResult<Vec<PathBuf>>;
}

// ---------------------------------------------------------------------------
// JobErrorRepository
// ---------------------------------------------------------------------------

/// Persistent storage for per-file [`IngestError`] records.
#[async_trait]
pub trait JobErrorRepository: Send + Sync {
    /// Record an error for a file within a job.
    ///
    /// Silently ignores duplicate `(job_id, file_path, error_type)` tuples
    /// via `INSERT OR IGNORE` / `ON CONFLICT DO NOTHING`.
    async fn record_error(
        &self,
        job_id: &str,
        file_path: &Path,
        error_type: &str,
        message: &str,
    ) -> DbResult<()>;

    /// Stream all errors for a job ordered by `timestamp`.
    fn list_errors<'a>(&'a self, job_id: &str) -> BoxStream<'a, DbResult<IngestError>>;

    /// Stream the distinct `file_path` values from errors for a job as raw
    /// strings.
    ///
    /// Used for embed jobs where `file_path` stores document ID hashes
    /// rather than filesystem paths.
    fn get_failed_doc_ids<'a>(&'a self, job_id: &str) -> BoxStream<'a, DbResult<String>>;

    /// Stream the distinct file paths that have errors for a job.
    fn get_failed_paths<'a>(
        &'a self,
        job_id: &str,
        filter: ErrorFilter<'_>,
    ) -> BoxStream<'a, DbResult<std::path::PathBuf>>;
}

// ---------------------------------------------------------------------------
// IndexEmbeddingRepository
// ---------------------------------------------------------------------------

/// Persistent storage for per-index embedding model metadata.
#[async_trait]
pub trait IndexEmbeddingRepository: Send + Sync {
    /// Retrieve the embedding model metadata for an index.
    async fn get_embedding_model(&self, index_name: &str) -> DbResult<Option<EmbeddingModelInfo>>;

    /// Upsert the embedding model metadata for an index.
    async fn set_embedding_model(
        &self,
        index_name: &str,
        info: &EmbeddingModelInfo,
    ) -> DbResult<()>;

    /// Remove the embedding model record for an index, if it exists.
    async fn clear_embedding_model(&self, index_name: &str) -> DbResult<()>;
}
