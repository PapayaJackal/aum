//! Abstract repository traits for the aum database layer.
//!
//! These traits decouple business logic from the underlying database driver.
//! The concrete sqlx implementations live in the sibling `jobs`, `errors`, and
//! `embeddings` modules. Tests can supply in-memory or mock implementations
//! without touching real database state.

use std::path::Path;

use async_trait::async_trait;
use futures::stream::BoxStream;

use crate::models::{IngestError, IngestJob, JobProgress, JobStatus, JobType};

use super::error::DbResult;

// ---------------------------------------------------------------------------
// JobRepository
// ---------------------------------------------------------------------------

/// Persistent storage for [`IngestJob`] records.
#[async_trait]
pub trait JobRepository: Send + Sync {
    /// Insert a new job row and return the created record.
    async fn create_job(
        &self,
        job_id: &str,
        source_dir: &Path,
        index_name: &str,
        job_type: JobType,
        total_files: i64,
    ) -> DbResult<IngestJob>;

    /// Atomically overwrite all progress counters for a job.
    async fn update_progress(&self, job_id: &str, progress: &JobProgress) -> DbResult<()>;

    /// Mark a job as terminal, setting `finished_at` to the current UTC time.
    async fn complete_job(&self, job_id: &str, status: JobStatus) -> DbResult<()>;

    /// Fetch a single job by ID.
    ///
    /// When `include_errors` is `true` the returned job's `errors` field is
    /// populated from the `job_errors` table; otherwise it is left empty.
    async fn get_job(&self, job_id: &str, include_errors: bool) -> DbResult<Option<IngestJob>>;

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

    /// Delete all jobs (and their errors) for a given index.
    ///
    /// Returns the number of job rows deleted.
    async fn clear_index(&self, index_name: &str) -> DbResult<u64>;
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

    /// Return the distinct file paths that have errors for a job.
    ///
    /// - `exclude_type`: skip errors of this type.
    /// - `only_type`: return only errors of this type.
    async fn get_failed_paths(
        &self,
        job_id: &str,
        exclude_type: Option<&str>,
        only_type: Option<&str>,
    ) -> DbResult<Vec<std::path::PathBuf>>;
}

// ---------------------------------------------------------------------------
// IndexEmbeddingRepository
// ---------------------------------------------------------------------------

/// Persistent storage for per-index embedding model metadata.
#[async_trait]
pub trait IndexEmbeddingRepository: Send + Sync {
    /// Retrieve the embedding model metadata for an index.
    ///
    /// Returns `(model, backend, dimension)` or `None` if no record exists.
    async fn get_embedding_model(
        &self,
        index_name: &str,
    ) -> DbResult<Option<(String, String, i64)>>;

    /// Upsert the embedding model metadata for an index.
    async fn set_embedding_model(
        &self,
        index_name: &str,
        model: &str,
        backend: &str,
        dimension: i64,
    ) -> DbResult<()>;

    /// Remove the embedding model record for an index, if it exists.
    async fn clear_embedding_model(&self, index_name: &str) -> DbResult<()>;
}
