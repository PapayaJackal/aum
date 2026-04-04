//! Unified job-tracking facade.
//!
//! [`JobTracker`] bundles the three repository traits into a single handle
//! that the ingest/embed pipeline passes around.  It adds application-level
//! logging and metrics on top of the per-query instrumentation already
//! present in the repository implementations.

use std::path::{Path, PathBuf};

use futures::TryStreamExt as _;
use futures::stream::BoxStream;
use sqlx::AnyPool;
use tracing::{info, instrument, warn};

use crate::ingest::lock;
use crate::models::{
    EmbeddingModelInfo, ErrorFilter, IngestError, IngestJob, JobProgress, JobStatus, JobType,
    OcrSettings,
};

use super::error::DbResult;
use super::repository::{IndexEmbeddingRepository, JobErrorRepository, JobRepository};
use super::{SqlxIndexEmbeddingRepository, SqlxJobErrorRepository, SqlxJobRepository};

/// Unified facade over [`JobRepository`], [`JobErrorRepository`], and
/// [`IndexEmbeddingRepository`].
///
/// Cheaply cloneable (all interior data is `Arc`-backed via [`AnyPool`]).
#[derive(Clone)]
pub struct JobTracker {
    jobs: SqlxJobRepository,
    errors: SqlxJobErrorRepository,
    embeddings: SqlxIndexEmbeddingRepository,
}

impl JobTracker {
    /// Create a new tracker backed by the given connection pool.
    #[must_use]
    pub fn new(pool: AnyPool) -> Self {
        Self {
            jobs: SqlxJobRepository::new(pool.clone()),
            errors: SqlxJobErrorRepository::new(pool.clone()),
            embeddings: SqlxIndexEmbeddingRepository::new(pool),
        }
    }

    // --- Job lifecycle -------------------------------------------------------

    /// Insert a new job row and return the created record.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the insert or subsequent fetch fails.
    #[instrument(skip(self))]
    pub async fn create_job(
        &self,
        job_id: &str,
        source_dir: &Path,
        index_name: &str,
        job_type: JobType,
        total_files: i64,
        ocr_settings: Option<OcrSettings>,
    ) -> DbResult<IngestJob> {
        self.jobs
            .create_job(
                job_id,
                source_dir,
                index_name,
                job_type,
                total_files,
                ocr_settings,
            )
            .await
    }

    /// Update just the `total_files` count for a job.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the update fails.
    #[instrument(skip(self))]
    pub async fn update_total_files(&self, job_id: &str, total_files: i64) -> DbResult<()> {
        self.jobs.update_total_files(job_id, total_files).await
    }

    /// Atomically overwrite all progress counters for a job.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the update fails.
    #[instrument(skip(self))]
    pub async fn update_progress(&self, job_id: &str, progress: &JobProgress) -> DbResult<()> {
        self.jobs.update_progress(job_id, progress).await
    }

    /// Mark a job as terminal, setting `finished_at` to the current UTC time.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the update fails.
    #[instrument(skip(self))]
    pub async fn complete_job(&self, job_id: &str, status: JobStatus) -> DbResult<()> {
        info!(job_id, ?status, "job finished");
        self.jobs.complete_job(job_id, status).await
    }

    /// Check all non-terminal jobs and mark those whose flock is no longer
    /// held as interrupted.
    ///
    /// Should be called once at startup so that jobs left in-progress by a
    /// crashed process are correctly shown as interrupted, while jobs that are
    /// genuinely still running (held by another process) are left alone.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the database queries fail.
    #[instrument(skip(self))]
    pub async fn mark_stale_jobs_interrupted(&self, lock_dir: &Path) -> DbResult<u64> {
        let mut count = 0u64;

        for status in [JobStatus::Running, JobStatus::Pending] {
            let active: Vec<IngestJob> = self.jobs.list_jobs(Some(status)).try_collect().await?;
            for job in active {
                let locked = match job.job_type {
                    JobType::Ingest => lock::is_locked(lock_dir, &job.source_dir),
                    JobType::Embed => lock::embed_is_locked(lock_dir, &job.index_name),
                };
                if !locked {
                    info!(
                        job_id = %job.job_id,
                        job_type = ?job.job_type,
                        "marking stale job as interrupted (lock not held)"
                    );
                    self.jobs
                        .complete_job(&job.job_id, JobStatus::Interrupted)
                        .await?;
                    count += 1;
                }
            }
        }

        if count > 0 {
            info!(count, "marked stale jobs as interrupted");
        }
        Ok(count)
    }

    /// Fetch a single job by ID, optionally including its errors.
    ///
    /// When `include_errors` is `true` the returned job's `errors` field is
    /// populated; otherwise it is left empty.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the query fails.
    #[instrument(skip(self))]
    pub async fn get_job(&self, job_id: &str, include_errors: bool) -> DbResult<Option<IngestJob>> {
        let Some(mut job) = self.jobs.get_job(job_id).await? else {
            return Ok(None);
        };

        if include_errors {
            job.errors = self.errors.list_errors(job_id).try_collect().await?;
        }

        Ok(Some(job))
    }

    /// Stream all jobs ordered by `created_at DESC`, optionally filtered by status.
    #[must_use]
    pub fn list_jobs(&self, status: Option<JobStatus>) -> BoxStream<'_, DbResult<IngestJob>> {
        self.jobs.list_jobs(status)
    }

    /// Return the most recent interrupted job for the given type and optional directory.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the query fails.
    #[instrument(skip(self))]
    pub async fn find_resumable_job(
        &self,
        source_dir: Option<&Path>,
        job_type: JobType,
    ) -> DbResult<Option<IngestJob>> {
        self.jobs.find_resumable_job(source_dir, job_type).await
    }

    // --- Error tracking ------------------------------------------------------

    /// Record an error for a file within a job.
    ///
    /// Emits a warning log and increments the `aum_job_errors_total` counter.
    /// Silently ignores duplicate `(job_id, file_path, error_type)` tuples.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the insert fails.
    #[instrument(skip(self))]
    pub async fn record_error(
        &self,
        job_id: &str,
        file_path: &Path,
        error_type: &str,
        message: &str,
    ) -> DbResult<()> {
        warn!(job_id, ?file_path, error_type, message, "ingest error");
        metrics::counter!("aum_job_errors_total", "error_type" => error_type.to_owned())
            .increment(1);
        self.errors
            .record_error(job_id, file_path, error_type, message)
            .await
    }

    /// Stream all errors for a job ordered by `timestamp`.
    #[must_use]
    pub fn list_errors<'a>(&'a self, job_id: &str) -> BoxStream<'a, DbResult<IngestError>> {
        self.errors.list_errors(job_id)
    }

    /// Stream distinct `file_path` values from errors as raw strings.
    ///
    /// Used for embed jobs where `file_path` stores document ID hashes.
    #[must_use]
    pub fn get_failed_doc_ids<'a>(&'a self, job_id: &str) -> BoxStream<'a, DbResult<String>> {
        self.errors.get_failed_doc_ids(job_id)
    }

    /// Stream the distinct file paths that have errors for a job.
    #[must_use]
    pub fn get_failed_paths<'a>(
        &'a self,
        job_id: &str,
        filter: ErrorFilter<'_>,
    ) -> BoxStream<'a, DbResult<PathBuf>> {
        self.errors.get_failed_paths(job_id, filter)
    }

    // --- Index management ----------------------------------------------------

    /// Delete all jobs (and their errors) for a given index.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the delete fails.
    #[instrument(skip(self))]
    pub async fn clear_index(&self, index_name: &str) -> DbResult<u64> {
        self.jobs.clear_index(index_name).await
    }

    // --- Embedding model tracking --------------------------------------------

    /// Retrieve the embedding model metadata for an index.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the query fails.
    #[instrument(skip(self))]
    pub async fn get_embedding_model(
        &self,
        index_name: &str,
    ) -> DbResult<Option<EmbeddingModelInfo>> {
        self.embeddings.get_embedding_model(index_name).await
    }

    /// Upsert the embedding model metadata for an index.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the upsert fails.
    #[instrument(skip(self))]
    pub async fn set_embedding_model(
        &self,
        index_name: &str,
        model: &str,
        backend: &str,
        dimension: i64,
    ) -> DbResult<()> {
        self.embeddings
            .set_embedding_model(index_name, model, backend, dimension)
            .await
    }

    /// Remove the embedding model record for an index.
    ///
    /// # Errors
    ///
    /// Returns `DbError` if the delete fails.
    #[instrument(skip(self))]
    pub async fn clear_embedding_model(&self, index_name: &str) -> DbResult<()> {
        self.embeddings.clear_embedding_model(index_name).await
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::path::Path;

    use futures::TryStreamExt as _;

    use super::*;
    use crate::db::test_pool;
    use crate::models::{ErrorFilter, JobProgress, JobType};

    async fn tracker() -> anyhow::Result<JobTracker> {
        Ok(JobTracker::new(test_pool().await?))
    }

    #[tokio::test]
    async fn test_create_and_get_job() -> anyhow::Result<()> {
        let t = tracker().await?;
        let job = t
            .create_job(
                "t_job_1",
                Path::new("/src"),
                "aum",
                JobType::Ingest,
                42,
                None,
            )
            .await?;
        assert_eq!(job.job_id, "t_job_1");
        assert_eq!(job.total_files, 42);

        let fetched = t
            .get_job("t_job_1", false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected job"))?;
        assert_eq!(fetched.job_id, "t_job_1");
        Ok(())
    }

    #[tokio::test]
    async fn test_update_total_files() -> anyhow::Result<()> {
        let t = tracker().await?;
        t.create_job("t_walk", Path::new("/w"), "aum", JobType::Ingest, 0, None)
            .await?;

        t.update_total_files("t_walk", 500).await?;

        let job = t
            .get_job("t_walk", false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected job"))?;
        assert_eq!(job.total_files, 500);
        Ok(())
    }

    #[tokio::test]
    async fn test_progress_and_completion() -> anyhow::Result<()> {
        let t = tracker().await?;
        t.create_job("t_prog", Path::new("/p"), "aum", JobType::Ingest, 10, None)
            .await?;

        t.update_progress(
            "t_prog",
            &JobProgress {
                extracted: 8,
                processed: 7,
                failed: 1,
                empty: 2,
                skipped: 0,
            },
        )
        .await?;

        t.complete_job("t_prog", JobStatus::Completed).await?;

        let job = t
            .get_job("t_prog", false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected job"))?;
        assert_eq!(job.status, JobStatus::Completed);
        assert_eq!(job.processed, 7);
        assert!(job.finished_at.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_record_error_and_list() -> anyhow::Result<()> {
        let t = tracker().await?;
        t.create_job("t_err", Path::new("/e"), "aum", JobType::Ingest, 0, None)
            .await?;

        t.record_error("t_err", Path::new("/e/bad.pdf"), "ParseError", "corrupt")
            .await?;
        t.record_error(
            "t_err",
            Path::new("/e/slow.pdf"),
            "TimeoutError",
            "timed out",
        )
        .await?;

        let errors: Vec<_> = t.list_errors("t_err").try_collect().await?;
        assert_eq!(errors.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_failed_doc_ids() -> anyhow::Result<()> {
        let t = tracker().await?;
        t.create_job("t_embed", Path::new("/em"), "aum", JobType::Embed, 0, None)
            .await?;

        t.record_error("t_embed", Path::new("doc_aaa"), "EmbeddingError", "fail")
            .await?;
        t.record_error("t_embed", Path::new("doc_bbb"), "EmbeddingError", "fail")
            .await?;
        // duplicate doc with different error type
        t.record_error("t_embed", Path::new("doc_aaa"), "TimeoutError", "slow")
            .await?;

        let ids: Vec<_> = t.get_failed_doc_ids("t_embed").try_collect().await?;
        assert_eq!(ids.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_list_jobs() -> anyhow::Result<()> {
        let t = tracker().await?;
        t.create_job("t_list_a", Path::new("/a"), "aum", JobType::Ingest, 0, None)
            .await?;
        t.create_job("t_list_b", Path::new("/b"), "aum", JobType::Ingest, 0, None)
            .await?;
        t.complete_job("t_list_a", JobStatus::Completed).await?;

        let all: Vec<_> = t.list_jobs(None).try_collect().await?;
        assert_eq!(all.len(), 2);

        let pending: Vec<_> = t.list_jobs(Some(JobStatus::Pending)).try_collect().await?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].job_id, "t_list_b");
        Ok(())
    }

    #[tokio::test]
    async fn test_find_resumable_job() -> anyhow::Result<()> {
        let t = tracker().await?;
        t.create_job(
            "t_resume",
            Path::new("/r"),
            "aum",
            JobType::Ingest,
            10,
            None,
        )
        .await?;
        t.complete_job("t_resume", JobStatus::Interrupted).await?;

        let found = t
            .find_resumable_job(Some(Path::new("/r")), JobType::Ingest)
            .await?;
        assert_eq!(found.as_ref().map(|j| j.job_id.as_str()), Some("t_resume"));

        let not_found = t
            .find_resumable_job(Some(Path::new("/other")), JobType::Ingest)
            .await?;
        assert!(not_found.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_failed_paths() -> anyhow::Result<()> {
        let t = tracker().await?;
        t.create_job("t_fp", Path::new("/fp"), "aum", JobType::Ingest, 0, None)
            .await?;

        t.record_error("t_fp", Path::new("/fp/a.pdf"), "ParseError", "bad")
            .await?;
        t.record_error("t_fp", Path::new("/fp/b.pdf"), "EmptyExtraction", "empty")
            .await?;

        let all: Vec<_> = t
            .get_failed_paths("t_fp", ErrorFilter::All)
            .try_collect()
            .await?;
        assert_eq!(all.len(), 2);

        let excluding_empty: Vec<_> = t
            .get_failed_paths("t_fp", ErrorFilter::Exclude(&["EmptyExtraction"]))
            .try_collect()
            .await?;
        assert_eq!(excluding_empty.len(), 1);

        let only_empty: Vec<_> = t
            .get_failed_paths("t_fp", ErrorFilter::Only("EmptyExtraction"))
            .try_collect()
            .await?;
        assert_eq!(only_empty.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_index() -> anyhow::Result<()> {
        let t = tracker().await?;
        t.create_job(
            "t_del_1",
            Path::new("/d"),
            "idx_a",
            JobType::Ingest,
            0,
            None,
        )
        .await?;
        t.create_job(
            "t_del_2",
            Path::new("/d"),
            "idx_a",
            JobType::Ingest,
            0,
            None,
        )
        .await?;
        t.create_job("t_keep", Path::new("/d"), "idx_b", JobType::Ingest, 0, None)
            .await?;

        t.record_error("t_del_1", Path::new("/d/x.pdf"), "ParseError", "err")
            .await?;

        let deleted = t.clear_index("idx_a").await?;
        assert_eq!(deleted, 2);
        assert!(t.get_job("t_del_1", false).await?.is_none());
        assert!(t.get_job("t_keep", false).await?.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_embedding_model_roundtrip() -> anyhow::Result<()> {
        let t = tracker().await?;

        assert!(t.get_embedding_model("my_idx").await?.is_none());

        t.set_embedding_model("my_idx", "arctic-embed", "ollama", 1024)
            .await?;
        let info = t
            .get_embedding_model("my_idx")
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected model"))?;
        assert_eq!(info.model, "arctic-embed");
        assert_eq!(info.backend, "ollama");
        assert_eq!(info.dimension, 1024);

        t.clear_embedding_model("my_idx").await?;
        assert!(t.get_embedding_model("my_idx").await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_ocr_settings_roundtrip() -> anyhow::Result<()> {
        let t = tracker().await?;

        // Job with OCR settings stored.
        let job = t
            .create_job(
                "t_ocr",
                Path::new("/ocr"),
                "aum",
                JobType::Ingest,
                0,
                Some(OcrSettings {
                    enabled: true,
                    language: "eng+fra".to_owned(),
                }),
            )
            .await?;
        let expected = OcrSettings {
            enabled: true,
            language: "eng+fra".to_owned(),
        };
        assert_eq!(job.ocr_settings, Some(expected.clone()));

        // Verify round-trip through get_job.
        let fetched = t
            .get_job("t_ocr", false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected job"))?;
        assert_eq!(fetched.ocr_settings, Some(expected));

        // Job with OCR disabled.
        t.create_job(
            "t_ocr_off",
            Path::new("/ocr2"),
            "aum",
            JobType::Ingest,
            0,
            Some(OcrSettings {
                enabled: false,
                language: "eng".to_owned(),
            }),
        )
        .await?;
        let fetched2 = t
            .get_job("t_ocr_off", false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected job"))?;
        assert_eq!(
            fetched2.ocr_settings,
            Some(OcrSettings {
                enabled: false,
                language: "eng".to_owned()
            })
        );

        // Job with no OCR settings (legacy / None).
        t.create_job(
            "t_ocr_none",
            Path::new("/ocr3"),
            "aum",
            JobType::Ingest,
            0,
            None,
        )
        .await?;
        let fetched3 = t
            .get_job("t_ocr_none", false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("expected job"))?;
        assert_eq!(fetched3.ocr_settings, None);

        Ok(())
    }
}
