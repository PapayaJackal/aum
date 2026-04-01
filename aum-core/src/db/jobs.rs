//! sqlx implementation of [`JobRepository`].

use std::path::{Path, PathBuf};
use std::time::Instant;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::StreamExt as _;
use futures::stream::BoxStream;
use sqlx::AnyPool;
use tracing::{info_span, instrument};
use tracing_futures::Instrument as _;

use crate::models::{IngestError, IngestJob, JobProgress, JobStatus, JobType};

use super::error::{DbError, DbResult};
use super::repository::JobRepository;

// ---------------------------------------------------------------------------
// AnyPool-compatible helpers
//
// sqlx::Any does not implement Type/Encode/Decode for DateTime<Utc> or for
// custom enum types derived with sqlx::Type.  We therefore store everything
// as TEXT and do the conversion ourselves.
// ---------------------------------------------------------------------------

fn job_status_str(s: JobStatus) -> &'static str {
    match s {
        JobStatus::Pending => "pending",
        JobStatus::Running => "running",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
        JobStatus::Interrupted => "interrupted",
    }
}

fn parse_job_status(s: &str) -> JobStatus {
    match s {
        "running" => JobStatus::Running,
        "completed" => JobStatus::Completed,
        "failed" => JobStatus::Failed,
        "interrupted" => JobStatus::Interrupted,
        _ => JobStatus::Pending,
    }
}

fn job_type_str(t: JobType) -> &'static str {
    match t {
        JobType::Ingest => "ingest",
        JobType::Embed => "embed",
    }
}

fn parse_job_type(s: &str) -> JobType {
    match s {
        "embed" => JobType::Embed,
        _ => JobType::Ingest,
    }
}

fn parse_dt(s: &str) -> DateTime<Utc> {
    #[allow(clippy::expect_used)] // DB timestamps are always valid RFC3339; corrupt data is a bug
    DateTime::parse_from_rfc3339(s)
        .expect("timestamps in the database are always valid RFC3339")
        .with_timezone(&Utc)
}

// ---------------------------------------------------------------------------
// Row struct (sqlx mapping)
// ---------------------------------------------------------------------------

/// Flat row that sqlx maps directly from a `jobs` query result.
///
/// All non-primitive columns are held as `String` so they work with
/// `AnyPool`, and are converted to the correct domain types in `From<JobRow>`.
#[derive(sqlx::FromRow)]
struct JobRow {
    job_id: String,
    source_dir: String,
    index_name: String,
    status: String,
    total_files: i64,
    extracted: i64,
    processed: i64,
    failed: i64,
    empty: i64,
    skipped: i64,
    created_at: String,
    finished_at: Option<String>,
    job_type: String,
}

impl From<JobRow> for IngestJob {
    fn from(r: JobRow) -> Self {
        IngestJob {
            job_id: r.job_id,
            source_dir: PathBuf::from(r.source_dir),
            index_name: r.index_name,
            status: parse_job_status(&r.status),
            total_files: r.total_files,
            extracted: r.extracted,
            processed: r.processed,
            failed: r.failed,
            empty: r.empty,
            skipped: r.skipped,
            created_at: parse_dt(&r.created_at),
            finished_at: r.finished_at.as_deref().map(parse_dt),
            job_type: parse_job_type(&r.job_type),
            errors: vec![],
        }
    }
}

// ---------------------------------------------------------------------------
// Repository implementation
// ---------------------------------------------------------------------------

/// sqlx-backed implementation of [`JobRepository`].
pub struct SqlxJobRepository {
    pool: AnyPool,
}

impl SqlxJobRepository {
    /// Create a new repository backed by the given pool.
    #[must_use]
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }
}

use super::record_db_metrics;

#[async_trait]
impl JobRepository for SqlxJobRepository {
    #[instrument(skip(self), fields(table = "jobs"))]
    async fn create_job(
        &self,
        job_id: &str,
        source_dir: &Path,
        index_name: &str,
        job_type: JobType,
        total_files: i64,
    ) -> DbResult<IngestJob> {
        let now = Utc::now().to_rfc3339();
        let source_dir_str = source_dir.to_string_lossy();
        let start = Instant::now();

        // INSERT then SELECT to stay compatible with all backends (MySQL does
        // not support RETURNING).
        let insert_result = sqlx::query(
            "INSERT INTO jobs \
             (job_id, source_dir, index_name, status, job_type, total_files, created_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7)",
        )
        .bind(job_id)
        .bind(source_dir_str.as_ref())
        .bind(index_name)
        .bind(job_status_str(JobStatus::Pending))
        .bind(job_type_str(job_type))
        .bind(total_files)
        .bind(&now)
        .execute(&self.pool)
        .await
        .map_err(DbError::from);
        record_db_metrics("create_job_insert", "jobs", start, insert_result.is_ok());
        insert_result?;

        let fetch_start = Instant::now();
        let result = sqlx::query_as::<sqlx::Any, JobRow>(
            "SELECT job_id, source_dir, index_name, status, total_files, \
             extracted, processed, failed, empty, skipped, \
             created_at, finished_at, job_type \
             FROM jobs WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_one(&self.pool)
        .await
        .map(IngestJob::from)
        .map_err(DbError::from);
        record_db_metrics("create_job_fetch", "jobs", fetch_start, result.is_ok());
        result
    }

    #[instrument(skip(self), fields(table = "jobs"))]
    async fn update_progress(&self, job_id: &str, progress: &JobProgress) -> DbResult<()> {
        let start = Instant::now();
        let result = sqlx::query(
            "UPDATE jobs \
             SET extracted = $1, processed = $2, failed = $3, empty = $4, skipped = $5 \
             WHERE job_id = $6",
        )
        .bind(progress.extracted)
        .bind(progress.processed)
        .bind(progress.failed)
        .bind(progress.empty)
        .bind(progress.skipped)
        .bind(job_id)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(DbError::from);
        record_db_metrics("update_progress", "jobs", start, result.is_ok());
        result
    }

    #[instrument(skip(self), fields(table = "jobs"))]
    async fn complete_job(&self, job_id: &str, status: JobStatus) -> DbResult<()> {
        let now = Utc::now().to_rfc3339();
        let start = Instant::now();
        let result = sqlx::query("UPDATE jobs SET status = $1, finished_at = $2 WHERE job_id = $3")
            .bind(job_status_str(status))
            .bind(&now)
            .bind(job_id)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(DbError::from);
        record_db_metrics("complete_job", "jobs", start, result.is_ok());
        result
    }

    #[instrument(skip(self), fields(table = "jobs"))]
    async fn get_job(&self, job_id: &str, include_errors: bool) -> DbResult<Option<IngestJob>> {
        let start = Instant::now();
        let result = sqlx::query_as::<sqlx::Any, JobRow>(
            "SELECT job_id, source_dir, index_name, status, total_files, \
             extracted, processed, failed, empty, skipped, \
             created_at, finished_at, job_type \
             FROM jobs WHERE job_id = $1",
        )
        .bind(job_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::from);
        record_db_metrics("get_job", "jobs", start, result.is_ok());

        let mut job = match result? {
            Some(row) => IngestJob::from(row),
            None => return Ok(None),
        };

        if include_errors {
            let errors_start = Instant::now();
            let errors_result = sqlx::query_as::<sqlx::Any, ErrorRow>(
                "SELECT job_id, file_path, error_type, message, timestamp \
                 FROM job_errors WHERE job_id = $1 ORDER BY timestamp",
            )
            .bind(job_id)
            .fetch_all(&self.pool)
            .await
            .map_err(DbError::from);
            record_db_metrics(
                "get_job_errors",
                "job_errors",
                errors_start,
                errors_result.is_ok(),
            );
            job.errors = errors_result?.into_iter().map(IngestError::from).collect();
        }

        Ok(Some(job))
    }

    fn list_jobs(&self, status: Option<JobStatus>) -> BoxStream<'_, DbResult<IngestJob>> {
        let span = info_span!("db.query", operation = "list_jobs", table = "jobs");
        match status {
            Some(s) => Box::pin(
                sqlx::query_as::<sqlx::Any, JobRow>(
                    "SELECT job_id, source_dir, index_name, status, total_files, \
                     extracted, processed, failed, empty, skipped, \
                     created_at, finished_at, job_type \
                     FROM jobs WHERE status = $1 ORDER BY created_at DESC",
                )
                .bind(job_status_str(s))
                .fetch(&self.pool)
                .instrument(span)
                .map(|r| r.map(IngestJob::from).map_err(DbError::from)),
            ),
            None => Box::pin(
                sqlx::query_as::<sqlx::Any, JobRow>(
                    "SELECT job_id, source_dir, index_name, status, total_files, \
                     extracted, processed, failed, empty, skipped, \
                     created_at, finished_at, job_type \
                     FROM jobs ORDER BY created_at DESC",
                )
                .fetch(&self.pool)
                .instrument(span)
                .map(|r| r.map(IngestJob::from).map_err(DbError::from)),
            ),
        }
    }

    #[instrument(skip(self), fields(table = "jobs"))]
    async fn find_resumable_job(
        &self,
        source_dir: Option<&Path>,
        job_type: JobType,
    ) -> DbResult<Option<IngestJob>> {
        let start = Instant::now();
        let result = if let Some(dir) = source_dir {
            let dir_str = dir.to_string_lossy();
            sqlx::query_as::<sqlx::Any, JobRow>(
                "SELECT job_id, source_dir, index_name, status, total_files, \
                 extracted, processed, failed, empty, skipped, \
                 created_at, finished_at, job_type \
                 FROM jobs \
                 WHERE status = 'interrupted' AND job_type = $1 AND source_dir = $2 \
                 ORDER BY created_at DESC LIMIT 1",
            )
            .bind(job_type_str(job_type))
            .bind(dir_str.as_ref())
            .fetch_optional(&self.pool)
            .await
        } else {
            sqlx::query_as::<sqlx::Any, JobRow>(
                "SELECT job_id, source_dir, index_name, status, total_files, \
                 extracted, processed, failed, empty, skipped, \
                 created_at, finished_at, job_type \
                 FROM jobs \
                 WHERE status = 'interrupted' AND job_type = $1 \
                 ORDER BY created_at DESC LIMIT 1",
            )
            .bind(job_type_str(job_type))
            .fetch_optional(&self.pool)
            .await
        };
        let result = result
            .map(|opt| opt.map(IngestJob::from))
            .map_err(DbError::from);
        record_db_metrics("find_resumable_job", "jobs", start, result.is_ok());
        result
    }

    #[instrument(skip(self), fields(table = "jobs"))]
    async fn clear_index(&self, index_name: &str) -> DbResult<u64> {
        let start = Instant::now();
        // Delete errors first to satisfy the FK constraint.
        sqlx::query(
            "DELETE FROM job_errors WHERE job_id IN \
             (SELECT job_id FROM jobs WHERE index_name = $1)",
        )
        .bind(index_name)
        .execute(&self.pool)
        .await
        .map_err(DbError::from)?;

        let result = sqlx::query("DELETE FROM jobs WHERE index_name = $1")
            .bind(index_name)
            .execute(&self.pool)
            .await
            .map(|r| r.rows_affected())
            .map_err(DbError::from);
        record_db_metrics("clear_index", "jobs", start, result.is_ok());
        result
    }
}

// ---------------------------------------------------------------------------
// ErrorRow — shared with errors.rs via pub(super)
// ---------------------------------------------------------------------------

#[derive(sqlx::FromRow)]
pub(super) struct ErrorRow {
    #[allow(dead_code)]
    pub(super) job_id: String,
    pub(super) file_path: String,
    pub(super) error_type: String,
    pub(super) message: String,
    pub(super) timestamp: String,
}

impl From<ErrorRow> for IngestError {
    fn from(r: ErrorRow) -> Self {
        IngestError {
            file_path: PathBuf::from(r.file_path),
            error_type: r.error_type,
            message: r.message,
            timestamp: parse_dt(&r.timestamp),
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_pool;
    use crate::models::JobProgress;

    #[tokio::test]
    async fn test_create_and_get_job_roundtrip() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxJobRepository::new(pool);

        let job = repo
            .create_job(
                "happy_fox_abc",
                Path::new("/data"),
                "aum",
                JobType::Ingest,
                100,
            )
            .await?;

        assert_eq!(job.job_id, "happy_fox_abc");
        assert_eq!(job.source_dir, PathBuf::from("/data"));
        assert_eq!(job.index_name, "aum");
        assert_eq!(job.status, JobStatus::Pending);
        assert_eq!(job.total_files, 100);
        assert!(job.finished_at.is_none());

        let fetched = repo
            .get_job("happy_fox_abc", false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job should exist"))?;
        assert_eq!(fetched.job_id, job.job_id);
        assert_eq!(fetched.total_files, 100);
        assert!(fetched.errors.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_get_job_not_found_returns_none() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxJobRepository::new(pool);
        let result = repo.get_job("no_such_job_xyz", false).await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_update_progress_reflected_in_get_job() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxJobRepository::new(pool);
        repo.create_job(
            "brave_owl_def",
            Path::new("/docs"),
            "aum",
            JobType::Ingest,
            50,
        )
        .await?;

        repo.update_progress(
            "brave_owl_def",
            &JobProgress {
                extracted: 10,
                processed: 8,
                failed: 2,
                empty: 1,
                skipped: 3,
            },
        )
        .await?;

        let job = repo
            .get_job("brave_owl_def", false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job should exist"))?;
        assert_eq!(job.extracted, 10);
        assert_eq!(job.processed, 8);
        assert_eq!(job.failed, 2);
        assert_eq!(job.empty, 1);
        assert_eq!(job.skipped, 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_complete_job_sets_status_and_finished_at() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxJobRepository::new(pool);
        repo.create_job("calm_deer_ghi", Path::new("/x"), "aum", JobType::Embed, 0)
            .await?;

        repo.complete_job("calm_deer_ghi", JobStatus::Completed)
            .await?;

        let job = repo
            .get_job("calm_deer_ghi", false)
            .await?
            .ok_or_else(|| anyhow::anyhow!("job should exist"))?;
        assert_eq!(job.status, JobStatus::Completed);
        assert!(job.finished_at.is_some());
        Ok(())
    }

    #[tokio::test]
    async fn test_list_jobs_status_filter() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxJobRepository::new(pool);

        repo.create_job("job_a", Path::new("/a"), "aum", JobType::Ingest, 0)
            .await?;
        repo.create_job("job_b", Path::new("/b"), "aum", JobType::Ingest, 0)
            .await?;
        repo.complete_job("job_a", JobStatus::Completed).await?;

        let all: Vec<_> = repo
            .list_jobs(None)
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        assert_eq!(all.len(), 2);

        let pending: Vec<_> = repo
            .list_jobs(Some(JobStatus::Pending))
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].job_id, "job_b");
        Ok(())
    }

    #[tokio::test]
    async fn test_find_resumable_job() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxJobRepository::new(pool);

        repo.create_job("int_job", Path::new("/resume"), "aum", JobType::Ingest, 10)
            .await?;
        repo.complete_job("int_job", JobStatus::Interrupted).await?;

        let found = repo
            .find_resumable_job(Some(Path::new("/resume")), JobType::Ingest)
            .await?
            .ok_or_else(|| anyhow::anyhow!("should find resumable job"))?;
        assert_eq!(found.job_id, "int_job");

        let not_found = repo
            .find_resumable_job(Some(Path::new("/other")), JobType::Ingest)
            .await?;
        assert!(not_found.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_index_removes_jobs() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxJobRepository::new(pool);

        repo.create_job("to_del_1", Path::new("/d"), "my_index", JobType::Ingest, 0)
            .await?;
        repo.create_job("to_del_2", Path::new("/d"), "my_index", JobType::Ingest, 0)
            .await?;
        repo.create_job("keep_me", Path::new("/d"), "other", JobType::Ingest, 0)
            .await?;

        let deleted = repo.clear_index("my_index").await?;
        assert_eq!(deleted, 2);

        assert!(repo.get_job("to_del_1", false).await?.is_none());
        assert!(repo.get_job("keep_me", false).await?.is_some());
        Ok(())
    }
}
