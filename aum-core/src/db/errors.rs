//! sqlx implementation of [`JobErrorRepository`].

use std::path::{Path, PathBuf};
use std::time::Instant;

use async_trait::async_trait;
use futures::StreamExt as _;
use futures::stream::BoxStream;
use sqlx::AnyPool;
use tracing::{info_span, instrument};
use tracing_futures::Instrument as _;

use crate::models::IngestError;

use super::error::{DbError, DbResult};
use super::jobs::ErrorRow;
use super::record_db_metrics;
use super::repository::JobErrorRepository;

/// sqlx-backed implementation of [`JobErrorRepository`].
pub struct SqlxJobErrorRepository {
    pool: AnyPool,
}

impl SqlxJobErrorRepository {
    /// Create a new repository backed by the given pool.
    #[must_use]
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl JobErrorRepository for SqlxJobErrorRepository {
    #[instrument(skip(self), fields(table = "job_errors"))]
    async fn record_error(
        &self,
        job_id: &str,
        file_path: &Path,
        error_type: &str,
        message: &str,
    ) -> DbResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let path_str = file_path.to_string_lossy();
        let start = Instant::now();
        // ON CONFLICT DO NOTHING works in SQLite 3.24+, PostgreSQL, and MariaDB.
        // For MySQL, INSERT IGNORE would be needed, but MySQL support is best-effort.
        let result = sqlx::query(
            "INSERT INTO job_errors (job_id, file_path, error_type, message, timestamp) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (job_id, file_path, error_type) DO NOTHING",
        )
        .bind(job_id)
        .bind(path_str.as_ref())
        .bind(error_type)
        .bind(message)
        .bind(&now)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(DbError::from);
        record_db_metrics("record_error", "job_errors", start, result.is_ok());
        result
    }

    fn list_errors<'a>(&'a self, job_id: &str) -> BoxStream<'a, DbResult<IngestError>> {
        let span = info_span!("db.query", operation = "list_errors", table = "job_errors");
        let job_id = job_id.to_owned();
        Box::pin(
            sqlx::query_as::<sqlx::Any, ErrorRow>(
                "SELECT job_id, file_path, error_type, message, timestamp \
                 FROM job_errors WHERE job_id = $1 ORDER BY timestamp",
            )
            .bind(job_id)
            .fetch(&self.pool)
            .instrument(span)
            .map(|r| r.map(IngestError::from).map_err(DbError::from)),
        )
    }

    #[instrument(skip(self), fields(table = "job_errors"))]
    async fn get_failed_paths(
        &self,
        job_id: &str,
        exclude_type: Option<&str>,
        only_type: Option<&str>,
    ) -> DbResult<Vec<PathBuf>> {
        let start = Instant::now();
        let result = match (exclude_type, only_type) {
            (Some(excl), None) => {
                sqlx::query_as::<sqlx::Any, (String,)>(
                    "SELECT DISTINCT file_path FROM job_errors \
                     WHERE job_id = $1 AND error_type != $2",
                )
                .bind(job_id)
                .bind(excl)
                .fetch_all(&self.pool)
                .await
            }
            (None, Some(only)) => {
                sqlx::query_as::<sqlx::Any, (String,)>(
                    "SELECT DISTINCT file_path FROM job_errors \
                     WHERE job_id = $1 AND error_type = $2",
                )
                .bind(job_id)
                .bind(only)
                .fetch_all(&self.pool)
                .await
            }
            _ => {
                sqlx::query_as::<sqlx::Any, (String,)>(
                    "SELECT DISTINCT file_path FROM job_errors WHERE job_id = $1",
                )
                .bind(job_id)
                .fetch_all(&self.pool)
                .await
            }
        };
        let result = result
            .map(|rows| rows.into_iter().map(|(p,)| PathBuf::from(p)).collect())
            .map_err(DbError::from);
        record_db_metrics("get_failed_paths", "job_errors", start, result.is_ok());
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::repository::JobRepository as _;
    use crate::db::{SqlxJobRepository, test_pool};
    use crate::models::JobType;

    async fn setup_job(pool: &AnyPool, job_id: &str) -> anyhow::Result<()> {
        SqlxJobRepository::new(pool.clone())
            .create_job(job_id, Path::new("/data"), "aum", JobType::Ingest, 0)
            .await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_record_error_and_list_errors() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        setup_job(&pool, "err_job_1").await?;
        let repo = SqlxJobErrorRepository::new(pool);

        repo.record_error("err_job_1", Path::new("/a/b.pdf"), "ParseError", "oops")
            .await?;

        let errors: Vec<_> = repo
            .list_errors("err_job_1")
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].file_path, PathBuf::from("/a/b.pdf"));
        assert_eq!(errors[0].error_type, "ParseError");
        assert_eq!(errors[0].message, "oops");
        Ok(())
    }

    #[tokio::test]
    async fn test_duplicate_error_is_silently_ignored() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        setup_job(&pool, "err_job_2").await?;
        let repo = SqlxJobErrorRepository::new(pool);

        repo.record_error("err_job_2", Path::new("/x.pdf"), "Timeout", "slow")
            .await?;
        repo.record_error("err_job_2", Path::new("/x.pdf"), "Timeout", "still slow")
            .await?;

        let errors: Vec<_> = repo
            .list_errors("err_job_2")
            .collect::<Vec<_>>()
            .await
            .into_iter()
            .collect::<Result<_, _>>()?;
        assert_eq!(errors.len(), 1, "duplicate should not insert a second row");
        Ok(())
    }

    #[tokio::test]
    async fn test_get_failed_paths_no_filter() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        setup_job(&pool, "err_job_3").await?;
        let repo = SqlxJobErrorRepository::new(pool);

        repo.record_error("err_job_3", Path::new("/a.pdf"), "TypeA", "msg")
            .await?;
        repo.record_error("err_job_3", Path::new("/b.pdf"), "TypeB", "msg")
            .await?;

        let paths = repo.get_failed_paths("err_job_3", None, None).await?;
        assert_eq!(paths.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn test_get_failed_paths_only_type() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        setup_job(&pool, "err_job_4").await?;
        let repo = SqlxJobErrorRepository::new(pool);

        repo.record_error("err_job_4", Path::new("/a.pdf"), "TypeA", "msg")
            .await?;
        repo.record_error("err_job_4", Path::new("/b.pdf"), "TypeB", "msg")
            .await?;

        let paths = repo
            .get_failed_paths("err_job_4", None, Some("TypeA"))
            .await?;
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], PathBuf::from("/a.pdf"));
        Ok(())
    }

    #[tokio::test]
    async fn test_get_failed_paths_exclude_type() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        setup_job(&pool, "err_job_5").await?;
        let repo = SqlxJobErrorRepository::new(pool);

        repo.record_error("err_job_5", Path::new("/a.pdf"), "EmptyExtraction", "msg")
            .await?;
        repo.record_error("err_job_5", Path::new("/b.pdf"), "ParseError", "msg")
            .await?;

        let paths = repo
            .get_failed_paths("err_job_5", Some("EmptyExtraction"), None)
            .await?;
        assert_eq!(paths.len(), 1);
        assert_eq!(paths[0], PathBuf::from("/b.pdf"));
        Ok(())
    }
}
