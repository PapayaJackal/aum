//! sqlx implementation of [`IndexEmbeddingRepository`].

use std::time::Instant;

use async_trait::async_trait;
use sqlx::AnyPool;
use tracing::instrument;

use super::error::{DbError, DbResult};
use super::record_db_metrics;
use super::repository::IndexEmbeddingRepository;

/// sqlx-backed implementation of [`IndexEmbeddingRepository`].
pub struct SqlxIndexEmbeddingRepository {
    pool: AnyPool,
}

impl SqlxIndexEmbeddingRepository {
    /// Create a new repository backed by the given pool.
    #[must_use]
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl IndexEmbeddingRepository for SqlxIndexEmbeddingRepository {
    #[instrument(skip(self), fields(table = "index_embeddings"))]
    async fn get_embedding_model(
        &self,
        index_name: &str,
    ) -> DbResult<Option<(String, String, i64)>> {
        let start = Instant::now();
        let result = sqlx::query_as::<sqlx::Any, (String, String, i64)>(
            "SELECT model, backend, dimension FROM index_embeddings WHERE index_name = $1",
        )
        .bind(index_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::from);
        record_db_metrics(
            "get_embedding_model",
            "index_embeddings",
            start,
            result.is_ok(),
        );
        result
    }

    #[instrument(skip(self), fields(table = "index_embeddings"))]
    async fn set_embedding_model(
        &self,
        index_name: &str,
        model: &str,
        backend: &str,
        dimension: i64,
    ) -> DbResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        let start = Instant::now();
        // INSERT OR REPLACE is SQLite syntax; use the ANSI equivalent that all
        // three backends support.
        let result = sqlx::query(
            "INSERT INTO index_embeddings (index_name, model, backend, dimension, updated_at) \
             VALUES ($1, $2, $3, $4, $5) \
             ON CONFLICT (index_name) DO UPDATE \
             SET model = excluded.model, backend = excluded.backend, \
                 dimension = excluded.dimension, updated_at = excluded.updated_at",
        )
        .bind(index_name)
        .bind(model)
        .bind(backend)
        .bind(dimension)
        .bind(&now)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(DbError::from);
        record_db_metrics(
            "set_embedding_model",
            "index_embeddings",
            start,
            result.is_ok(),
        );
        result
    }

    #[instrument(skip(self), fields(table = "index_embeddings"))]
    async fn clear_embedding_model(&self, index_name: &str) -> DbResult<()> {
        let start = Instant::now();
        let result = sqlx::query("DELETE FROM index_embeddings WHERE index_name = $1")
            .bind(index_name)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(DbError::from);
        record_db_metrics(
            "clear_embedding_model",
            "index_embeddings",
            start,
            result.is_ok(),
        );
        result
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_pool;

    #[tokio::test]
    async fn test_get_returns_none_before_set() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxIndexEmbeddingRepository::new(pool);
        let result = repo.get_embedding_model("my_index").await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_set_and_get_roundtrip() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxIndexEmbeddingRepository::new(pool);
        repo.set_embedding_model("my_index", "arctic-embed", "ollama", 1024)
            .await?;
        let (model, backend, dim) = repo
            .get_embedding_model("my_index")
            .await?
            .ok_or_else(|| anyhow::anyhow!("should exist"))?;
        assert_eq!(model, "arctic-embed");
        assert_eq!(backend, "ollama");
        assert_eq!(dim, 1024);
        Ok(())
    }

    #[tokio::test]
    async fn test_set_is_idempotent_upsert() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxIndexEmbeddingRepository::new(pool);
        repo.set_embedding_model("idx", "old-model", "ollama", 512)
            .await?;
        repo.set_embedding_model("idx", "new-model", "openai", 1536)
            .await?;
        let (model, backend, dim) = repo
            .get_embedding_model("idx")
            .await?
            .ok_or_else(|| anyhow::anyhow!("should exist"))?;
        assert_eq!(model, "new-model");
        assert_eq!(backend, "openai");
        assert_eq!(dim, 1536);
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_removes_record() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxIndexEmbeddingRepository::new(pool);
        repo.set_embedding_model("to_clear", "m", "b", 64).await?;
        repo.clear_embedding_model("to_clear").await?;
        let result = repo.get_embedding_model("to_clear").await?;
        assert!(result.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_nonexistent_is_ok() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxIndexEmbeddingRepository::new(pool);
        repo.clear_embedding_model("ghost_index").await?;
        Ok(())
    }
}
