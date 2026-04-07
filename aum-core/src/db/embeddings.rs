//! sqlx implementation of [`IndexEmbeddingRepository`].

use async_trait::async_trait;
use sqlx::AnyPool;

use crate::models::EmbeddingModelInfo;

use super::error::{DbError, DbResult};
use super::repository::IndexEmbeddingRepository;

/// sqlx-backed implementation of [`IndexEmbeddingRepository`].
#[derive(Clone)]
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
    async fn get_embedding_model(&self, index_name: &str) -> DbResult<Option<EmbeddingModelInfo>> {
        sqlx::query_as::<sqlx::Any, (String, String, i64, i64, String)>(
            "SELECT model, backend, dimension, context_length, query_prefix \
             FROM index_embeddings WHERE index_name = $1",
        )
        .bind(index_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(DbError::from)?
        .map(
            |(model, backend_str, dimension, context_length, query_prefix): (
                String,
                String,
                i64,
                i64,
                String,
            )| {
                let backend: crate::config::EmbeddingsBackend = backend_str
                    .parse()
                    .map_err(|e: String| DbError::NotFound(e))?;
                Ok(EmbeddingModelInfo {
                    model,
                    backend,
                    dimension,
                    context_length,
                    query_prefix,
                })
            },
        )
        .transpose()
    }

    async fn set_embedding_model(
        &self,
        index_name: &str,
        info: &EmbeddingModelInfo,
    ) -> DbResult<()> {
        let now = chrono::Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO index_embeddings \
                 (index_name, model, backend, dimension, context_length, query_prefix, updated_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7) \
             ON CONFLICT (index_name) DO UPDATE \
             SET model = excluded.model, backend = excluded.backend, \
                 dimension = excluded.dimension, context_length = excluded.context_length, \
                 query_prefix = excluded.query_prefix, updated_at = excluded.updated_at",
        )
        .bind(index_name)
        .bind(&info.model)
        .bind(info.backend.to_string())
        .bind(info.dimension)
        .bind(info.context_length)
        .bind(&info.query_prefix)
        .bind(&now)
        .execute(&self.pool)
        .await
        .map(|_| ())
        .map_err(DbError::from)
    }

    async fn clear_embedding_model(&self, index_name: &str) -> DbResult<()> {
        sqlx::query("DELETE FROM index_embeddings WHERE index_name = $1")
            .bind(index_name)
            .execute(&self.pool)
            .await
            .map(|_| ())
            .map_err(DbError::from)
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EmbeddingsBackend;
    use crate::db::test_pool;

    #[tokio::test]
    async fn test_get_returns_none_before_set() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxIndexEmbeddingRepository::new(pool);
        let result = repo.get_embedding_model("my_index").await?;
        assert!(result.is_none());
        Ok(())
    }

    fn test_info(model: &str, backend: EmbeddingsBackend, dimension: i64) -> EmbeddingModelInfo {
        EmbeddingModelInfo {
            model: model.to_owned(),
            backend,
            dimension,
            context_length: 8192,
            query_prefix: "query: ".to_owned(),
        }
    }

    #[tokio::test]
    async fn test_set_and_get_roundtrip() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxIndexEmbeddingRepository::new(pool);
        let info = test_info("arctic-embed", EmbeddingsBackend::Ollama, 1024);
        repo.set_embedding_model("my_index", &info).await?;
        let got = repo
            .get_embedding_model("my_index")
            .await?
            .ok_or_else(|| anyhow::anyhow!("should exist"))?;
        assert_eq!(got.model, "arctic-embed");
        assert_eq!(got.backend, EmbeddingsBackend::Ollama);
        assert_eq!(got.dimension, 1024);
        assert_eq!(got.context_length, 8192);
        assert_eq!(got.query_prefix, "query: ");
        Ok(())
    }

    #[tokio::test]
    async fn test_set_is_idempotent_upsert() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxIndexEmbeddingRepository::new(pool);
        repo.set_embedding_model(
            "idx",
            &test_info("old-model", EmbeddingsBackend::Ollama, 512),
        )
        .await?;
        let new_info = EmbeddingModelInfo {
            model: "new-model".to_owned(),
            backend: EmbeddingsBackend::OpenAi,
            dimension: 1536,
            context_length: 4096,
            query_prefix: "search: ".to_owned(),
        };
        repo.set_embedding_model("idx", &new_info).await?;
        let got = repo
            .get_embedding_model("idx")
            .await?
            .ok_or_else(|| anyhow::anyhow!("should exist"))?;
        assert_eq!(got.model, "new-model");
        assert_eq!(got.backend, EmbeddingsBackend::OpenAi);
        assert_eq!(got.dimension, 1536);
        assert_eq!(got.context_length, 4096);
        assert_eq!(got.query_prefix, "search: ");
        Ok(())
    }

    #[tokio::test]
    async fn test_clear_removes_record() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxIndexEmbeddingRepository::new(pool);
        repo.set_embedding_model("to_clear", &test_info("m", EmbeddingsBackend::Ollama, 64))
            .await?;
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
