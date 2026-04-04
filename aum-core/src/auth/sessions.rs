//! sqlx implementation of [`SessionRepository`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::AnyPool;

use super::error::AuthResult;
use super::models::Session;
use super::repository::SessionRepository;
use crate::db::parse_dt;

#[derive(sqlx::FromRow)]
struct SessionRow {
    token: String,
    user_id: i64,
    created_at: String,
    expires_at: String,
}

impl From<SessionRow> for Session {
    fn from(r: SessionRow) -> Self {
        Session {
            token: r.token,
            user_id: r.user_id,
            created_at: parse_dt(&r.created_at),
            expires_at: parse_dt(&r.expires_at),
        }
    }
}

// ---------------------------------------------------------------------------
// Repository implementation
// ---------------------------------------------------------------------------

/// sqlx-backed implementation of [`SessionRepository`].
#[derive(Clone)]
pub struct SqlxSessionRepository {
    pool: AnyPool,
}

impl SqlxSessionRepository {
    /// Create a new repository backed by the given pool.
    #[must_use]
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl SessionRepository for SqlxSessionRepository {
    async fn create_session(
        &self,
        token: &str,
        user_id: i64,
        expires_at: DateTime<Utc>,
    ) -> AuthResult<Session> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let expires_str = expires_at.to_rfc3339();

        sqlx::query(
            "INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
        )
        .bind(token)
        .bind(user_id)
        .bind(&now_str)
        .bind(&expires_str)
        .execute(&self.pool)
        .await?;

        Ok(Session {
            token: token.to_owned(),
            user_id,
            created_at: now,
            expires_at,
        })
    }

    async fn get_session(&self, token: &str) -> AuthResult<Option<Session>> {
        let row: Option<SessionRow> = sqlx::query_as(
            "SELECT token, user_id, created_at, expires_at FROM sessions WHERE token = ?",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    async fn delete_session(&self, token: &str) -> AuthResult<()> {
        sqlx::query("DELETE FROM sessions WHERE token = ?")
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn delete_user_sessions(&self, user_id: i64) -> AuthResult<u64> {
        let result = sqlx::query("DELETE FROM sessions WHERE user_id = ?")
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }

    async fn renew_session(&self, token: &str, new_expires_at: DateTime<Utc>) -> AuthResult<()> {
        let expires_str = new_expires_at.to_rfc3339();
        sqlx::query("UPDATE sessions SET expires_at = ? WHERE token = ?")
            .bind(&expires_str)
            .bind(token)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn cleanup_expired(&self) -> AuthResult<u64> {
        let now = Utc::now().to_rfc3339();
        let result = sqlx::query("DELETE FROM sessions WHERE expires_at <= ?")
            .bind(&now)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::repository::UserRepository;
    use crate::auth::users::SqlxUserRepository;
    use crate::db::test_pool;
    use chrono::Duration;

    async fn setup() -> anyhow::Result<(SqlxSessionRepository, i64)> {
        let pool = test_pool().await?;
        let user_repo = SqlxUserRepository::new(pool.clone());
        let user = user_repo.insert_user("alice", Some("hash"), false).await?;
        Ok((SqlxSessionRepository::new(pool), user.id))
    }

    #[tokio::test]
    async fn test_create_and_get_session() -> anyhow::Result<()> {
        let (repo, user_id) = setup().await?;
        let expires = Utc::now() + Duration::hours(1);

        let session = repo.create_session("tok123", user_id, expires).await?;
        assert_eq!(session.token, "tok123");
        assert_eq!(session.user_id, user_id);

        let fetched = repo.get_session("tok123").await?;
        assert!(fetched.is_some());
        assert_eq!(
            fetched.as_ref().map(|s| &s.token),
            Some(&"tok123".to_owned())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_missing_session() -> anyhow::Result<()> {
        let (repo, _) = setup().await?;
        assert!(repo.get_session("nonexistent").await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_delete_session() -> anyhow::Result<()> {
        let (repo, user_id) = setup().await?;
        let expires = Utc::now() + Duration::hours(1);

        repo.create_session("tok1", user_id, expires).await?;
        repo.delete_session("tok1").await?;
        assert!(repo.get_session("tok1").await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_user_sessions() -> anyhow::Result<()> {
        let (repo, user_id) = setup().await?;
        let expires = Utc::now() + Duration::hours(1);

        repo.create_session("tok1", user_id, expires).await?;
        repo.create_session("tok2", user_id, expires).await?;

        let deleted = repo.delete_user_sessions(user_id).await?;
        assert_eq!(deleted, 2);
        assert!(repo.get_session("tok1").await?.is_none());
        assert!(repo.get_session("tok2").await?.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_cleanup_expired() -> anyhow::Result<()> {
        let (repo, user_id) = setup().await?;

        // One expired, one still valid.
        let past = Utc::now() - Duration::hours(1);
        let future = Utc::now() + Duration::hours(1);
        repo.create_session("expired", user_id, past).await?;
        repo.create_session("valid", user_id, future).await?;

        let cleaned = repo.cleanup_expired().await?;
        assert_eq!(cleaned, 1);
        assert!(repo.get_session("expired").await?.is_none());
        assert!(repo.get_session("valid").await?.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_renew_session() -> anyhow::Result<()> {
        let (repo, user_id) = setup().await?;
        let original_expires = Utc::now() + Duration::hours(1);

        repo.create_session("tok_renew", user_id, original_expires)
            .await?;

        // Renew with a later expiry.
        let new_expires = Utc::now() + Duration::hours(24);
        repo.renew_session("tok_renew", new_expires).await?;

        let session = repo
            .get_session("tok_renew")
            .await?
            .ok_or_else(|| anyhow::anyhow!("session should exist"))?;

        // The renewed expiry should be after the original.
        assert!(session.expires_at > original_expires);

        Ok(())
    }
}
