//! sqlx implementation of [`InvitationRepository`].

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use sqlx::AnyPool;

use super::error::AuthResult;
use super::models::Invitation;
use super::repository::InvitationRepository;
use crate::db::parse_dt;

#[derive(sqlx::FromRow)]
struct InvitationRow {
    id: i64,
    token: String,
    username: String,
    is_admin: i64,
    created_at: String,
    expires_at: String,
    used_at: Option<String>,
}

impl From<InvitationRow> for Invitation {
    fn from(r: InvitationRow) -> Self {
        Invitation {
            id: r.id,
            token: r.token,
            username: r.username,
            is_admin: r.is_admin != 0,
            created_at: parse_dt(&r.created_at),
            expires_at: parse_dt(&r.expires_at),
            used_at: r.used_at.as_deref().map(parse_dt),
        }
    }
}

// ---------------------------------------------------------------------------
// Repository implementation
// ---------------------------------------------------------------------------

/// sqlx-backed implementation of [`InvitationRepository`].
#[derive(Clone)]
pub struct SqlxInvitationRepository {
    pool: AnyPool,
}

impl SqlxInvitationRepository {
    /// Create a new repository backed by the given pool.
    #[must_use]
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl InvitationRepository for SqlxInvitationRepository {
    async fn create_invitation(
        &self,
        token: &str,
        username: &str,
        is_admin: bool,
        expires_at: DateTime<Utc>,
    ) -> AuthResult<Invitation> {
        let now = Utc::now();
        let now_str = now.to_rfc3339();
        let expires_str = expires_at.to_rfc3339();
        let admin_val: i64 = i64::from(is_admin);

        sqlx::query(
            "INSERT INTO invitations (token, username, is_admin, created_at, expires_at) \
             VALUES (?, ?, ?, ?, ?)",
        )
        .bind(token)
        .bind(username)
        .bind(admin_val)
        .bind(&now_str)
        .bind(&expires_str)
        .execute(&self.pool)
        .await?;

        // Fetch back to get the auto-generated id.
        let row: InvitationRow = sqlx::query_as(
            "SELECT id, token, username, is_admin, created_at, expires_at, used_at \
             FROM invitations WHERE token = ?",
        )
        .bind(token)
        .fetch_one(&self.pool)
        .await?;

        Ok(row.into())
    }

    async fn get_invitation(&self, token: &str) -> AuthResult<Option<Invitation>> {
        let row: Option<InvitationRow> = sqlx::query_as(
            "SELECT id, token, username, is_admin, created_at, expires_at, used_at \
             FROM invitations WHERE token = ?",
        )
        .bind(token)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    async fn mark_used(&self, id: i64) -> AuthResult<()> {
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE invitations SET used_at = ? WHERE id = ?")
            .bind(&now)
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::test_pool;
    use chrono::Duration;

    #[tokio::test]
    async fn test_create_and_get_invitation() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxInvitationRepository::new(pool);
        let expires = Utc::now() + Duration::hours(48);

        let inv = repo
            .create_invitation("tok_abc", "alice", false, expires)
            .await?;
        assert_eq!(inv.token, "tok_abc");
        assert_eq!(inv.username, "alice");
        assert!(!inv.is_admin);
        assert!(inv.used_at.is_none());

        let fetched = repo.get_invitation("tok_abc").await?;
        assert!(fetched.is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_get_missing_invitation() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxInvitationRepository::new(pool);
        assert!(repo.get_invitation("nonexistent").await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn test_mark_used() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxInvitationRepository::new(pool);
        let expires = Utc::now() + Duration::hours(48);

        let inv = repo.create_invitation("tok1", "bob", true, expires).await?;
        assert!(inv.used_at.is_none());

        repo.mark_used(inv.id).await?;

        let fetched = repo.get_invitation("tok1").await?;
        assert!(fetched.is_some());
        assert!(fetched.as_ref().and_then(|i| i.used_at).is_some());

        Ok(())
    }

    #[tokio::test]
    async fn test_create_admin_invitation() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxInvitationRepository::new(pool);
        let expires = Utc::now() + Duration::hours(48);

        let inv = repo
            .create_invitation("tok_admin", "admin_user", true, expires)
            .await?;
        assert!(inv.is_admin);

        Ok(())
    }
}
