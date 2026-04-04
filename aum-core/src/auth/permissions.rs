//! sqlx implementation of [`PermissionRepository`].

use async_trait::async_trait;
use sqlx::AnyPool;

use super::error::AuthResult;
use super::repository::PermissionRepository;

// ---------------------------------------------------------------------------
// Repository implementation
// ---------------------------------------------------------------------------

/// sqlx-backed implementation of [`PermissionRepository`].
#[derive(Clone)]
pub struct SqlxPermissionRepository {
    pool: AnyPool,
}

impl SqlxPermissionRepository {
    /// Create a new repository backed by the given pool.
    #[must_use]
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl PermissionRepository for SqlxPermissionRepository {
    async fn grant(&self, user_id: i64, index_name: &str) -> AuthResult<bool> {
        // Check if already granted.
        let existing: Option<(i64,)> = sqlx::query_as(
            "SELECT id FROM user_index_permissions WHERE user_id = ? AND index_name = ?",
        )
        .bind(user_id)
        .bind(index_name)
        .fetch_optional(&self.pool)
        .await?;

        if existing.is_some() {
            return Ok(false);
        }

        sqlx::query("INSERT INTO user_index_permissions (user_id, index_name) VALUES (?, ?)")
            .bind(user_id)
            .bind(index_name)
            .execute(&self.pool)
            .await?;

        Ok(true)
    }

    async fn revoke(&self, user_id: i64, index_name: &str) -> AuthResult<bool> {
        let result =
            sqlx::query("DELETE FROM user_index_permissions WHERE user_id = ? AND index_name = ?")
                .bind(user_id)
                .bind(index_name)
                .execute(&self.pool)
                .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn check(&self, user_id: i64, index_name: &str) -> AuthResult<bool> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT id FROM user_index_permissions WHERE user_id = ? AND index_name = ?",
        )
        .bind(user_id)
        .bind(index_name)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    async fn list_user_indices(&self, user_id: i64) -> AuthResult<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT index_name FROM user_index_permissions WHERE user_id = ? ORDER BY index_name",
        )
        .bind(user_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(name,)| name).collect())
    }

    async fn list_index_users(&self, index_name: &str) -> AuthResult<Vec<String>> {
        let rows: Vec<(String,)> = sqlx::query_as(
            "SELECT u.username FROM users u \
             JOIN user_index_permissions p ON u.id = p.user_id \
             WHERE p.index_name = ? \
             ORDER BY u.username",
        )
        .bind(index_name)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(|(name,)| name).collect())
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

    async fn setup() -> anyhow::Result<(SqlxPermissionRepository, i64, i64)> {
        let pool = test_pool().await?;
        let user_repo = SqlxUserRepository::new(pool.clone());
        let alice = user_repo.insert_user("alice", Some("h"), false).await?;
        let bob = user_repo.insert_user("bob", Some("h"), false).await?;
        Ok((SqlxPermissionRepository::new(pool), alice.id, bob.id))
    }

    #[tokio::test]
    async fn test_grant_and_check() -> anyhow::Result<()> {
        let (repo, alice_id, _) = setup().await?;

        assert!(repo.grant(alice_id, "docs").await?);
        assert!(repo.check(alice_id, "docs").await?);
        assert!(!repo.check(alice_id, "other").await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_grant_idempotent() -> anyhow::Result<()> {
        let (repo, alice_id, _) = setup().await?;

        assert!(repo.grant(alice_id, "docs").await?);
        assert!(!repo.grant(alice_id, "docs").await?); // already granted

        Ok(())
    }

    #[tokio::test]
    async fn test_revoke() -> anyhow::Result<()> {
        let (repo, alice_id, _) = setup().await?;

        repo.grant(alice_id, "docs").await?;
        assert!(repo.revoke(alice_id, "docs").await?);
        assert!(!repo.check(alice_id, "docs").await?);
        assert!(!repo.revoke(alice_id, "docs").await?); // already revoked

        Ok(())
    }

    #[tokio::test]
    async fn test_list_user_indices() -> anyhow::Result<()> {
        let (repo, alice_id, _) = setup().await?;

        repo.grant(alice_id, "docs").await?;
        repo.grant(alice_id, "email").await?;

        let indices = repo.list_user_indices(alice_id).await?;
        assert_eq!(indices, vec!["docs", "email"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_index_users() -> anyhow::Result<()> {
        let (repo, alice_id, bob_id) = setup().await?;

        repo.grant(alice_id, "docs").await?;
        repo.grant(bob_id, "docs").await?;

        let users = repo.list_index_users("docs").await?;
        assert_eq!(users, vec!["alice", "bob"]); // ordered by username

        Ok(())
    }
}
