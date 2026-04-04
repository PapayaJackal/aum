//! sqlx implementation of [`UserRepository`].

use async_trait::async_trait;
use chrono::Utc;
use sqlx::AnyPool;

use super::error::{AuthError, AuthResult};
use super::models::User;
use super::repository::UserRepository;
use crate::db::parse_dt;

#[derive(sqlx::FromRow)]
struct UserRow {
    id: i64,
    username: String,
    password_hash: Option<String>,
    is_admin: i64,
    created_at: String,
}

impl From<UserRow> for User {
    fn from(r: UserRow) -> Self {
        User {
            id: r.id,
            username: r.username,
            password_hash: r.password_hash,
            is_admin: r.is_admin != 0,
            created_at: parse_dt(&r.created_at),
        }
    }
}

// ---------------------------------------------------------------------------
// Repository implementation
// ---------------------------------------------------------------------------

/// sqlx-backed implementation of [`UserRepository`].
#[derive(Clone)]
pub struct SqlxUserRepository {
    pool: AnyPool,
}

impl SqlxUserRepository {
    /// Create a new repository backed by the given pool.
    #[must_use]
    pub fn new(pool: AnyPool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl UserRepository for SqlxUserRepository {
    async fn insert_user(
        &self,
        username: &str,
        password_hash: Option<&str>,
        is_admin: bool,
    ) -> AuthResult<User> {
        let now = Utc::now().to_rfc3339();
        let admin_val: i64 = i64::from(is_admin);

        // Check for duplicate username first (sqlx::Any doesn't surface
        // constraint-violation errors uniformly across backends).
        let existing: Option<UserRow> =
            sqlx::query_as("SELECT id, username, password_hash, is_admin, created_at FROM users WHERE username = ?")
                .bind(username)
                .fetch_optional(&self.pool)
                .await?;
        if existing.is_some() {
            return Err(AuthError::UsernameTaken(username.to_owned()));
        }

        sqlx::query(
            "INSERT INTO users (username, password_hash, is_admin, created_at) VALUES (?, ?, ?, ?)",
        )
        .bind(username)
        .bind(password_hash)
        .bind(admin_val)
        .bind(&now)
        .execute(&self.pool)
        .await?;

        // Fetch the inserted row to get the auto-generated id.
        let row: UserRow =
            sqlx::query_as("SELECT id, username, password_hash, is_admin, created_at FROM users WHERE username = ?")
                .bind(username)
                .fetch_one(&self.pool)
                .await?;

        Ok(row.into())
    }

    async fn get_user(&self, id: i64) -> AuthResult<Option<User>> {
        let row: Option<UserRow> = sqlx::query_as(
            "SELECT id, username, password_hash, is_admin, created_at FROM users WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(Into::into))
    }

    async fn get_user_by_username(&self, username: &str) -> AuthResult<Option<User>> {
        let row: Option<UserRow> =
            sqlx::query_as("SELECT id, username, password_hash, is_admin, created_at FROM users WHERE username = ?")
                .bind(username)
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.map(Into::into))
    }

    async fn list_users(&self) -> AuthResult<Vec<User>> {
        let rows: Vec<UserRow> = sqlx::query_as(
            "SELECT id, username, password_hash, is_admin, created_at FROM users ORDER BY username",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.into_iter().map(Into::into).collect())
    }

    async fn delete_user(&self, username: &str) -> AuthResult<bool> {
        let result = sqlx::query("DELETE FROM users WHERE username = ?")
            .bind(username)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn update_password_hash(&self, user_id: i64, hash: &str) -> AuthResult<()> {
        sqlx::query("UPDATE users SET password_hash = ? WHERE id = ?")
            .bind(hash)
            .bind(user_id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn set_admin(&self, username: &str, is_admin: bool) -> AuthResult<bool> {
        let admin_val: i64 = i64::from(is_admin);
        let result = sqlx::query("UPDATE users SET is_admin = ? WHERE username = ?")
            .bind(admin_val)
            .bind(username)
            .execute(&self.pool)
            .await?;
        Ok(result.rows_affected() > 0)
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
    async fn test_insert_and_get_user() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxUserRepository::new(pool);

        let user = repo.insert_user("alice", Some("hash123"), false).await?;
        assert_eq!(user.username, "alice");
        assert_eq!(user.password_hash.as_deref(), Some("hash123"));
        assert!(!user.is_admin);

        let fetched = repo.get_user(user.id).await?;
        assert!(fetched.is_some());
        assert_eq!(
            fetched.as_ref().map(|u| &u.username),
            Some(&"alice".to_owned())
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_get_user_by_username() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxUserRepository::new(pool);

        repo.insert_user("bob", Some("hash"), true).await?;
        let user = repo.get_user_by_username("bob").await?;
        assert!(user.is_some());
        assert!(user.as_ref().is_some_and(|u| u.is_admin));

        let missing = repo.get_user_by_username("nobody").await?;
        assert!(missing.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_duplicate_username() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxUserRepository::new(pool);

        repo.insert_user("alice", Some("hash"), false).await?;
        let err = repo.insert_user("alice", Some("hash2"), false).await;
        assert!(matches!(err, Err(AuthError::UsernameTaken(_))));

        Ok(())
    }

    #[tokio::test]
    async fn test_list_users() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxUserRepository::new(pool);

        repo.insert_user("charlie", Some("h"), false).await?;
        repo.insert_user("alice", Some("h"), true).await?;
        let users = repo.list_users().await?;
        assert_eq!(users.len(), 2);
        assert_eq!(users[0].username, "alice"); // ordered by username
        assert_eq!(users[1].username, "charlie");

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_user() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxUserRepository::new(pool);

        repo.insert_user("alice", Some("h"), false).await?;
        assert!(repo.delete_user("alice").await?);
        assert!(!repo.delete_user("alice").await?); // already deleted

        Ok(())
    }

    #[tokio::test]
    async fn test_update_password_hash() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxUserRepository::new(pool);

        let user = repo.insert_user("alice", Some("old"), false).await?;
        repo.update_password_hash(user.id, "new").await?;

        let updated = repo.get_user(user.id).await?;
        assert_eq!(
            updated.as_ref().and_then(|u| u.password_hash.as_deref()),
            Some("new")
        );

        Ok(())
    }

    #[tokio::test]
    async fn test_set_admin() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxUserRepository::new(pool);

        repo.insert_user("alice", Some("h"), false).await?;
        assert!(repo.set_admin("alice", true).await?);

        let user = repo.get_user_by_username("alice").await?;
        assert!(user.is_some_and(|u| u.is_admin));

        assert!(!repo.set_admin("nobody", true).await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_insert_user_without_password() -> anyhow::Result<()> {
        let pool = test_pool().await?;
        let repo = SqlxUserRepository::new(pool);

        let user = repo.insert_user("invite_user", None, false).await?;
        assert!(user.password_hash.is_none());

        Ok(())
    }
}
