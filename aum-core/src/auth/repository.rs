//! Abstract repository traits for the auth database layer.

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use super::error::AuthResult;
use super::models::{Invitation, Session, User};

// ---------------------------------------------------------------------------
// UserRepository
// ---------------------------------------------------------------------------

/// Persistent storage for [`User`] records.
#[async_trait]
pub trait UserRepository: Send + Sync {
    /// Insert a new user and return the created record.
    async fn insert_user(
        &self,
        username: &str,
        password_hash: Option<&str>,
        is_admin: bool,
    ) -> AuthResult<User>;

    /// Fetch a user by ID.
    async fn get_user(&self, id: i64) -> AuthResult<Option<User>>;

    /// Fetch a user by username.
    async fn get_user_by_username(&self, username: &str) -> AuthResult<Option<User>>;

    /// List all users ordered by username.
    async fn list_users(&self) -> AuthResult<Vec<User>>;

    /// Delete a user by username. Returns `true` if the user existed.
    async fn delete_user(&self, username: &str) -> AuthResult<bool>;

    /// Update the password hash for a user.
    async fn update_password_hash(&self, user_id: i64, hash: &str) -> AuthResult<()>;

    /// Set or revoke admin status. Returns `true` if the user existed.
    async fn set_admin(&self, username: &str, is_admin: bool) -> AuthResult<bool>;
}

// ---------------------------------------------------------------------------
// SessionRepository
// ---------------------------------------------------------------------------

/// Persistent storage for server-side [`Session`] records.
#[async_trait]
pub trait SessionRepository: Send + Sync {
    /// Insert a new session.
    async fn create_session(
        &self,
        token: &str,
        user_id: i64,
        expires_at: DateTime<Utc>,
    ) -> AuthResult<Session>;

    /// Look up a session by token.
    async fn get_session(&self, token: &str) -> AuthResult<Option<Session>>;

    /// Delete a single session.
    async fn delete_session(&self, token: &str) -> AuthResult<()>;

    /// Delete all sessions for a user. Returns the number deleted.
    async fn delete_user_sessions(&self, user_id: i64) -> AuthResult<u64>;

    /// Extend a session's expiry to a new timestamp.
    async fn renew_session(&self, token: &str, new_expires_at: DateTime<Utc>) -> AuthResult<()>;

    /// Delete all expired sessions. Returns the number deleted.
    async fn cleanup_expired(&self) -> AuthResult<u64>;
}

// ---------------------------------------------------------------------------
// InvitationRepository
// ---------------------------------------------------------------------------

/// Persistent storage for [`Invitation`] records.
#[async_trait]
pub trait InvitationRepository: Send + Sync {
    /// Insert a new invitation.
    async fn create_invitation(
        &self,
        token: &str,
        username: &str,
        is_admin: bool,
        expires_at: DateTime<Utc>,
    ) -> AuthResult<Invitation>;

    /// Look up an invitation by token.
    async fn get_invitation(&self, token: &str) -> AuthResult<Option<Invitation>>;

    /// Mark an invitation as used.
    async fn mark_used(&self, id: i64) -> AuthResult<()>;
}

// ---------------------------------------------------------------------------
// PermissionRepository
// ---------------------------------------------------------------------------

/// Persistent storage for user-to-index access permissions.
#[async_trait]
pub trait PermissionRepository: Send + Sync {
    /// Grant a user access to an index. Returns `true` if newly granted.
    async fn grant(&self, user_id: i64, index_name: &str) -> AuthResult<bool>;

    /// Revoke a user's access to an index. Returns `true` if was revoked.
    async fn revoke(&self, user_id: i64, index_name: &str) -> AuthResult<bool>;

    /// Check if a user has explicit access to an index (ignoring admin status).
    async fn check(&self, user_id: i64, index_name: &str) -> AuthResult<bool>;

    /// List all index names a user has explicit access to.
    async fn list_user_indices(&self, user_id: i64) -> AuthResult<Vec<String>>;

    /// List all usernames with access to an index.
    async fn list_index_users(&self, index_name: &str) -> AuthResult<Vec<String>>;
}
