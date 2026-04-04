//! Domain models for authentication and authorization.

use chrono::{DateTime, Utc};

/// A registered user.
#[derive(Debug, Clone)]
pub struct User {
    /// Auto-generated primary key.
    pub id: i64,
    /// Unique login name.
    pub username: String,
    /// Argon2id PHC-format hash, `None` for invitation-only accounts.
    pub password_hash: Option<String>,
    /// Whether the user has full administrative privileges.
    pub is_admin: bool,
    /// When the account was created.
    pub created_at: DateTime<Utc>,
}

/// An active server-side session (opaque token).
#[derive(Debug, Clone)]
pub struct Session {
    /// Base64url-encoded opaque token.
    pub token: String,
    /// ID of the owning [`User`].
    pub user_id: i64,
    /// When the session was created.
    pub created_at: DateTime<Utc>,
    /// When the session expires (auto-renewed on use).
    pub expires_at: DateTime<Utc>,
}

/// A one-time invitation token for user onboarding.
#[derive(Debug, Clone)]
pub struct Invitation {
    /// Auto-generated primary key.
    pub id: i64,
    /// Base64url-encoded opaque token.
    pub token: String,
    /// Username reserved for the invited user.
    pub username: String,
    /// Whether the invited user will be an admin.
    pub is_admin: bool,
    /// When the invitation was created.
    pub created_at: DateTime<Utc>,
    /// When the invitation expires.
    pub expires_at: DateTime<Utc>,
    /// When the invitation was redeemed, if ever.
    pub used_at: Option<DateTime<Utc>>,
}
