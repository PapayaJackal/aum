//! Authentication and authorization: local auth, sessions, invitations, permissions, rate limiting.

pub mod error;
pub mod invitations;
pub mod models;
pub mod password;
pub mod permissions;
pub mod rate_limit;
pub mod repository;
pub mod sessions;
pub mod users;

use std::sync::Arc;

use base64ct::{Base64UrlUnpadded, Encoding};
use chrono::{Duration, Utc};
use rand::RngExt;
use sqlx::AnyPool;
use tracing::{info, warn};

pub use error::{AuthError, AuthResult};
pub use invitations::SqlxInvitationRepository;
pub use models::{Invitation, Session, User};
pub use permissions::SqlxPermissionRepository;
pub use rate_limit::LoginRateLimiter;
pub use repository::{
    InvitationRepository, PermissionRepository, SessionRepository, UserRepository,
};
pub use sessions::SqlxSessionRepository;
pub use users::SqlxUserRepository;

/// Sentinel value returned by [`AuthService::list_user_indices`] for admin users,
/// indicating access to all indices.
pub const ADMIN_ALL_INDICES: &str = "*";

// ---------------------------------------------------------------------------
// AuthService facade
// ---------------------------------------------------------------------------

/// Unified facade for all authentication and authorization operations.
///
/// Coordinates password validation, hashing, session management, invitations,
/// permissions, and rate limiting. Analogous to [`crate::db::JobTracker`].
#[derive(Clone)]
pub struct AuthService {
    users: SqlxUserRepository,
    sessions: SqlxSessionRepository,
    invitations: SqlxInvitationRepository,
    permissions: SqlxPermissionRepository,
    rate_limiter: Arc<LoginRateLimiter>,
    password_min_length: u32,
    session_expire_hours: i64,
}

impl AuthService {
    /// Create a new `AuthService` backed by the given pool and configuration.
    #[must_use]
    pub fn new(pool: AnyPool, config: &crate::config::AuthConfig) -> Self {
        Self {
            users: SqlxUserRepository::new(pool.clone()),
            sessions: SqlxSessionRepository::new(pool.clone()),
            invitations: SqlxInvitationRepository::new(pool.clone()),
            permissions: SqlxPermissionRepository::new(pool),
            rate_limiter: Arc::new(LoginRateLimiter::new()),
            password_min_length: config.password_min_length,
            session_expire_hours: config.session_expire_hours,
        }
    }

    // -- User management ---------------------------------------------------

    /// Create a new user with a password.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::PasswordPolicy`] if the password is too weak,
    /// [`AuthError::UsernameTaken`] if the username already exists, or
    /// [`AuthError::Db`] on database failure.
    pub async fn create_user(
        &self,
        username: &str,
        password: &str,
        is_admin: bool,
    ) -> AuthResult<User> {
        password::validate_password(password, self.password_min_length)?;
        let hash = password::hash_password(password)?;
        let user = self
            .users
            .insert_user(username, Some(&hash), is_admin)
            .await?;
        info!(username, is_admin, "user created");
        Ok(user)
    }

    /// Fetch a user by ID.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn get_user(&self, id: i64) -> AuthResult<Option<User>> {
        self.users.get_user(id).await
    }

    /// Fetch a user by username.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn get_user_by_username(&self, username: &str) -> AuthResult<Option<User>> {
        self.users.get_user_by_username(username).await
    }

    /// List all users ordered by username.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn list_users(&self) -> AuthResult<Vec<User>> {
        self.users.list_users().await
    }

    /// Delete a user by username. Returns `true` if the user existed.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn delete_user(&self, username: &str) -> AuthResult<bool> {
        let deleted = self.users.delete_user(username).await?;
        if deleted {
            info!(username, "user deleted");
        }
        Ok(deleted)
    }

    /// Set a new password for a user. Validates against the password policy.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::PasswordPolicy`] if the password is too weak,
    /// [`AuthError::UserNotFound`] if the user does not exist, or
    /// [`AuthError::Db`] on database failure.
    pub async fn set_password(&self, username: &str, new_password: &str) -> AuthResult<bool> {
        password::validate_password(new_password, self.password_min_length)?;
        let user = self
            .users
            .get_user_by_username(username)
            .await?
            .ok_or_else(|| AuthError::UserNotFound(username.to_owned()))?;
        let hash = password::hash_password(new_password)?;
        self.users.update_password_hash(user.id, &hash).await?;
        info!(username, "password changed");
        Ok(true)
    }

    /// Set or revoke admin status. Returns `true` if the user existed.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn set_admin(&self, username: &str, is_admin: bool) -> AuthResult<bool> {
        let updated = self.users.set_admin(username, is_admin).await?;
        if updated {
            info!(username, is_admin, "admin status changed");
        }
        Ok(updated)
    }

    // -- Authentication ----------------------------------------------------

    /// Authenticate a user with username and password.
    ///
    /// Returns the user on success, or an appropriate error. Automatically
    /// re-hashes the password if Argon2 parameters have drifted.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::InvalidCredentials`] if the user does not exist
    /// or the password is wrong, [`AuthError::NoLocalPassword`] if the
    /// account has no password set, or [`AuthError::Db`] on database failure.
    pub async fn authenticate(&self, username: &str, password: &str) -> AuthResult<User> {
        let user = self
            .users
            .get_user_by_username(username)
            .await?
            .ok_or(AuthError::InvalidCredentials)?;

        let hash = user
            .password_hash
            .as_deref()
            .ok_or(AuthError::NoLocalPassword)?;

        let valid = password::verify_password(password, hash)?;
        if !valid {
            return Err(AuthError::InvalidCredentials);
        }

        // Re-hash if parameters have drifted.
        if password::needs_rehash(hash)
            && let Ok(new_hash) = password::hash_password(password)
            && let Err(e) = self.users.update_password_hash(user.id, &new_hash).await
        {
            warn!(username, error = %e, "failed to rehash password");
        }

        Ok(user)
    }

    // -- Sessions ----------------------------------------------------------

    /// Generate an opaque session token (48 random bytes, base64url-encoded).
    fn generate_token() -> String {
        let bytes: [u8; 48] = rand::rng().random();
        Base64UrlUnpadded::encode_string(&bytes)
    }

    /// Create a new session for a user with the default expiry.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn create_session(&self, user: &User) -> AuthResult<String> {
        let token = Self::generate_token();
        let expires_at = Utc::now() + Duration::hours(self.session_expire_hours);
        self.sessions
            .create_session(&token, user.id, expires_at)
            .await?;
        Ok(token)
    }

    /// Create a session with a custom expiry (in days). Used by `aum user token`.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn create_session_with_expiry_days(
        &self,
        user: &User,
        days: i64,
    ) -> AuthResult<String> {
        let token = Self::generate_token();
        let expires_at = Utc::now() + Duration::days(days);
        self.sessions
            .create_session(&token, user.id, expires_at)
            .await?;
        Ok(token)
    }

    /// Validate a session token and return the associated user if valid.
    ///
    /// Automatically renews the session expiry on each successful validation,
    /// so active sessions stay alive as long as they are being used.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn validate_session(&self, token: &str) -> AuthResult<Option<User>> {
        let Some(session) = self.sessions.get_session(token).await? else {
            return Ok(None);
        };

        if session.expires_at <= Utc::now() {
            // Expired — clean it up.
            let _ = self.sessions.delete_session(token).await;
            return Ok(None);
        }

        // Auto-renew: extend expiry from now.
        let new_expires = Utc::now() + Duration::hours(self.session_expire_hours);
        if let Err(e) = self.sessions.renew_session(token, new_expires).await {
            warn!(error = %e, "failed to renew session");
        }

        self.users.get_user(session.user_id).await
    }

    /// Delete a session (logout).
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn delete_session(&self, token: &str) -> AuthResult<()> {
        self.sessions.delete_session(token).await
    }

    /// Delete all sessions for a user.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn delete_user_sessions(&self, user_id: i64) -> AuthResult<u64> {
        self.sessions.delete_user_sessions(user_id).await
    }

    /// Clean up expired sessions.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn cleanup_expired_sessions(&self) -> AuthResult<u64> {
        self.sessions.cleanup_expired().await
    }

    // -- Invitations -------------------------------------------------------

    /// Create a one-time invitation for a new user.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn create_invitation(
        &self,
        username: &str,
        is_admin: bool,
        expires_hours: i64,
    ) -> AuthResult<Invitation> {
        let token = Self::generate_token();
        let expires_at = Utc::now() + Duration::hours(expires_hours);
        let invitation = self
            .invitations
            .create_invitation(&token, username, is_admin, expires_at)
            .await?;
        info!(username, is_admin, "invitation created");
        Ok(invitation)
    }

    /// Validate an invitation token without consuming it.
    ///
    /// Returns `Some(invitation)` if the token exists, is unused, and not expired.
    /// Returns `None` otherwise.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn validate_invitation(&self, token: &str) -> AuthResult<Option<Invitation>> {
        let invitation = self.invitations.get_invitation(token).await?;
        match invitation {
            Some(inv) if inv.used_at.is_none() && inv.expires_at > Utc::now() => Ok(Some(inv)),
            _ => Ok(None),
        }
    }

    /// Redeem an invitation: validate, create the user, and mark the invitation used.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::InvalidInvitation`] if the token is invalid, expired,
    /// or already used, [`AuthError::UsernameTaken`] if the username is taken,
    /// [`AuthError::PasswordPolicy`] if the password is too weak, or
    /// [`AuthError::Db`] on database failure.
    pub async fn redeem_invitation(&self, token: &str, password: &str) -> AuthResult<User> {
        let invitation = self
            .invitations
            .get_invitation(token)
            .await?
            .ok_or(AuthError::InvalidInvitation)?;

        // Must be unused and not expired.
        if invitation.used_at.is_some() || invitation.expires_at <= Utc::now() {
            return Err(AuthError::InvalidInvitation);
        }

        // Check username not already taken.
        if self
            .users
            .get_user_by_username(&invitation.username)
            .await?
            .is_some()
        {
            return Err(AuthError::UsernameTaken(invitation.username.clone()));
        }

        let user = self
            .create_user(&invitation.username, password, invitation.is_admin)
            .await?;

        self.invitations.mark_used(invitation.id).await?;
        info!(username = invitation.username, "invitation redeemed");

        Ok(user)
    }

    // -- Permissions -------------------------------------------------------

    /// Grant a user access to an index. Returns `true` if newly granted.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::UserNotFound`] if the user does not exist, or
    /// [`AuthError::Db`] on database failure.
    pub async fn grant_permission(&self, username: &str, index_name: &str) -> AuthResult<bool> {
        let user = self
            .users
            .get_user_by_username(username)
            .await?
            .ok_or_else(|| AuthError::UserNotFound(username.to_owned()))?;
        let granted = self.permissions.grant(user.id, index_name).await?;
        if granted {
            info!(username, index_name, "permission granted");
        }
        Ok(granted)
    }

    /// Revoke a user's access to an index. Returns `true` if was revoked.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::UserNotFound`] if the user does not exist, or
    /// [`AuthError::Db`] on database failure.
    pub async fn revoke_permission(&self, username: &str, index_name: &str) -> AuthResult<bool> {
        let user = self
            .users
            .get_user_by_username(username)
            .await?
            .ok_or_else(|| AuthError::UserNotFound(username.to_owned()))?;
        let revoked = self.permissions.revoke(user.id, index_name).await?;
        if revoked {
            info!(username, index_name, "permission revoked");
        }
        Ok(revoked)
    }

    /// Check if a user has access to an index. Admins always have access.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn check_permission(&self, user: &User, index_name: &str) -> AuthResult<bool> {
        if user.is_admin {
            return Ok(true);
        }
        self.permissions.check(user.id, index_name).await
    }

    /// List all index names a user can access. Returns `["*"]` for admins.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn list_user_indices(&self, user: &User) -> AuthResult<Vec<String>> {
        if user.is_admin {
            return Ok(vec![ADMIN_ALL_INDICES.to_owned()]);
        }
        self.permissions.list_user_indices(user.id).await
    }

    /// List all usernames with access to an index.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::Db`] on database failure.
    pub async fn list_index_users(&self, index_name: &str) -> AuthResult<Vec<String>> {
        self.permissions.list_index_users(index_name).await
    }

    // -- Rate limiting -----------------------------------------------------

    /// Check if an IP is rate-limited.
    ///
    /// # Errors
    ///
    /// Returns [`AuthError::RateLimited`] if the IP has exceeded the failure threshold.
    pub fn check_rate_limit(&self, client_ip: &str) -> AuthResult<()> {
        self.rate_limiter.check(client_ip)
    }

    /// Record a failed login attempt.
    pub fn record_login_failure(&self, client_ip: &str) {
        self.rate_limiter.record_failure(client_ip);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::AuthConfig;
    use crate::db::test_pool;

    async fn auth_service() -> anyhow::Result<AuthService> {
        let pool = test_pool().await?;
        let config = AuthConfig::default();
        Ok(AuthService::new(pool, &config))
    }

    #[tokio::test]
    async fn test_create_user_and_authenticate() -> anyhow::Result<()> {
        let svc = auth_service().await?;

        svc.create_user("alice", "Test1234!", true).await?;
        let user = svc.authenticate("alice", "Test1234!").await?;
        assert_eq!(user.username, "alice");
        assert!(user.is_admin);

        Ok(())
    }

    #[tokio::test]
    async fn test_authenticate_wrong_password() -> anyhow::Result<()> {
        let svc = auth_service().await?;

        svc.create_user("alice", "Test1234!", false).await?;
        let err = svc.authenticate("alice", "Wrong1234!").await;
        assert!(matches!(err, Err(AuthError::InvalidCredentials)));

        Ok(())
    }

    #[tokio::test]
    async fn test_authenticate_nonexistent_user() -> anyhow::Result<()> {
        let svc = auth_service().await?;

        let err = svc.authenticate("nobody", "Test1234!").await;
        assert!(matches!(err, Err(AuthError::InvalidCredentials)));

        Ok(())
    }

    #[tokio::test]
    async fn test_session_lifecycle() -> anyhow::Result<()> {
        let svc = auth_service().await?;

        let user = svc.create_user("alice", "Test1234!", false).await?;
        let token = svc.create_session(&user).await?;

        // Validate returns the user.
        let validated = svc.validate_session(&token).await?;
        assert!(validated.is_some());
        assert_eq!(
            validated.as_ref().map(|u| &u.username),
            Some(&"alice".to_owned())
        );

        // Delete and re-validate.
        svc.delete_session(&token).await?;
        let gone = svc.validate_session(&token).await?;
        assert!(gone.is_none());

        Ok(())
    }

    #[tokio::test]
    async fn test_invitation_flow() -> anyhow::Result<()> {
        let svc = auth_service().await?;

        let inv = svc.create_invitation("bob", false, 48).await?;
        let user = svc.redeem_invitation(&inv.token, "BobPass1!").await?;
        assert_eq!(user.username, "bob");
        assert!(!user.is_admin);

        // Cannot redeem again.
        let err = svc.redeem_invitation(&inv.token, "BobPass1!").await;
        assert!(matches!(err, Err(AuthError::InvalidInvitation)));

        Ok(())
    }

    #[tokio::test]
    async fn test_permission_check_admin_bypass() -> anyhow::Result<()> {
        let svc = auth_service().await?;

        let admin = svc.create_user("admin", "Admin123!", true).await?;
        assert!(svc.check_permission(&admin, "any_index").await?);

        let indices = svc.list_user_indices(&admin).await?;
        assert_eq!(indices, vec!["*"]);

        Ok(())
    }

    #[tokio::test]
    async fn test_permission_grant_revoke() -> anyhow::Result<()> {
        let svc = auth_service().await?;

        let user = svc.create_user("alice", "Test1234!", false).await?;
        assert!(!svc.check_permission(&user, "docs").await?);

        svc.grant_permission("alice", "docs").await?;
        assert!(svc.check_permission(&user, "docs").await?);

        svc.revoke_permission("alice", "docs").await?;
        assert!(!svc.check_permission(&user, "docs").await?);

        Ok(())
    }

    #[tokio::test]
    async fn test_set_password() -> anyhow::Result<()> {
        let svc = auth_service().await?;

        svc.create_user("alice", "OldPass1!", false).await?;
        svc.set_password("alice", "NewPass1!").await?;

        // Old password should fail.
        let err = svc.authenticate("alice", "OldPass1!").await;
        assert!(matches!(err, Err(AuthError::InvalidCredentials)));

        // New password should work.
        svc.authenticate("alice", "NewPass1!").await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_token_length() {
        // Verify generated tokens are 64 characters (48 bytes base64url-encoded).
        let token = AuthService::generate_token();
        assert_eq!(token.len(), 64);
    }

    #[tokio::test]
    async fn test_session_auto_renew() -> anyhow::Result<()> {
        let svc = auth_service().await?;

        let user = svc.create_user("alice", "Test1234!", false).await?;
        let token = svc.create_session(&user).await?;

        // Get the initial session expiry.
        let session_before = svc.sessions.get_session(&token).await?;
        let expires_before = session_before.as_ref().map(|s| s.expires_at);

        // Small sleep to ensure time progresses.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        // Validate should auto-renew.
        let validated = svc.validate_session(&token).await?;
        assert!(validated.is_some());

        // Check the expiry was extended.
        let session_after = svc.sessions.get_session(&token).await?;
        let expires_after = session_after.as_ref().map(|s| s.expires_at);
        assert!(
            expires_after > expires_before,
            "session expiry should be extended on validate"
        );

        Ok(())
    }
}
