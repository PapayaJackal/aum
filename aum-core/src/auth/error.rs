//! Authentication error types.

/// Errors that can occur during authentication and authorization operations.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// An error propagated from the database layer.
    #[error("database error: {0}")]
    Db(#[from] sqlx::Error),

    /// The username/password combination was incorrect.
    #[error("invalid username or password")]
    InvalidCredentials,

    /// The account exists but has no local password set (e.g. invitation-only).
    #[error("account has no local password")]
    NoLocalPassword,

    /// The requested user does not exist.
    #[error("user not found: {0}")]
    UserNotFound(String),

    /// A user with this username already exists.
    #[error("username already taken: {0}")]
    UsernameTaken(String),

    /// The invitation token is invalid, expired, or already used.
    #[error("invalid or expired invitation")]
    InvalidInvitation,

    /// The session token is expired or does not exist.
    #[error("session expired or invalid")]
    InvalidSession,

    /// Too many failed login attempts from this IP.
    #[error("too many failed login attempts, try again later")]
    RateLimited,

    /// The password does not meet the configured policy requirements.
    #[error("password policy violation: {0}")]
    PasswordPolicy(String),
}

/// Shorthand result type for auth operations.
pub type AuthResult<T> = Result<T, AuthError>;
