//! Database error type for the aum db layer.

/// Error type for all database operations.
#[derive(Debug, thiserror::Error)]
pub enum DbError {
    /// Underlying sqlx error (query failure, connection error, etc.).
    #[error("database error: {0}")]
    Sqlx(#[from] sqlx::Error),
    /// Migration failed.
    #[error("migration error: {0}")]
    Migrate(#[from] sqlx::migrate::MigrateError),
    /// The connection URL uses a scheme that is not supported.
    #[error("unsupported database URL scheme: {0}")]
    UnsupportedBackend(String),
    /// A required record was not found.
    #[error("record not found: {0}")]
    NotFound(String),
}

/// Convenience alias for `Result<T, DbError>`.
pub type DbResult<T> = Result<T, DbError>;
