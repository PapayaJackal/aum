//! Database layer for the aum workspace.
//!
//! Provides an abstract repository API (`JobRepository`, `JobErrorRepository`,
//! `IndexEmbeddingRepository`) backed by sqlx with support for SQLite,
//! PostgreSQL, and MySQL.
//!
//! # Usage
//!
//! ```no_run
//! use std::sync::Arc;
//! use aum_core::db::{init_pool, SqlxJobRepository};
//!
//! # async fn example() {
//! static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
//! let pool = init_pool("sqlite:data/aum.db", 16, &MIGRATOR)
//!     .await
//!     .expect("db init");
//! let jobs = Arc::new(SqlxJobRepository::new(pool));
//! # }
//! ```

pub mod embeddings;
pub mod error;
pub mod errors;
pub mod jobs;
pub mod pool;
pub mod repository;
pub mod tracker;

use chrono::{DateTime, Utc};

/// Parse an RFC 3339 timestamp stored in the database into a `DateTime<Utc>`.
///
/// Panics if the string is not valid RFC 3339 — this is intentional because
/// all timestamps are written by the application and corruption is a bug.
pub(crate) fn parse_dt(s: &str) -> DateTime<Utc> {
    #[allow(clippy::expect_used)]
    DateTime::parse_from_rfc3339(s)
        .expect("timestamps in the database are always valid RFC3339")
        .with_timezone(&Utc)
}

pub use embeddings::SqlxIndexEmbeddingRepository;
pub use error::{DbError, DbResult};
pub use errors::SqlxJobErrorRepository;
pub use jobs::SqlxJobRepository;
pub use pool::init_pool;
pub use repository::{IndexEmbeddingRepository, JobErrorRepository, JobRepository};
pub use tracker::JobTracker;

// ---------------------------------------------------------------------------
// Test helper
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) async fn test_pool() -> anyhow::Result<sqlx::AnyPool> {
    use sqlx::any::AnyPoolOptions;
    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    MIGRATOR.run(&pool).await?;
    Ok(pool)
}
