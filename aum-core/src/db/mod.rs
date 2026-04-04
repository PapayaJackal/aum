//! Database layer for the aum workspace.
//!
//! Provides an abstract repository API (`JobRepository`, `JobErrorRepository`,
//! `IndexEmbeddingRepository`) backed by sqlx with support for SQLite,
//! PostgreSQL, and MySQL.
//!
//! # Usage
//!
//! ```no_run
//! use std::path::Path;
//! use std::sync::Arc;
//! use aum_core::db::{init_pool, SqlxJobRepository};
//!
//! # async fn example() {
//! let pool = init_pool("sqlite:data/aum.db", 16, Path::new("migrations"))
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

use std::time::Instant;

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

/// Record query count and duration metrics for a database operation.
pub(crate) fn record_db_metrics(
    op: &'static str,
    table: &'static str,
    start: Instant,
    is_ok: bool,
) {
    let status = if is_ok { "ok" } else { "error" };
    metrics::counter!("aum_db_queries_total",
        "operation" => op, "table" => table, "status" => status)
    .increment(1);
    metrics::histogram!("aum_db_query_duration_seconds",
        "operation" => op, "table" => table)
    .record(start.elapsed().as_secs_f64());
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
    sqlx::any::install_default_drivers();
    let pool = AnyPoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await?;
    sqlx::migrate::Migrator::new(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("migrations")
            .as_path(),
    )
    .await?
    .run(&pool)
    .await?;
    Ok(pool)
}
