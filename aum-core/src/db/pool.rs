//! Database pool initialisation for the aum db layer.

use std::path::Path;

use sqlx::any::{AnyPoolOptions, install_default_drivers};
use sqlx::{AnyPool, Executor as _};

use super::error::{DbError, DbResult};

/// Which database backend was detected from the connection URL.
#[derive(Clone, Copy)]
enum DbBackend {
    Sqlite,
    Postgres,
    MySql,
}

fn detect_backend(url: &str) -> DbResult<DbBackend> {
    if url.starts_with("sqlite:") {
        Ok(DbBackend::Sqlite)
    } else if url.starts_with("postgres:") || url.starts_with("postgresql:") {
        Ok(DbBackend::Postgres)
    } else if url.starts_with("mysql:") || url.starts_with("mariadb:") {
        Ok(DbBackend::MySql)
    } else {
        // Grab just the scheme portion for the error message.
        let scheme = url.split(':').next().unwrap_or(url);
        Err(DbError::UnsupportedBackend(scheme.to_owned()))
    }
}

#[allow(clippy::doc_markdown)] // SQLite, PostgreSQL, MySQL, MariaDB are proper nouns
/// Initialise a connection pool and run pending migrations.
///
/// The backend is determined from the URL scheme:
/// - `sqlite:` — SQLite with WAL mode and foreign-key enforcement
/// - `postgres:` / `postgresql:` — PostgreSQL
/// - `mysql:` / `mariadb:` — MySQL / MariaDB
///
/// `migrations_dir` must contain the SQL migration files for the selected
/// backend. The directory is resolved at runtime, not embedded at compile time,
/// so callers should pass an absolute path or a path relative to the process
/// working directory.
///
/// # Errors
/// Returns [`DbError`] if the pool cannot connect, PRAGMAs fail, or migrations fail.
pub async fn init_pool(
    url: &str,
    max_connections: u32,
    migrations_dir: &Path,
) -> DbResult<AnyPool> {
    install_default_drivers();

    let backend = detect_backend(url)?;

    let pool = AnyPoolOptions::new()
        .max_connections(max_connections)
        .after_connect(move |conn, _meta| {
            Box::pin(async move {
                if matches!(backend, DbBackend::Sqlite) {
                    conn.execute("PRAGMA journal_mode=WAL").await?;
                    conn.execute("PRAGMA foreign_keys=ON").await?;
                }
                Ok(())
            })
        })
        .connect(url)
        .await?;

    sqlx::migrate::Migrator::new(migrations_dir)
        .await?
        .run(&pool)
        .await?;

    tracing::info!(url = url, "database pool initialised");
    Ok(pool)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_init_pool_creates_sqlite_db_and_runs_migrations() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let db_path = dir.path().join("aum.db");
        let url = format!("sqlite:{}?mode=rwc", db_path.display());
        let migrations_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");

        let pool = init_pool(&url, 4, &migrations_dir).await?;

        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(row.0, 0);

        pool.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_unsupported_scheme_returns_error() {
        let migrations_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
        let result = init_pool("ftp://localhost/db", 1, &migrations_dir).await;
        assert!(matches!(result, Err(DbError::UnsupportedBackend(_))));
    }
}
