//! Database pool initialisation for the aum db layer.

use sqlx::any::{AnyPoolOptions, install_default_drivers};
use sqlx::migrate::Migrator;
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
/// Pass the static migrator produced by `sqlx::migrate!("./migrations")`.
///
/// The backend is determined from the URL scheme:
/// - `sqlite:` — SQLite with WAL mode and foreign-key enforcement
/// - `postgres:` / `postgresql:` — PostgreSQL
/// - `mysql:` / `mariadb:` — MySQL / MariaDB
///
/// The `migrator` should be the static value from `sqlx::migrate!("./migrations")`.
/// Migration SQL is embedded at compile time via the proc-macro.
///
/// # Errors
/// Returns [`DbError`] if the pool cannot connect, PRAGMAs fail, or migrations fail.
pub async fn init_pool(url: &str, max_connections: u32, migrator: &Migrator) -> DbResult<AnyPool> {
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

    migrator.run(&pool).await?;

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

    static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

    #[tokio::test]
    async fn test_init_pool_creates_sqlite_db_and_runs_migrations() -> anyhow::Result<()> {
        let dir = TempDir::new()?;
        let db_path = dir.path().join("aum.db");
        let url = format!("sqlite:{}?mode=rwc", db_path.display());

        let pool = init_pool(&url, 4, &MIGRATOR).await?;

        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM jobs")
            .fetch_one(&pool)
            .await?;
        assert_eq!(row.0, 0);

        pool.close().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_unsupported_scheme_returns_error() {
        let result = init_pool("ftp://localhost/db", 1, &MIGRATOR).await;
        assert!(matches!(result, Err(DbError::UnsupportedBackend(_))));
    }
}
