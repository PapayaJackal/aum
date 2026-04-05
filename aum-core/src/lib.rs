//! Core library for the aum document search server.

/// Configuration types, loading, and display logic.
pub mod config;

/// Logging initialisation from [`crate::config::LoggingConfig`].
pub mod log;

/// Database layer: connection pool, repository traits, and sqlx implementations.
pub mod db;

/// Domain models shared across the aum workspace.
pub mod models;

/// Document extraction pipeline: async Extractor trait and backend implementations.
pub mod extraction;

/// Async generic instance pool with health-aware round-robin and per-instance concurrency.
pub mod pool;

/// Human-readable name generator for job IDs.
pub mod names;

/// Document ingest pipeline: concurrent extraction, batching, and progress tracking.
pub mod ingest;

/// Search backend abstraction: Meilisearch client, types, and BatchSink/ExistenceChecker implementations.
pub mod search;

/// Embedding pipeline: backends (Ollama, OpenAI), text chunking, and orchestration.
pub mod embeddings;

/// Authentication and authorization: local auth, sessions, invitations, permissions, rate limiting.
pub mod auth;

/// Load config and initialise logging, printing errors to stderr and exiting on failure.
///
/// Call this at the very start of `main()` before any `tracing` macros.
///
/// # Panics / Exits
///
/// Calls [`std::process::exit`] with status 1 if config loading or logging setup fails.
#[must_use]
pub fn bootstrap() -> config::AumConfig {
    let config = config::load_config().unwrap_or_else(|e| {
        eprintln!("error: failed to load config: {e}");
        std::process::exit(1);
    });
    if let Err(e) = log::init(
        &config.log,
        tracing_subscriber::fmt::writer::BoxMakeWriter::new(std::io::stderr),
    ) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
    config
}

/// Open the database pool and run pending migrations, printing errors to stderr and exiting
/// on failure.
///
/// Call this after [`bootstrap`] once the async runtime is available. The migrations directory
/// is resolved relative to the `aum-core` crate at compile time, so no runtime path
/// configuration is required.
///
/// For `SQLite` URLs, the parent directory is created automatically if it does not exist, and
/// `?mode=rwc` is appended so `SQLite` will create the database file on first use.
///
/// # Panics / Exits
///
/// Calls [`std::process::exit`] with status 1 if the pool cannot be initialised or migrations
/// fail.
pub async fn bootstrap_db(config: &config::AumConfig) -> sqlx::AnyPool {
    static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

    let url = prepare_sqlite_url(&config.database_url());

    db::init_pool(&url, config.database.max_connections, &MIGRATOR)
        .await
        .unwrap_or_else(|e| {
            eprintln!("error: failed to open database: {e}");
            std::process::exit(1);
        })
}

/// For `sqlite:` URLs, creates the parent directory of the database file if needed and ensures
/// `?mode=rwc` is present so `SQLite` creates the file on first connection. Non-`SQLite` URLs are
/// returned unchanged. Exits the process on I/O failure.
fn prepare_sqlite_url(url: &str) -> String {
    let Some(rest) = url.strip_prefix("sqlite:") else {
        return url.to_owned();
    };
    let (file_part, query) = rest.split_once('?').unwrap_or((rest, ""));
    let path = std::path::Path::new(file_part);
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
        && let Err(e) = std::fs::create_dir_all(parent)
    {
        eprintln!(
            "error: failed to create data directory '{}': {e}",
            parent.display()
        );
        std::process::exit(1);
    }
    let mut params: Vec<&str> = query.split('&').filter(|s| !s.is_empty()).collect();
    if !params.iter().any(|p| p.starts_with("mode=")) {
        params.push("mode=rwc");
    }
    format!("sqlite:{}?{}", file_part, params.join("&"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_prepare_sqlite_url_adds_mode_rwc() {
        let url = prepare_sqlite_url("sqlite:aum.db");
        assert_eq!(url, "sqlite:aum.db?mode=rwc");
    }

    #[test]
    fn test_prepare_sqlite_url_preserves_existing_mode() {
        let url = prepare_sqlite_url("sqlite:aum.db?mode=ro");
        assert_eq!(url, "sqlite:aum.db?mode=ro");
    }

    #[test]
    fn test_prepare_sqlite_url_preserves_other_params() {
        let url = prepare_sqlite_url("sqlite:aum.db?cache=shared");
        assert_eq!(url, "sqlite:aum.db?cache=shared&mode=rwc");
    }

    #[test]
    fn test_prepare_sqlite_url_non_sqlite_passthrough() {
        let url = prepare_sqlite_url("postgres://user:pass@localhost/aum");
        assert_eq!(url, "postgres://user:pass@localhost/aum");
    }

    #[test]
    fn test_prepare_sqlite_url_creates_parent_dir() -> anyhow::Result<()> {
        let tmp = TempDir::new()?;
        let db_path = tmp.path().join("subdir").join("aum.db");
        let url = format!("sqlite:{}", db_path.display());

        prepare_sqlite_url(&url);

        assert!(tmp.path().join("subdir").is_dir());
        Ok(())
    }
}
