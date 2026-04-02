//! Core library for the aum document search server.

/// Configuration types, loading, and display logic.
pub mod config;

/// Logging initialisation from [`crate::config::LoggingConfig`].
pub mod log;

/// Prometheus metrics helpers for the aum workspace.
pub mod metrics;

/// Database layer: connection pool, repository traits, and sqlx implementations.
pub mod db;

/// Domain models shared across the aum workspace.
pub mod models;

/// Document extraction pipeline: async Extractor trait and backend implementations.
pub mod extraction;

/// Async generic instance pool with health-aware round-robin and per-instance concurrency.
pub mod pool;

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
    if let Err(e) = log::init(&config.log) {
        eprintln!("error: {e}");
        std::process::exit(1);
    }
    metrics::record_build_info();
    config
}

/// Open the database pool and run pending migrations, printing errors to stderr and exiting
/// on failure.
///
/// Call this after [`bootstrap`] once the async runtime is available. The migrations directory
/// is resolved relative to the `aum-core` crate at compile time, so no runtime path
/// configuration is required.
///
/// # Panics / Exits
///
/// Calls [`std::process::exit`] with status 1 if the pool cannot be initialised or migrations
/// fail.
pub async fn bootstrap_db(config: &config::AumConfig) -> sqlx::AnyPool {
    let migrations_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("migrations");
    db::init_pool(
        &config.database_url(),
        config.database.max_connections,
        &migrations_dir,
    )
    .await
    .unwrap_or_else(|e| {
        eprintln!("error: failed to open database: {e}");
        std::process::exit(1);
    })
}
