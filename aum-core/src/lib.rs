//! Core library for the aum document search server.

/// Configuration types, loading, and display logic.
pub mod config;

/// Logging initialisation from [`crate::config::LoggingConfig`].
pub mod log;

/// Domain models shared across the aum workspace.
pub mod models;

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
    config
}
