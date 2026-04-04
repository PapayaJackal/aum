//! Logging initialisation for the aum workspace.
//!
//! Call [`init`] once at the start of `main()`, before any tracing macros.

use std::fmt;

use tracing_subscriber::{
    EnvFilter, fmt::writer::BoxMakeWriter, layer::SubscriberExt as _, util::SubscriberInitExt as _,
};

use crate::config::{LogFormat, LogLevel, LoggingConfig};

/// Error returned when the global tracing subscriber cannot be installed.
#[derive(Debug)]
pub struct LoggingInitError(String);

impl fmt::Display for LoggingInitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "failed to initialize logging: {}", self.0)
    }
}

impl std::error::Error for LoggingInitError {}

pub(crate) fn to_tracing_level(level: LogLevel) -> tracing::Level {
    match level {
        LogLevel::Debug => tracing::Level::DEBUG,
        LogLevel::Info => tracing::Level::INFO,
        LogLevel::Warn => tracing::Level::WARN,
        LogLevel::Error => tracing::Level::ERROR,
    }
}

fn build_filter(level: LogLevel) -> EnvFilter {
    // RUST_LOG overrides the config-specified level at runtime; invalid directives are ignored.
    EnvFilter::builder()
        .with_default_directive(to_tracing_level(level).into())
        .from_env_lossy()
}

/// Initialise the global tracing subscriber from `config`.
///
/// `writer` controls where formatted log lines are sent.  Pass
/// `BoxMakeWriter::new(std::io::stderr)` for the default behaviour, or wrap
/// an indicatif `MultiProgress` writer to keep progress bars intact.
///
/// Must be called once, early in `main()`, before any `tracing` macros are used.
/// The log level can be overridden at runtime with the `RUST_LOG` environment variable.
///
/// # Errors
///
/// Returns [`LoggingInitError`] if a global subscriber has already been set.
pub fn init(config: &LoggingConfig, writer: BoxMakeWriter) -> Result<(), LoggingInitError> {
    let filter = build_filter(config.level);

    let fmt_layer: Box<dyn tracing_subscriber::Layer<_> + Send + Sync> = match config.format {
        LogFormat::Console => Box::new(
            tracing_subscriber::fmt::layer()
                .compact()
                .with_writer(writer),
        ),
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt::layer()
                .json()
                .with_current_span(true)
                .with_span_list(false)
                .with_writer(writer),
        ),
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt_layer)
        .try_init()
        .map_err(|e| LoggingInitError(e.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::LoggingConfig;

    #[test]
    fn level_mapping_covers_all_variants() {
        assert_eq!(to_tracing_level(LogLevel::Debug), tracing::Level::DEBUG);
        assert_eq!(to_tracing_level(LogLevel::Info), tracing::Level::INFO);
        assert_eq!(to_tracing_level(LogLevel::Warn), tracing::Level::WARN);
        assert_eq!(to_tracing_level(LogLevel::Error), tracing::Level::ERROR);
    }

    #[test]
    fn logging_init_error_display() {
        let err = LoggingInitError("subscriber already set".to_owned());
        assert_eq!(
            err.to_string(),
            "failed to initialize logging: subscriber already set"
        );
    }

    fn stderr_writer() -> tracing_subscriber::fmt::writer::BoxMakeWriter {
        tracing_subscriber::fmt::writer::BoxMakeWriter::new(std::io::stderr)
    }

    #[test]
    fn init_errors_on_second_call() {
        let config = LoggingConfig::default();
        // First call may succeed or fail (another test may have already set the global subscriber).
        let _ = init(&config, stderr_writer());
        // Second call must always fail because the subscriber is now set.
        assert!(init(&config, stderr_writer()).is_err());
    }
}
