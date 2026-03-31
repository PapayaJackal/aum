//! Prometheus metrics helpers for the aum workspace.
//!
//! This module provides helpers for recording metrics using the [`metrics`] facade.
//! The actual exporter backend (e.g. `metrics-exporter-prometheus`) is installed by
//! the binary crate (`aum-api`) when the web server is initialised. Calling any
//! function here before a recorder is installed is safe — metrics are silently dropped.

/// Records process-level metrics that are constant for the lifetime of the process.
///
/// Emits the `aum_build_info` gauge set to `1.0`, labelled with the crate version
/// and the git commit hash (if set at compile time via the `GIT_COMMIT` environment
/// variable).
///
/// Call once at startup, after the recorder is installed. Safe to call with no
/// recorder installed — metrics are silently dropped.
pub fn record_build_info() {
    metrics::gauge!(
        "aum_build_info",
        "version" => env!("CARGO_PKG_VERSION"),
        "git_commit" => option_env!("GIT_COMMIT").unwrap_or("unknown"),
    )
    .set(1.0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_build_info_does_not_panic() {
        // With no recorder installed, metrics macros are no-ops.
        // This test asserts that the call is always safe.
        record_build_info();
    }
}
