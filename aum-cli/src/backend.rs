//! Search backend construction helper.

use anyhow::Context as _;

use aum_core::config::AumConfig;
use aum_core::search::AumBackend;

/// Create the search backend selected by `config.search_backend`.
///
/// # Errors
///
/// Returns an error if the selected backend is not compiled into this binary,
/// or if the client cannot be constructed (e.g. invalid URL).
pub fn create_backend(config: &AumConfig) -> anyhow::Result<AumBackend> {
    AumBackend::from_config(config).context("failed to create search backend")
}
