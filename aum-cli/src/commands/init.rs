//! `aum init <INDEX>` — create or update a search index.

use clap::Args;

use aum_core::config::AumConfig;
use aum_core::search::AumBackend;

use crate::ingest_common::initialize_backend;

#[derive(Args)]
pub struct InitArgs {
    /// Name of the index to initialise.
    pub index: String,
}

/// # Errors
///
/// Returns an error if the backend cannot be reached or the index cannot be created.
pub async fn run(args: &InitArgs, config: &AumConfig, backend: &AumBackend) -> anyhow::Result<()> {
    initialize_backend(backend, config, &args.index).await?;

    let vector_info = if config.embeddings.enabled {
        format!("with {}-dimension vectors", config.embeddings.dimension)
    } else {
        "without vector search".to_owned()
    };
    println!("Initialised index '{}' ({vector_info}).", args.index);
    Ok(())
}
