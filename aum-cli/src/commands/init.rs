//! `aum init <INDEX>` — create or update a search index.

use clap::Args;

use aum_core::config::AumConfig;
use aum_core::search::SearchBackend;

#[derive(Args)]
pub struct InitArgs {
    /// Name of the index to initialise.
    pub index: String,
}

/// # Errors
///
/// Returns an error if the backend cannot be reached or the index cannot be created.
pub async fn run(
    args: &InitArgs,
    config: &AumConfig,
    backend: &dyn SearchBackend,
) -> anyhow::Result<()> {
    let vector_dimension = config
        .embeddings
        .enabled
        .then_some(config.embeddings.dimension);

    backend
        .initialize(&args.index, vector_dimension)
        .await
        .map_err(|e| anyhow::anyhow!("failed to initialise index '{}': {e}", args.index))?;

    let vector_info = match vector_dimension {
        Some(d) => format!("with {d}-dimension vectors"),
        None => "without vector search".to_owned(),
    };
    println!("Initialised index '{}' ({vector_info}).", args.index);
    Ok(())
}
