//! `aum embed <INDEX>` — generate embeddings for documents in a search index.

use std::sync::Arc;

use anyhow::Context as _;
use clap::Args;
use owo_colors::OwoColorize as _;

use aum_core::config::AumConfig;
use aum_core::db::JobTracker;
use aum_core::embeddings::{EmbedPipeline, EmbedSnapshot};
use aum_core::search::{AumBackend, SearchBackend as _};

use crate::ingest_common::{
    acquire_embed_lock, build_embedder_pool, embedding_model_info, initialize_backend,
    render_embed_progress,
};
use crate::output::print_job_summary;

#[derive(Args)]
pub struct EmbedArgs {
    /// Name of the search index to embed documents in.
    pub index: String,
    /// Number of documents per scroll batch (overrides config).
    #[arg(long)]
    pub batch_size: Option<u32>,
    /// Show in-flight document paths above the progress bar.
    #[arg(long)]
    pub debug: bool,
    /// Re-embed all documents, even those already embedded.
    ///
    /// Required when switching to a different embedding model. Clears existing
    /// vectors and re-embeds every document with the currently configured model.
    #[arg(long)]
    pub force: bool,
}

/// # Errors
///
/// Returns an error if embeddings are not configured, the pool cannot be built,
/// or the pipeline fails.
pub async fn run(
    args: &EmbedArgs,
    config: &AumConfig,
    backend: Arc<AumBackend>,
    tracker: JobTracker,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.embeddings.enabled,
        "embeddings are not enabled in the configuration (set embeddings.enabled = true)"
    );

    let _lock = acquire_embed_lock(config, &args.index)?;

    // Check whether the index was previously embedded with a different model.
    let previous = tracker.get_embedding_model(&args.index).await?;
    if let Some(ref prev) = previous {
        let model_changed =
            prev.model != config.embeddings.model || prev.backend != config.embeddings.backend;
        let dim_changed = prev.dimension != i64::from(config.embeddings.dimension);

        if (model_changed || dim_changed) && !args.force {
            let mut reasons = Vec::new();
            if model_changed {
                reasons.push(format!(
                    "model: {}/{} → {}/{}",
                    prev.backend, prev.model, config.embeddings.backend, config.embeddings.model,
                ));
            }
            if dim_changed {
                reasons.push(format!(
                    "dimension: {} → {}",
                    prev.dimension, config.embeddings.dimension,
                ));
            }
            anyhow::bail!(
                "embedding configuration changed for index '{}' ({}).\n\
                 Run with {} to clear existing vectors and re-embed all documents.",
                args.index,
                reasons.join(", "),
                "--force".bold(),
            );
        }
    }

    // Ensure the search index exists with the correct vector dimension before
    // embedding, so that `aum init` is never required as a separate step.
    initialize_backend(&backend, config, &args.index).await?;

    // If --force was passed (or the model changed), clear existing embeddings
    // so that every document is re-embedded with the new model.
    if args.force && previous.is_some() {
        tracing::info!(index = %args.index, "clearing existing embeddings for re-embed");
        println!("Clearing existing embeddings for index '{}'…", args.index);
        backend
            .clear_embeddings(&args.index)
            .await
            .context("failed to clear existing embeddings")?;
        // Remove stale metadata so a partial re-embed doesn't leave the old
        // model info in place.
        tracker
            .clear_embedding_model(&args.index)
            .await
            .context("failed to clear embedding metadata")?;
    }

    let pool = build_embedder_pool(config)
        .await
        .context("failed to build embedder pool")?;

    let batch_size = args.batch_size.unwrap_or(config.embeddings.batch_size) as usize;
    let max_chunk_chars = (config.embeddings.context_length * 4) as usize;
    let overlap_chars = config.embeddings.chunk_overlap as usize;

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(EmbedSnapshot::default());

    let mut pipeline = EmbedPipeline::new(
        Arc::clone(&backend),
        Arc::clone(&pool),
        tracker.clone(),
        args.index.clone(),
        batch_size,
        max_chunk_chars,
        overlap_chars,
    )
    .with_progress(progress_tx);

    if args.debug {
        pipeline = pipeline.with_debug();
    }

    let render_handle = tokio::spawn(render_embed_progress(progress_rx, args.debug));
    let job = pipeline.run().await.context("embed pipeline failed")?;
    render_handle.abort();

    if job.processed > 0 {
        let dimension = pool.first_client().dimension();
        let info = embedding_model_info(config, dimension);
        tracker
            .set_embedding_model(&args.index, &info)
            .await
            .context("failed to store embedding model metadata")?;
    }

    print_job_summary(&job);
    Ok(())
}
