//! `aum ingest <INDEX> <DIRECTORY>` — ingest documents into a search index.

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context as _;
use clap::Args;

use aum_core::config::AumConfig;
use aum_core::db::JobTracker;
use aum_core::ingest::{IngestPipeline, IngestSnapshot};
use aum_core::search::AumBackend;

use crate::ingest_common::{
    CommonIngestArgs, acquire_ingest_lock, build_tika_pool, effective_ocr_settings,
    initialize_backend, render_progress, resolve_ocr_override,
};
use crate::output::print_job_summary;

#[derive(Args)]
pub struct IngestArgs {
    /// Name of the search index to ingest into.
    pub index: String,
    /// Path to the directory containing documents to ingest.
    pub directory: PathBuf,
    #[command(flatten)]
    pub common: CommonIngestArgs,
}

/// # Errors
///
/// Returns an error if the directory cannot be walked, the search backend is
/// unreachable, or the pipeline fails.
pub async fn run(
    args: &IngestArgs,
    config: &AumConfig,
    backend: Arc<AumBackend>,
    tracker: JobTracker,
) -> anyhow::Result<()> {
    let source_dir = args
        .directory
        .canonicalize()
        .with_context(|| format!("cannot resolve '{}'", args.directory.display()))?;
    let _lock = acquire_ingest_lock(config, &source_dir)?;

    let batch_size = args.common.batch_size.unwrap_or(config.ingest.batch_size);
    let max_workers = args.common.workers.unwrap_or(config.ingest.max_workers);

    let ocr_override = resolve_ocr_override(args.common.ocr, args.common.no_ocr);
    let ocr = effective_ocr_settings(config, ocr_override, args.common.ocr_language.clone());

    initialize_backend(&backend, config, &args.index).await?;

    let pool = build_tika_pool(config, &args.index, &ocr).context("failed to build Tika pool")?;

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(IngestSnapshot::default());

    let pipeline = IngestPipeline::new(
        pool,
        Arc::clone(&backend),
        tracker,
        args.index.clone(),
        batch_size,
        max_workers,
    )
    .with_ocr_settings(ocr)
    .with_progress(progress_tx);

    let render_handle = tokio::spawn(render_progress(progress_rx, args.common.debug));
    let job = pipeline
        .run(&args.directory)
        .await
        .context("ingest pipeline failed")?;
    render_handle.abort();

    print_job_summary(&job);
    Ok(())
}
