//! `aum retry <JOB_ID>` — retry failed files from a previous ingest job.

use std::sync::Arc;

use anyhow::Context as _;
use clap::Args;
use futures::TryStreamExt as _;

use aum_core::config::AumConfig;
use aum_core::db::JobTracker;
use aum_core::ingest::{IngestPipeline, IngestSnapshot};
use aum_core::models::ErrorFilter;
use aum_core::search::AumBackend;

use crate::ingest_common::{
    CommonIngestArgs, build_tika_pool, render_progress, resolve_ocr_override,
};
use crate::output::print_job_summary;

#[derive(Args)]
pub struct RetryArgs {
    /// Job ID whose failures should be retried.
    pub job_id: String,
    #[command(flatten)]
    pub common: CommonIngestArgs,
}

/// # Errors
///
/// Returns an error if the job is not found or the retry pipeline fails.
pub async fn run(
    args: &RetryArgs,
    config: &AumConfig,
    backend: Arc<AumBackend>,
    tracker: JobTracker,
) -> anyhow::Result<()> {
    let job = tracker
        .get_job(&args.job_id, false)
        .await
        .context("failed to query job")?
        .with_context(|| format!("job '{}' not found", args.job_id))?;

    // Collect all failed paths.
    let failed_paths: Vec<_> = tracker
        .get_failed_paths(&args.job_id, ErrorFilter::All)
        .try_collect()
        .await
        .context("failed to read error records")?;

    // Filter to paths that still exist on disk.
    let existing_paths: Vec<_> = failed_paths.into_iter().filter(|p| p.exists()).collect();

    if existing_paths.is_empty() {
        println!(
            "No failed files found for job '{}' (or none exist on disk).",
            args.job_id
        );
        return Ok(());
    }

    println!(
        "Retrying {} failed file(s) from job '{}' (index '{}')…",
        existing_paths.len(),
        job.job_id,
        job.index_name,
    );

    let batch_size = args.common.batch_size.unwrap_or(config.ingest.batch_size);
    let max_workers = args.common.workers.unwrap_or(config.ingest.max_workers);

    let pool = build_tika_pool(
        config,
        &job.index_name,
        resolve_ocr_override(args.common.ocr, args.common.no_ocr),
        args.common.ocr_language.clone(),
    )
    .context("failed to build Tika pool")?;

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(IngestSnapshot::default());

    let pipeline = IngestPipeline::new(
        pool,
        Arc::clone(&backend),
        tracker,
        job.index_name.clone(),
        batch_size,
        max_workers,
    )
    .with_progress(progress_tx);

    let render_handle = tokio::spawn(render_progress(progress_rx, args.common.debug));
    let completed_job = pipeline
        .run_retry(existing_paths, &job.source_dir)
        .await
        .context("retry pipeline failed")?;
    render_handle.abort();

    print_job_summary(&completed_job);
    Ok(())
}
