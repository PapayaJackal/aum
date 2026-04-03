//! `aum retry <JOB_ID>` — retry failed files from a previous ingest job.

use std::sync::Arc;

use anyhow::Context as _;
use clap::Args;
use futures::TryStreamExt as _;

use aum_core::config::AumConfig;
use aum_core::db::JobTracker;
use aum_core::ingest::{IngestPipeline, IngestSnapshot};
use aum_core::models::{EMPTY_EXTRACTION_ERROR_TYPE, ErrorFilter};
use aum_core::search::AumBackend;

use crate::ingest_common::{
    CommonIngestArgs, acquire_ingest_lock, build_tika_pool, effective_ocr_settings,
    render_progress, resolve_ocr_override,
};
use crate::output::print_job_summary;

/// Restricts retry to a specific failure category.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum RetryScope {
    /// Only retry files that produced actual extraction errors (skips empty extractions).
    Failed,
    /// Only retry files that produced empty content.
    Empty,
}

#[derive(Args)]
pub struct RetryArgs {
    /// Job ID whose failures should be retried.
    pub job_id: String,
    #[command(flatten)]
    pub common: CommonIngestArgs,
    /// Restrict retry to a specific failure category.
    ///
    /// By default, failed files are retried and empty extractions are skipped
    /// unless OCR settings have changed, in which case both are retried.
    #[arg(long, value_enum)]
    pub only: Option<RetryScope>,
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

    let _lock = acquire_ingest_lock(config, &job.source_dir)?;

    // Determine which error category to retry.
    let ocr_override = resolve_ocr_override(args.common.ocr, args.common.no_ocr);
    let ocr = effective_ocr_settings(config, ocr_override, args.common.ocr_language.clone());

    // Old jobs (before OCR tracking was added) have no stored settings.
    // Treat them as "unchanged" so empty extractions are not auto-included.
    let ocr_changed = job.ocr_settings.as_ref().is_some_and(|prev| prev != &ocr);

    let filter = match args.only {
        Some(RetryScope::Empty) => ErrorFilter::Only(EMPTY_EXTRACTION_ERROR_TYPE),
        None if ocr_changed => ErrorFilter::All,
        Some(RetryScope::Failed) | None => ErrorFilter::Exclude(EMPTY_EXTRACTION_ERROR_TYPE),
    };

    // Collect failed paths using the selected filter.
    let failed_paths: Vec<_> = tracker
        .get_failed_paths(&args.job_id, filter)
        .try_collect()
        .await
        .context("failed to read error records")?;

    // Filter to paths that still exist on disk.
    let existing_paths: Vec<_> = failed_paths.into_iter().filter(|p| p.exists()).collect();

    if existing_paths.is_empty() {
        match args.only {
            Some(RetryScope::Empty) => println!(
                "No empty extractions recorded for job '{}' (or none exist on disk).\n\
                 Note: empty extractions are only tracked for jobs run with this version of aum.",
                args.job_id
            ),
            _ => println!(
                "No failed files found for job '{}' (or none exist on disk).",
                args.job_id
            ),
        }
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

    let pool =
        build_tika_pool(config, &job.index_name, &ocr).context("failed to build Tika pool")?;

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(IngestSnapshot::default());

    let pipeline = IngestPipeline::new(
        pool,
        Arc::clone(&backend),
        tracker,
        job.index_name.clone(),
        batch_size,
        max_workers,
    )
    .with_ocr_settings(ocr)
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
