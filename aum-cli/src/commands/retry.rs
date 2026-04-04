//! `aum retry <JOB_ID>` — retry failed files from a previous ingest or embed job.

use std::sync::Arc;

use anyhow::Context as _;
use clap::Args;
use futures::TryStreamExt as _;

use aum_core::config::AumConfig;
use aum_core::db::JobTracker;
use aum_core::embeddings::{EmbedPipeline, EmbedSnapshot};
use aum_core::ingest::{IngestPipeline, IngestSnapshot};
use aum_core::models::{
    EMPTY_EXTRACTION_ERROR_TYPE, ErrorFilter, JobType, TRUNCATED_EXTRACTION_ERROR_TYPE,
};
use aum_core::search::AumBackend;

use crate::ingest_common::{
    CommonIngestArgs, acquire_embed_lock, acquire_ingest_lock, build_embedder_pool,
    build_tika_pool, effective_ocr_settings, initialize_backend, render_embed_progress,
    render_progress, resolve_ocr_override,
};
use crate::output::print_job_summary;

/// Restricts retry to a specific failure category.
#[derive(Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum RetryScope {
    /// Only retry files that produced actual extraction errors (skips empty and truncated).
    Failed,
    /// Only retry files that produced empty content.
    Empty,
    /// Only retry files whose content was truncated due to the content-length limit.
    Truncated,
}

#[derive(Args)]
pub struct RetryArgs {
    /// Job ID whose failures should be retried.
    pub job_id: String,
    #[command(flatten)]
    pub common: CommonIngestArgs,
    /// Restrict retry to a specific failure category.
    ///
    /// By default, failed files are retried and empty/truncated extractions are
    /// skipped unless OCR settings have changed, in which case all are retried.
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

    match job.job_type {
        JobType::Embed => retry_embed(args, config, backend, tracker, &job).await,
        JobType::Ingest => retry_ingest(args, config, backend, tracker, &job).await,
    }
}

async fn retry_embed(
    args: &RetryArgs,
    config: &AumConfig,
    backend: Arc<AumBackend>,
    tracker: JobTracker,
    job: &aum_core::models::IngestJob,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.embeddings.enabled,
        "embeddings are not enabled in the configuration (set embeddings.enabled = true)"
    );

    if args.only.is_some() {
        anyhow::bail!(
            "the --only flag is not supported for embed jobs; all failed documents will be retried"
        );
    }

    let doc_ids: Vec<String> = tracker
        .get_failed_doc_ids(&args.job_id)
        .try_collect()
        .await
        .context("failed to read embed error records")?;

    if doc_ids.is_empty() {
        println!("No failed documents found for embed job '{}'.", args.job_id);
        return Ok(());
    }

    println!(
        "Retrying {} failed document(s) from embed job '{}' (index '{}')…",
        doc_ids.len(),
        job.job_id,
        job.index_name,
    );

    let _lock = acquire_embed_lock(config, &job.index_name)?;
    let pool = build_embedder_pool(config)
        .await
        .context("failed to build embedder pool")?;

    let batch_size = args
        .common
        .batch_size
        .unwrap_or(config.embeddings.batch_size) as usize;
    let max_chunk_chars = (config.embeddings.context_length * 4) as usize;
    let overlap_chars = config.embeddings.chunk_overlap as usize;

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(EmbedSnapshot::default());

    let pipeline = EmbedPipeline::new(
        Arc::clone(&backend),
        Arc::clone(&pool),
        tracker.clone(),
        job.index_name.clone(),
        batch_size,
        max_chunk_chars,
        overlap_chars,
    )
    .with_progress(progress_tx);

    let render_handle = tokio::spawn(render_embed_progress(progress_rx, false));
    let completed_job = pipeline
        .run_for_doc_ids(doc_ids)
        .await
        .context("embed retry pipeline failed")?;
    render_handle.abort();

    if completed_job.processed > 0 {
        let dimension = pool.first_client().dimension();
        tracker
            .set_embedding_model(
                &job.index_name,
                &config.embeddings.model,
                &config.embeddings.backend.to_string(),
                i64::from(dimension),
            )
            .await
            .context("failed to store embedding model metadata")?;
    }

    print_job_summary(&completed_job);
    Ok(())
}

async fn retry_ingest(
    args: &RetryArgs,
    config: &AumConfig,
    backend: Arc<AumBackend>,
    tracker: JobTracker,
    job: &aum_core::models::IngestJob,
) -> anyhow::Result<()> {
    const AUTO_SKIP: &[&str] = &[EMPTY_EXTRACTION_ERROR_TYPE, TRUNCATED_EXTRACTION_ERROR_TYPE];

    let _lock = acquire_ingest_lock(config, &job.source_dir)?;

    // Determine which error category to retry.
    let ocr_override = resolve_ocr_override(args.common.ocr, args.common.no_ocr);
    let ocr = effective_ocr_settings(config, ocr_override, args.common.ocr_language.clone());

    // Old jobs (before OCR tracking was added) have no stored settings.
    // Treat them as "unchanged" so empty extractions are not auto-included.
    let ocr_changed = job.ocr_settings.as_ref().is_some_and(|prev| prev != &ocr);

    let filter = match args.only {
        Some(RetryScope::Empty) => ErrorFilter::Only(EMPTY_EXTRACTION_ERROR_TYPE),
        Some(RetryScope::Truncated) => ErrorFilter::Only(TRUNCATED_EXTRACTION_ERROR_TYPE),
        None if ocr_changed => ErrorFilter::All,
        Some(RetryScope::Failed) | None => ErrorFilter::Exclude(AUTO_SKIP),
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
            Some(RetryScope::Truncated) => println!(
                "No truncated documents recorded for job '{}' (or none exist on disk).",
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

    initialize_backend(&backend, config, &job.index_name).await?;

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
