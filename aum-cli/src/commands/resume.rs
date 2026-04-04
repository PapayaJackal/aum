//! `aum resume [JOB_ID]` — resume an interrupted ingest or embed job.

use std::sync::Arc;

use anyhow::Context as _;
use clap::Args;

use aum_core::config::AumConfig;
use aum_core::db::JobTracker;
use aum_core::embeddings::{EmbedPipeline, EmbedSnapshot};
use aum_core::ingest::{ExistenceChecker, IngestPipeline, IngestSnapshot};
use aum_core::models::{JobStatus, JobType};
use aum_core::search::AumBackend;

use crate::ingest_common::{
    CommonIngestArgs, acquire_embed_lock, acquire_ingest_lock, build_embedder_pool,
    build_tika_pool, effective_ocr_settings, initialize_backend, render_embed_progress,
    render_progress, resolve_ocr_override,
};
use crate::output::print_job_summary;

#[derive(Args)]
pub struct ResumeArgs {
    /// Job ID to resume. If omitted, resumes the most recent interrupted ingest job.
    pub job_id: Option<String>,
    /// Override the target index name (ingest jobs only).
    #[arg(long)]
    pub index: Option<String>,
    #[command(flatten)]
    pub common: CommonIngestArgs,
}

/// # Errors
///
/// Returns an error if no resumable job is found, or the pipeline fails.
pub async fn run(
    args: &ResumeArgs,
    config: &AumConfig,
    backend: Arc<AumBackend>,
    tracker: JobTracker,
) -> anyhow::Result<()> {
    let job = match &args.job_id {
        Some(id) => tracker
            .get_job(id, false)
            .await
            .context("failed to query job")?
            .with_context(|| format!("job '{id}' not found"))?,
        None => tracker
            .find_resumable_job(None, JobType::Ingest)
            .await
            .context("failed to query jobs")?
            .context("no resumable ingest job found")?,
    };

    // Validate status.
    match job.status {
        JobStatus::Interrupted | JobStatus::Failed | JobStatus::Running => {}
        other => {
            anyhow::bail!(
                "job '{}' cannot be resumed: status is {}",
                job.job_id,
                crate::output::format_status(other)
            );
        }
    }

    match job.job_type {
        JobType::Embed => resume_embed(args, config, backend, tracker, &job).await,
        JobType::Ingest => resume_ingest(args, config, backend, tracker, &job).await,
    }
}

async fn resume_embed(
    _args: &ResumeArgs,
    config: &AumConfig,
    backend: Arc<AumBackend>,
    tracker: JobTracker,
    job: &aum_core::models::IngestJob,
) -> anyhow::Result<()> {
    anyhow::ensure!(
        config.embeddings.enabled,
        "embeddings are not enabled in the configuration (set embeddings.enabled = true)"
    );

    println!(
        "Resuming embed job '{}' (index '{}')…",
        job.job_id, job.index_name,
    );

    let _lock = acquire_embed_lock(config, &job.index_name)?;
    let pool = build_embedder_pool(config)
        .await
        .context("failed to build embedder pool")?;

    let batch_size = config.embeddings.batch_size as usize;
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
    let completed_job = pipeline.run().await.context("embed pipeline failed")?;
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

async fn resume_ingest(
    args: &ResumeArgs,
    config: &AumConfig,
    backend: Arc<AumBackend>,
    tracker: JobTracker,
    job: &aum_core::models::IngestJob,
) -> anyhow::Result<()> {
    let _lock = acquire_ingest_lock(config, &job.source_dir)?;

    let index_name = args.index.as_deref().unwrap_or(&job.index_name);
    let batch_size = args.common.batch_size.unwrap_or(config.ingest.batch_size);
    let max_workers = args.common.workers.unwrap_or(config.ingest.max_workers);

    println!(
        "Resuming job '{}' (index '{}', source '{}')…",
        job.job_id,
        index_name,
        job.source_dir.display()
    );

    let ocr_override = resolve_ocr_override(args.common.ocr, args.common.no_ocr);
    let ocr = effective_ocr_settings(config, ocr_override, args.common.ocr_language.clone());

    initialize_backend(&backend, config, index_name).await?;

    let pool = build_tika_pool(config, index_name, &ocr).context("failed to build Tika pool")?;

    let checker: Arc<dyn ExistenceChecker> = Arc::clone(&backend) as Arc<dyn ExistenceChecker>;

    let (progress_tx, progress_rx) = tokio::sync::watch::channel(IngestSnapshot::default());

    let pipeline = IngestPipeline::new(
        pool,
        Arc::clone(&backend),
        tracker,
        index_name.to_owned(),
        batch_size,
        max_workers,
    )
    .with_ocr_settings(ocr)
    .with_progress(progress_tx);

    let render_handle = tokio::spawn(render_progress(progress_rx, args.common.debug));
    let completed_job = pipeline
        .run_resume(&job.source_dir, checker)
        .await
        .context("resume pipeline failed")?;
    render_handle.abort();

    print_job_summary(&completed_job);
    Ok(())
}
