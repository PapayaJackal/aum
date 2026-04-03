//! `aum resume [JOB_ID]` — resume an interrupted or failed ingest job.

use std::sync::Arc;

use anyhow::Context as _;
use clap::Args;

use aum_core::config::AumConfig;
use aum_core::db::JobTracker;
use aum_core::ingest::{ExistenceChecker, IngestPipeline, IngestSnapshot};
use aum_core::models::{JobStatus, JobType};
use aum_core::search::AumBackend;

use crate::ingest_common::{
    CommonIngestArgs, acquire_ingest_lock, build_tika_pool, render_progress, resolve_ocr_override,
};
use crate::output::print_job_summary;

#[derive(Args)]
pub struct ResumeArgs {
    /// Job ID to resume. If omitted, resumes the most recent interrupted job.
    pub job_id: Option<String>,
    /// Override the target index name.
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

    let pool = build_tika_pool(
        config,
        index_name,
        resolve_ocr_override(args.common.ocr, args.common.no_ocr),
        args.common.ocr_language.clone(),
    )
    .context("failed to build Tika pool")?;

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
