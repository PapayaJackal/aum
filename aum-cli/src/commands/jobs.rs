//! `aum jobs` — list ingest and embed jobs.

use clap::Args;
use futures::TryStreamExt as _;

use aum_core::db::JobTracker;
use aum_core::models::JobStatus;

use crate::output::print_jobs_table;

#[derive(Args)]
pub struct JobsArgs {
    /// Filter by job status.
    #[arg(long)]
    pub status: Option<JobStatus>,
}

/// # Errors
///
/// Returns an error if the database query fails.
pub async fn run(args: &JobsArgs, tracker: &JobTracker) -> anyhow::Result<()> {
    let jobs: Vec<_> = tracker
        .list_jobs(args.status)
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list jobs: {e}"))?;

    print_jobs_table(&jobs);
    Ok(())
}
