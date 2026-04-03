//! `aum job <JOB_ID>` — show details of a single job.

use clap::Args;
use futures::TryStreamExt as _;

use aum_core::db::JobTracker;
use aum_core::models::JobStatus;

use crate::output::{format_status, format_type, truncate};

#[derive(Args)]
pub struct JobArgs {
    /// Job ID to display.
    pub job_id: String,
    /// Show per-file error details.
    #[arg(long)]
    pub errors: bool,
    /// Hide `EmptyExtraction` errors when displaying error details.
    #[arg(long)]
    pub hide_empty: bool,
}

/// # Errors
///
/// Returns an error if the database query fails or the job is not found.
pub async fn run(args: &JobArgs, tracker: &JobTracker) -> anyhow::Result<()> {
    let job = tracker
        .get_job(&args.job_id, false)
        .await
        .map_err(|e| anyhow::anyhow!("failed to query job: {e}"))?
        .ok_or_else(|| anyhow::anyhow!("job '{}' not found", args.job_id))?;

    println!("Job ID:    {}", job.job_id);
    println!("Type:      {}", format_type(job.job_type));
    println!("Index:     {}", job.index_name);
    println!("Status:    {}", format_status(job.status));
    println!("Source:    {}", job.source_dir.display());
    println!("Files:     {}", job.total_files);
    println!("Extracted: {}", job.extracted);
    println!("Indexed:   {}", job.processed);
    println!("Skipped:   {}", job.skipped);
    println!("Empty:     {}", job.empty);
    println!("Failed:    {}", job.failed);
    println!(
        "Created:   {}",
        job.created_at.format("%Y-%m-%d %H:%M:%S UTC")
    );
    if let Some(finished) = job.finished_at {
        println!("Finished:  {}", finished.format("%Y-%m-%d %H:%M:%S UTC"));
    }

    if job.status == JobStatus::Interrupted {
        println!();
        println!("  → To resume: aum resume {}", job.job_id);
    }
    if job.failed > 0 {
        println!();
        println!("  → To retry failures: aum retry {}", job.job_id);
    }

    if args.errors {
        let errors: Vec<_> = tracker
            .list_errors(&args.job_id)
            .try_collect()
            .await
            .map_err(|e| anyhow::anyhow!("failed to list errors: {e}"))?;

        let filtered: Vec<_> = errors
            .iter()
            .filter(|e| !args.hide_empty || e.error_type != "EmptyExtraction")
            .collect();

        if filtered.is_empty() {
            println!();
            println!("No errors found.");
        } else {
            println!();
            println!("{:<60}  {:<25}  MESSAGE", "FILE PATH", "ERROR TYPE");
            println!("{}", "-".repeat(120));
            for e in filtered {
                println!(
                    "{:<60}  {:<25}  {}",
                    truncate(&e.file_path.to_string_lossy(), 60),
                    truncate(&e.error_type, 25),
                    e.message,
                );
            }
        }
    }

    Ok(())
}
