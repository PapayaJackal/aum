//! `aum reset <INDEX>` — delete a search index and all its tracker data.

use std::io::{self, Write as _};

use clap::Args;

use aum_core::db::JobTracker;
use aum_core::search::SearchBackend;

#[derive(Args)]
pub struct ResetArgs {
    /// Name of the index to reset.
    pub index: String,
}

/// # Errors
///
/// Returns an error if reading from stdin fails, or if the backend or tracker
/// operations fail.
pub async fn run(
    args: &ResetArgs,
    backend: &dyn SearchBackend,
    tracker: &JobTracker,
) -> anyhow::Result<()> {
    print!(
        "This will delete the search index '{}' and all tracker data. Continue? [y/N] ",
        args.index
    );
    io::stdout().flush()?;

    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    if !matches!(line.trim(), "y" | "Y" | "yes" | "YES") {
        println!("Aborted.");
        return Ok(());
    }

    backend
        .delete_index(&args.index)
        .await
        .map_err(|e| anyhow::anyhow!("failed to delete index '{}': {e}", args.index))?;

    let deleted = tracker
        .clear_index(&args.index)
        .await
        .map_err(|e| anyhow::anyhow!("failed to clear tracker data for '{}': {e}", args.index))?;

    println!(
        "Reset '{}': index deleted, {deleted} job record(s) removed.",
        args.index
    );
    Ok(())
}
