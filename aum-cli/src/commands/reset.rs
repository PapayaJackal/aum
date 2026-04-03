//! `aum reset <INDEX>` — delete a search index, extracted files, and tracker data.

use std::fmt::Write as _;
use std::io::{self, Write as _};
use std::path::Path;

use clap::Args;
use tracing::info;

use aum_core::db::JobTracker;
use aum_core::search::SearchBackend;

#[derive(Args)]
pub struct ResetArgs {
    /// Name of the index to reset.
    pub index: String,
}

/// # Errors
///
/// Returns an error if reading from stdin fails, or if the backend, tracker,
/// or filesystem operations fail.
pub async fn run(
    args: &ResetArgs,
    backend: &dyn SearchBackend,
    tracker: &JobTracker,
    extract_dir: &Path,
) -> anyhow::Result<()> {
    print!(
        "This will delete the search index '{}', its extracted files, and all tracker data. Continue? [y/N] ",
        args.index
    );
    io::stdout().flush()?;

    let mut line = String::new();
    io::stdin().read_line(&mut line)?;
    if !matches!(line.trim(), "y" | "Y" | "yes" | "YES") {
        println!("Aborted.");
        return Ok(());
    }

    let (delete_result, clear_result) = tokio::join!(
        backend.delete_index(&args.index),
        tracker.clear_index(&args.index),
    );
    delete_result.map_err(|e| anyhow::anyhow!("failed to delete index '{}': {e}", args.index))?;
    let deleted = clear_result
        .map_err(|e| anyhow::anyhow!("failed to clear tracker data for '{}': {e}", args.index))?;

    let files_dir = extract_dir.join(&args.index);
    let size = {
        let p = files_dir.clone();
        tokio::task::spawn_blocking(move || dir_size(&p))
            .await
            .unwrap_or(0)
    };
    let files_removed = match tokio::fs::remove_dir_all(&files_dir).await {
        Ok(()) => {
            info!(path = %files_dir.display(), bytes = size, "removed extracted files");
            Some(size)
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => None,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "failed to remove extracted files at '{}': {e}",
                files_dir.display()
            ));
        }
    };

    let mut msg = format!(
        "Reset '{}': index deleted, {deleted} job record(s) removed",
        args.index
    );
    match files_removed {
        Some(size) => {
            let _ = write!(msg, ", {} of extracted files removed.", human_bytes(size));
        }
        None => msg.push_str(", no extracted files found."),
    }
    println!("{msg}");
    Ok(())
}

/// Recursively compute the total size of all files under `dir`.
fn dir_size(dir: &Path) -> u64 {
    let mut total: u64 = 0;
    let Ok(entries) = std::fs::read_dir(dir) else {
        return 0;
    };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else {
            continue;
        };
        if meta.is_dir() {
            total += dir_size(&entry.path());
        } else {
            total += meta.len();
        }
    }
    total
}

/// Format a byte count as a human-readable string.
fn human_bytes(bytes: u64) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    #[allow(clippy::cast_precision_loss)]
    let mut size = bytes as f64;
    for unit in UNITS {
        if size < 1024.0 {
            return format!("{size:.1} {unit}");
        }
        size /= 1024.0;
    }
    format!("{size:.1} PB")
}
