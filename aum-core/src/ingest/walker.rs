//! Async directory walking and file-path feeding for the ingest pipeline.
//!
//! The walker discovers files by recursively traversing a directory tree using
//! [`async_walkdir::WalkDir`] and sends paths into a bounded channel.  Channel
//! backpressure naturally throttles discovery when extraction workers fall
//! behind.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_walkdir::WalkDir;
use futures::StreamExt as _;
use tokio::sync::mpsc;
use tracing::{debug, warn};

/// Capacity of the path channel between the walker and extraction workers.
pub const PATH_CHANNEL_CAPACITY: usize = 1000;

/// How often to update the discovered count (every N files).
const PROGRESS_INTERVAL: u64 = 500;

// ---------------------------------------------------------------------------
// Directory walker
// ---------------------------------------------------------------------------

/// Recursively walk `root`, sending discovered file paths into `tx`.
///
/// Updates `discovered` every [`PROGRESS_INTERVAL`] files so the pipeline
/// orchestrator can report progress.  Returns the total number of files
/// discovered, or an I/O error if the root directory cannot be accessed.
///
/// The function returns when the walk completes or the receiver is dropped.
///
/// # Errors
///
/// Returns `std::io::Error` if `root` does not exist or is not a directory.
pub async fn walk_directory(
    root: &Path,
    tx: &mpsc::Sender<PathBuf>,
    discovered: &Arc<AtomicU64>,
) -> Result<u64, std::io::Error> {
    if !root.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::NotFound,
            format!("source directory does not exist: {}", root.display()),
        ));
    }

    let mut count: u64 = 0;
    let mut entries = WalkDir::new(root);

    while let Some(result) = entries.next().await {
        let entry = match result {
            Ok(e) => e,
            Err(e) => {
                warn!(error = %e, "skipping unreadable entry during walk");
                continue;
            }
        };

        let ft = match entry.file_type().await {
            Ok(ft) => ft,
            Err(e) => {
                warn!(path = %entry.path().display(), error = %e, "cannot stat entry");
                continue;
            }
        };
        if !ft.is_file() {
            continue;
        }

        if tx.send(entry.path()).await.is_err() {
            debug!("walker: receiver dropped, stopping walk");
            break;
        }

        count += 1;
        if count.is_multiple_of(PROGRESS_INTERVAL) {
            discovered.store(count, Ordering::Relaxed);
        }
    }

    discovered.store(count, Ordering::Relaxed);
    debug!(count, "directory walk complete");
    Ok(count)
}

// ---------------------------------------------------------------------------
// Explicit path feeder (retry mode)
// ---------------------------------------------------------------------------

/// Feed explicit file paths into the channel, filtering out paths that no
/// longer exist on disk.
///
/// Used by retry mode where the caller already knows which files to process.
/// Returns the number of paths actually sent (excluding missing files).
pub async fn feed_paths(
    paths: impl IntoIterator<Item = PathBuf>,
    tx: &mpsc::Sender<PathBuf>,
    discovered: &Arc<AtomicU64>,
) -> u64 {
    let mut count: u64 = 0;

    for path in paths {
        if !path.is_file() {
            debug!(path = %path.display(), "skipping missing file in retry list");
            continue;
        }
        if tx.send(path).await.is_err() {
            debug!("feeder: receiver dropped, stopping");
            break;
        }
        count += 1;
        if count.is_multiple_of(PROGRESS_INTERVAL) {
            discovered.store(count, Ordering::Relaxed);
        }
    }

    discovered.store(count, Ordering::Relaxed);
    count
}

// ---------------------------------------------------------------------------
// Existence filter (resume mode)
// ---------------------------------------------------------------------------

use super::doc_id::file_doc_id;
use super::sink::ExistenceChecker;

/// Batch size for checking existing document IDs.
const FILTER_BATCH_SIZE: usize = 200;

/// Filter out already-indexed files by checking their document IDs against
/// the search backend.
///
/// Reads paths from `rx`, batches them, checks which are already indexed
/// via `checker`, and forwards only new paths to `tx`.  Increments
/// `skip_count` for each skipped file.
pub async fn filter_existing(
    mut rx: mpsc::Receiver<PathBuf>,
    tx: mpsc::Sender<PathBuf>,
    checker: Arc<dyn ExistenceChecker>,
    index: &str,
    skip_count: Arc<AtomicU64>,
) {
    let mut batch: Vec<PathBuf> = Vec::with_capacity(FILTER_BATCH_SIZE);

    loop {
        // Fill a batch, breaking on channel close.
        let done = fill_filter_batch(&mut rx, &mut batch).await;

        if !batch.is_empty() {
            flush_filter_batch(&batch, &tx, &checker, index, &skip_count).await;
            batch.clear();
        }

        if done {
            break;
        }
    }

    debug!(
        skipped = skip_count.load(Ordering::Relaxed),
        "existence filter complete"
    );
}

/// Fill `batch` from `rx` up to [`FILTER_BATCH_SIZE`].
///
/// Returns `true` when the channel is closed (no more paths will arrive).
async fn fill_filter_batch(rx: &mut mpsc::Receiver<PathBuf>, batch: &mut Vec<PathBuf>) -> bool {
    // Block on the first item to avoid busy-looping; then drain without blocking.
    match rx.recv().await {
        Some(path) => batch.push(path),
        None => return true,
    }

    while batch.len() < FILTER_BATCH_SIZE {
        match rx.try_recv() {
            Ok(path) => batch.push(path),
            Err(_) => break,
        }
    }

    false
}

/// Check a batch of paths against the existence checker and forward new ones.
async fn flush_filter_batch(
    batch: &[PathBuf],
    tx: &mpsc::Sender<PathBuf>,
    checker: &Arc<dyn ExistenceChecker>,
    index: &str,
    skip_count: &Arc<AtomicU64>,
) {
    let doc_ids: Vec<String> = batch
        .iter()
        .map(|p| {
            let canonical = p.canonicalize().unwrap_or_else(|_| p.clone());
            file_doc_id(&canonical, 0)
        })
        .collect();

    let existing = checker.get_existing(index, &doc_ids).await;

    for (path, doc_id) in batch.iter().zip(doc_ids.iter()) {
        if existing.contains(doc_id) {
            skip_count.fetch_add(1, Ordering::Relaxed);
            debug!(path = %path.display(), "skipping already-indexed file");
        } else if tx.send(path.clone()).await.is_err() {
            debug!("filter: receiver dropped, stopping");
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use tokio::sync::mpsc;

    use super::*;
    use crate::ingest::sink::NoOpChecker;
    use crate::ingest::test_helpers::{TestChecker, make_temp_tree};

    #[tokio::test]
    async fn walk_discovers_all_files() -> anyhow::Result<()> {
        let dir = make_temp_tree(&["a.txt", "sub/b.txt", "sub/deep/c.txt"])?;
        let (tx, mut rx) = mpsc::channel(100);
        let discovered = Arc::new(AtomicU64::new(0));

        let count = walk_directory(dir.path(), &tx, &discovered).await?;
        drop(tx);

        let mut paths = Vec::new();
        while let Some(p) = rx.recv().await {
            paths.push(p);
        }

        assert_eq!(count, 3);
        assert_eq!(paths.len(), 3);
        assert_eq!(discovered.load(Ordering::Relaxed), 3);
        Ok(())
    }

    #[tokio::test]
    async fn walk_skips_directories() -> anyhow::Result<()> {
        use anyhow::Context as _;
        let dir = make_temp_tree(&["file.txt"])?;
        std::fs::create_dir_all(dir.path().join("empty_dir")).context("mkdir")?;
        let (tx, mut rx) = mpsc::channel(100);
        let discovered = Arc::new(AtomicU64::new(0));

        let count = walk_directory(dir.path(), &tx, &discovered).await?;
        drop(tx);

        let mut paths = Vec::new();
        while let Some(p) = rx.recv().await {
            paths.push(p);
        }

        assert_eq!(count, 1);
        assert_eq!(paths.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn walk_nonexistent_directory_errors() {
        let (tx, _rx) = mpsc::channel(100);
        let discovered = Arc::new(AtomicU64::new(0));
        let result = walk_directory(Path::new("/nonexistent_dir_abc"), &tx, &discovered).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn feed_paths_sends_existing_files() -> anyhow::Result<()> {
        let dir = make_temp_tree(&["a.txt", "b.txt"])?;
        let (tx, mut rx) = mpsc::channel(100);
        let discovered = Arc::new(AtomicU64::new(0));

        let paths = vec![
            dir.path().join("a.txt"),
            dir.path().join("missing.txt"), // should be skipped
            dir.path().join("b.txt"),
        ];
        let count = feed_paths(paths, &tx, &discovered).await;
        drop(tx);

        let mut received = Vec::new();
        while let Some(p) = rx.recv().await {
            received.push(p);
        }

        assert_eq!(count, 2);
        assert_eq!(received.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn filter_existing_skips_known_docs() -> anyhow::Result<()> {
        use anyhow::Context as _;
        let dir = make_temp_tree(&["a.txt", "b.txt", "c.txt"])?;

        // Pre-compute the doc ID for b.txt so the checker reports it as existing.
        let b_canonical = dir
            .path()
            .join("b.txt")
            .canonicalize()
            .context("canonicalize")?;
        let b_id = file_doc_id(&b_canonical, 0);

        let checker: Arc<dyn ExistenceChecker> = Arc::new(TestChecker(HashSet::from([b_id])));

        let (walk_tx, walk_rx) = mpsc::channel(100);
        let (filter_tx, mut filter_rx) = mpsc::channel(100);
        let skip_count = Arc::new(AtomicU64::new(0));

        // Send all three paths through the walker channel.
        for name in &["a.txt", "b.txt", "c.txt"] {
            walk_tx.send(dir.path().join(name)).await?;
        }
        drop(walk_tx);

        filter_existing(walk_rx, filter_tx, checker, "test", skip_count.clone()).await;

        let mut received = Vec::new();
        while let Some(p) = filter_rx.recv().await {
            received.push(p);
        }

        assert_eq!(received.len(), 2, "b.txt should be filtered out");
        assert_eq!(skip_count.load(Ordering::Relaxed), 1);
        Ok(())
    }

    #[tokio::test]
    async fn filter_with_no_op_checker_passes_all() -> anyhow::Result<()> {
        let (walk_tx, walk_rx) = mpsc::channel(100);
        let (filter_tx, mut filter_rx) = mpsc::channel(100);
        let skip_count = Arc::new(AtomicU64::new(0));

        walk_tx.send(PathBuf::from("/tmp/x.txt")).await?;
        drop(walk_tx);

        filter_existing(
            walk_rx,
            filter_tx,
            Arc::new(NoOpChecker),
            "test",
            skip_count.clone(),
        )
        .await;

        let mut received = Vec::new();
        while let Some(p) = filter_rx.recv().await {
            received.push(p);
        }

        assert_eq!(received.len(), 1);
        assert_eq!(skip_count.load(Ordering::Relaxed), 0);
        Ok(())
    }
}
