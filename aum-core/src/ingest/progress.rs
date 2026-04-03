//! Progress snapshot emitted by the ingest pipeline over a watch channel.
//!
//! [`IngestSnapshot`] is a point-in-time view of all counters the pipeline
//! maintains.  It is sent via a [`tokio::sync::watch`] channel so callers can
//! render live progress without polling the database.
//!
//! # Usage
//!
//! ```no_run
//! use tokio::sync::watch;
//! use aum_core::ingest::{IngestSnapshot, ProgressTx};
//!
//! let (tx, mut rx) = watch::channel(IngestSnapshot::default());
//! // Pass `tx` to `IngestPipeline::with_progress`.
//! // Spawn a task that calls `rx.changed().await` to render updates.
//! ```

use tokio::sync::watch;

// ---------------------------------------------------------------------------
// Snapshot
// ---------------------------------------------------------------------------

/// Point-in-time snapshot of ingest pipeline counters.
///
/// All integer fields are `u64` because they are loaded from [`std::sync::atomic::AtomicU64`]
/// values — they are never used in subtraction and are display-only.
#[derive(Debug, Clone, Default)]
pub struct IngestSnapshot {
    /// Total files discovered by the directory walker so far.
    pub discovered: u64,
    /// `true` once the walker has finished and no more paths will arrive.
    pub scan_complete: bool,
    /// Number of extraction workers currently holding a file.
    pub in_flight: u64,
    /// Display paths of files currently being extracted (one per active worker).
    pub in_flight_paths: Vec<String>,
    /// Files whose extraction succeeded (including those with empty content).
    pub extracted: u64,
    /// Documents flushed to the sink (cumulative indexed count).
    pub indexed: u64,
    /// Files skipped because they are already indexed (resume mode only).
    pub skipped: u64,
    /// Documents with empty content.
    pub empty: u64,
    /// Files whose extraction failed fatally.
    pub failed: u64,
    /// Cumulative sum of per-file extraction wall-clock times, in seconds.
    ///
    /// Divide by [`extracted`](Self::extracted) to obtain the rolling average.
    pub total_extraction_secs: f64,
}

// ---------------------------------------------------------------------------
// Type aliases
// ---------------------------------------------------------------------------

/// Sender half of the ingest progress watch channel.
///
/// Wrap in `Option` when attaching to a pipeline: `None` means no progress
/// display is requested and all `send_replace` calls become no-ops.
pub type ProgressTx = watch::Sender<IngestSnapshot>;
