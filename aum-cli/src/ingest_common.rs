//! Shared helpers for ingest, resume, and retry commands.

use std::path::Path;
use std::sync::Arc;

use anyhow::Context as _;
use aum_core::config::AumConfig;
use aum_core::extraction::TikaExtractor;
use aum_core::extraction::tika::TikaExtractorConfig;
use aum_core::ingest::IngestLock;
use aum_core::pool::{InstanceDesc, InstancePool, InstancePoolConfig};
use clap::Args;

/// Common CLI arguments shared by all three ingest commands (ingest / resume / retry).
#[derive(Args)]
pub struct CommonIngestArgs {
    /// Number of documents per indexing batch (overrides config).
    #[arg(long)]
    pub batch_size: Option<u32>,
    /// Number of extraction workers (overrides config).
    #[arg(long)]
    pub workers: Option<u32>,
    /// Enable OCR for image-based documents.
    #[arg(long = "ocr", overrides_with = "no_ocr")]
    pub ocr: bool,
    /// Disable OCR for image-based documents.
    #[arg(long = "no-ocr", overrides_with = "ocr")]
    pub no_ocr: bool,
    /// Tesseract language code for OCR (e.g. "eng", "eng+fra").
    #[arg(long)]
    pub ocr_language: Option<String>,
    /// Show in-flight file names above the progress bar.
    #[arg(long)]
    pub debug: bool,
}

/// Resolve OCR flag from a pair of mutually-exclusive `--ocr` / `--no-ocr` args.
pub fn resolve_ocr_override(ocr: bool, no_ocr: bool) -> Option<bool> {
    if ocr {
        Some(true)
    } else if no_ocr {
        Some(false)
    } else {
        None
    }
}

/// Acquire an exclusive ingest lock for `source_dir`, or return an error
/// explaining that another process already holds it.
pub fn acquire_ingest_lock(config: &AumConfig, source_dir: &Path) -> anyhow::Result<IngestLock> {
    IngestLock::try_acquire(&config.lock_dir(), source_dir)
        .context("failed to acquire ingest lock")?
        .with_context(|| {
            let pid = IngestLock::read_holder_pid(&config.lock_dir(), source_dir);
            format!(
                "another ingest job is already running on '{}' (holder pid: {})",
                source_dir.display(),
                pid.map_or("unknown".to_owned(), |p| p.to_string()),
            )
        })
}

/// Build a Tika extractor pool from config, with optional overrides.
///
/// # Errors
///
/// Returns an error if any `TikaExtractor` cannot be constructed (e.g. invalid config).
pub fn build_tika_pool(
    config: &AumConfig,
    index_name: &str,
    ocr_override: Option<bool>,
    lang_override: Option<String>,
) -> anyhow::Result<Arc<InstancePool<TikaExtractor>>> {
    let ocr_enabled = ocr_override.unwrap_or(config.tika.ocr_enabled);
    let ocr_language = lang_override.unwrap_or_else(|| config.tika.ocr_language.clone());
    let instances = config.effective_tika_instances();

    let descs = instances
        .iter()
        .map(|inst| -> anyhow::Result<InstanceDesc<TikaExtractor>> {
            let client = TikaExtractor::new(TikaExtractorConfig {
                server_url: inst.url.clone(),
                ocr_enabled,
                ocr_language: ocr_language.clone(),
                extract_dir: config.extract_dir(),
                index_name: index_name.to_owned(),
                max_depth: config.ingest.max_extract_depth,
                request_timeout_secs: u64::from(config.tika.request_timeout),
                max_content_length: config.ingest.max_content_length,
            })
            .context("failed to build Tika extractor")?;
            Ok(InstanceDesc {
                url: inst.url.clone(),
                client,
                concurrency: inst.concurrency,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    let pool =
        InstancePool::new(descs, InstancePoolConfig::new("tika")).context("Tika pool is empty")?;
    Ok(Arc::new(pool))
}

// ---------------------------------------------------------------------------
// Progress rendering helpers
// ---------------------------------------------------------------------------

use std::time::Instant;

use indicatif::{ProgressBar, ProgressStyle};
use owo_colors::OwoColorize as _;

/// Build the progress message matching the Python output style:
/// `[████████░░░░░░░░░░] done/discovered (pct%)  scan:status  tika:N  0.123s/file  idx:N  empty:N  fail:N  MM:SS`
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
fn format_progress_line(snap: &aum_core::ingest::IngestSnapshot, start: Instant) -> String {
    let files_done = snap.extracted;
    let discovered = snap.discovered;

    let mut parts = Vec::new();

    // Progress bar
    if discovered > 0 {
        let pct = (files_done as f64 / discovered as f64 * 100.0).min(100.0);
        let filled = (20.0 * pct / 100.0) as usize;
        let unfilled = 20 - filled;
        parts.push(format!(
            "{}{}{}{} {}",
            "[".dimmed(),
            "█".repeat(filled).blue(),
            "░".repeat(unfilled).blue().dimmed(),
            "]".dimmed(),
            format_args!("{files_done}/{discovered} ({pct:.0}%)").white(),
        ));
    } else {
        parts.push(format!("{}", format_args!("{files_done} files").white()));
    }

    // Directory scan status
    if snap.scan_complete {
        parts.push(format!("{}", "scan:done".dimmed().green()));
    } else {
        parts.push(format!(
            "{}",
            format!("scan:{discovered}").dimmed().yellow()
        ));
    }

    // In-flight Tika requests
    parts.push(format!("{}", format!("tika:{}", snap.in_flight).cyan()));

    // Average extraction speed
    let avg = if snap.extracted > 0 {
        snap.total_extraction_secs / snap.extracted as f64
    } else {
        0.0
    };
    parts.push(format!("{}", format!("{avg:.3}s/file").yellow()));

    // Indexed count
    parts.push(format!("{}", format!("idx:{}", snap.indexed).green()));

    // Skipped (only if > 0)
    if snap.skipped > 0 {
        parts.push(format!(
            "{}",
            format!("skip:{}", snap.skipped).dimmed().cyan()
        ));
    }

    // Empty count (only if > 0)
    if snap.empty > 0 {
        parts.push(format!("{}", format!("empty:{}", snap.empty).yellow()));
    }

    // Failed count (only if > 0)
    if snap.failed > 0 {
        parts.push(format!("{}", format!("fail:{}", snap.failed).red().bold()));
    }

    // Elapsed wall-clock time
    let elapsed = start.elapsed().as_secs();
    let m = elapsed / 60;
    let s = elapsed % 60;
    parts.push(format!("{}", format!("{m:02}:{s:02}").dimmed()));

    parts.join("  ")
}

fn apply_snap(pb: &ProgressBar, snap: &aum_core::ingest::IngestSnapshot, start: Instant) {
    if snap.scan_complete {
        pb.set_length(snap.discovered);
    }
    pb.set_position(snap.extracted);
    pb.set_message(format_progress_line(snap, start));
}

fn apply_debug(db: &ProgressBar, snap: &aum_core::ingest::IngestSnapshot) {
    let paths: Vec<String> = snap
        .in_flight_paths
        .iter()
        .map(|p| format!("{}", p.dimmed().cyan()))
        .collect();
    db.set_message(if paths.is_empty() {
        String::new()
    } else {
        paths.join("\n")
    });
}

/// Render an ingest progress bar from a watch receiver until the channel closes.
///
/// Returns immediately without displaying anything if stderr is not a TTY.
///
/// When `debug` is `true`, a second bar above the progress bar shows the file
/// names currently being extracted by each worker.
pub async fn render_progress(
    mut rx: tokio::sync::watch::Receiver<aum_core::ingest::IngestSnapshot>,
    debug: bool,
) {
    use std::io::IsTerminal as _;
    use std::time::Duration;

    // Only render when connected to a terminal; otherwise just drain the channel.
    if !std::io::stderr().is_terminal() {
        while rx.changed().await.is_ok() {}
        return;
    }

    let mp = crate::progress::get();
    let start = Instant::now();

    let debug_bar: Option<ProgressBar> = if debug {
        let b = mp.add(ProgressBar::new_spinner());
        b.set_style(
            ProgressStyle::with_template("{msg}")
                .unwrap_or_else(|_| ProgressStyle::default_spinner()),
        );
        Some(b)
    } else {
        None
    };

    let pb = mp.add(ProgressBar::new(0));
    pb.set_style(
        ProgressStyle::with_template("{msg}").unwrap_or_else(|_| ProgressStyle::default_bar()),
    );
    pb.enable_steady_tick(Duration::from_millis(250));

    loop {
        let snap = rx.borrow_and_update().clone();
        apply_snap(&pb, &snap, start);
        if let Some(ref db) = debug_bar {
            apply_debug(db, &snap);
        }
        if rx.changed().await.is_err() {
            break;
        }
    }

    // Final update: always set length so the bar shows 100% even on early exit.
    let snap = rx.borrow().clone();
    pb.set_length(snap.discovered);
    apply_snap(&pb, &snap, start);
    if let Some(ref db) = debug_bar {
        apply_debug(db, &snap);
        db.finish_and_clear();
    }
    pb.finish();
    mp.remove(&pb);
}
