//! Shared helpers for ingest, resume, and retry commands.

use std::sync::Arc;

use anyhow::Context as _;
use aum_core::config::AumConfig;
use aum_core::extraction::TikaExtractor;
use aum_core::extraction::tika::TikaExtractorConfig;
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

    use indicatif::{ProgressBar, ProgressStyle};

    fn apply_snap(pb: &ProgressBar, snap: &aum_core::ingest::IngestSnapshot) {
        if snap.scan_complete {
            pb.set_length(snap.discovered);
        }
        pb.set_position(snap.extracted);
        pb.set_message(format!(
            "indexed={} failed={} in_flight={}",
            snap.indexed, snap.failed, snap.in_flight
        ));
    }

    fn apply_debug(db: &ProgressBar, snap: &aum_core::ingest::IngestSnapshot) {
        let names: Vec<&str> = snap
            .in_flight_paths
            .iter()
            .filter_map(|p| std::path::Path::new(p).file_name()?.to_str())
            .collect();
        db.set_message(if names.is_empty() {
            String::new()
        } else {
            format!("in-flight: {}", names.join(", "))
        });
    }

    // Only render when connected to a terminal; otherwise just drain the channel.
    if !std::io::stderr().is_terminal() {
        while rx.changed().await.is_ok() {}
        return;
    }

    let mp = crate::progress::get();

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
        ProgressStyle::with_template(
            "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} extracted | {msg}",
        )
        .unwrap_or_else(|_| ProgressStyle::default_bar()),
    );
    pb.enable_steady_tick(Duration::from_millis(100));

    loop {
        let snap = rx.borrow_and_update().clone();
        apply_snap(&pb, &snap);
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
    apply_snap(&pb, &snap);
    if let Some(ref db) = debug_bar {
        apply_debug(db, &snap);
        db.finish_and_clear();
    }
    pb.finish();
    mp.remove(&pb);
}
