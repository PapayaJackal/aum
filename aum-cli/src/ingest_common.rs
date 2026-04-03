//! Shared helpers for ingest, resume, and retry commands.

use std::sync::Arc;

use anyhow::Context as _;
use aum_core::config::AumConfig;
use aum_core::extraction::TikaExtractor;
use aum_core::extraction::tika::TikaExtractorConfig;
use aum_core::pool::{InstanceDesc, InstancePool, InstancePoolConfig};

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
pub async fn render_progress(
    mut rx: tokio::sync::watch::Receiver<aum_core::ingest::IngestSnapshot>,
) {
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

    let pb = ProgressBar::new(0);
    let style = ProgressStyle::with_template(
        "[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} extracted | {msg}",
    )
    .unwrap_or_else(|_| ProgressStyle::default_bar());
    pb.set_style(style);

    loop {
        apply_snap(&pb, &rx.borrow_and_update().clone());
        if rx.changed().await.is_err() {
            break;
        }
    }

    // Final update: always set length so the bar shows 100% even on early exit.
    let snap = rx.borrow().clone();
    pb.set_length(snap.discovered);
    apply_snap(&pb, &snap);
    pb.finish();
}
