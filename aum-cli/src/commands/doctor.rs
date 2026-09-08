//! Actionable first-run diagnostics.
use std::{path::Path, time::Duration};

use anyhow::bail;
use aum_core::config::{AumConfig, SearchBackendType};

pub async fn run(config: &AumConfig) -> anyhow::Result<()> {
    let mut failures = 0;
    let writable = std::fs::create_dir_all(&config.data.dir)
        .and_then(|()| tempfile::NamedTempFile::new_in(&config.data.dir).map(|_| ()));
    report(
        writable.is_ok(),
        "Data directory",
        "set AUM_DATA__DIR to a writable directory",
        &mut failures,
    );
    let frontend =
        cfg!(feature = "bundle-frontend") || Path::new("frontend/dist/index.html").is_file();
    report(
        frontend,
        "Frontend",
        "run npm ci && npm run build in frontend/, or use a bundled image",
        &mut failures,
    );
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()?;
    // Check HTTP status explicitly: some backend listing APIs accept error JSON.
    let request = match config.search_backend {
        SearchBackendType::OpenSearch => {
            Some(client.get(format!("{}/*", config.opensearch.url.trim_end_matches('/'))))
        }
        #[cfg(feature = "meilisearch")]
        SearchBackendType::Meilisearch => Some(
            client
                .get(format!(
                    "{}/indexes",
                    config.meilisearch.url.trim_end_matches('/')
                ))
                .bearer_auth(&config.meilisearch.api_key),
        ),
        #[cfg(not(feature = "meilisearch"))]
        SearchBackendType::Meilisearch => None,
    };
    let search_ok = if let Some(request) = request
        && crate::backend::create_backend(config).is_ok()
    {
        match request.send().await {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    } else {
        false
    };
    report(
        search_ok,
        "Search backend",
        "start the search service; check AUM_SEARCH_BACKEND, backend URL, credentials, and compiled features",
        &mut failures,
    );
    let urls: Vec<&str> = if config.tika.instances.is_empty() {
        vec![&config.tika.server_url]
    } else {
        config
            .tika
            .instances
            .iter()
            .map(|instance| instance.url.as_str())
            .collect()
    };
    for (index, url) in urls.iter().enumerate() {
        let ok = match client
            .get(format!("{}/tika", url.trim_end_matches('/')))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        };
        report(
            ok,
            &format!("Tika instance {}", index + 1),
            "start Tika; check AUM_TIKA__SERVER_URL or tika.instances",
            &mut failures,
        );
    }
    if failures > 0 {
        bail!("{failures} setup check(s) failed");
    }
    println!("All setup checks passed. Start with `aum serve`.");
    Ok(())
}

fn report(ok: bool, label: &str, fix: &str, failures: &mut usize) {
    if ok {
        println!("PASS {label}");
    } else {
        *failures += 1;
        println!("FAIL {label}: {fix}");
    }
}
