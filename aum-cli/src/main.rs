//! aum command-line interface.

use std::sync::Arc;

use tracing::debug;

#[tokio::main]
async fn main() {
    let config = aum_core::bootstrap();
    debug!("configuration loaded");

    let pool = aum_core::bootstrap_db(&config).await;
    let _jobs = Arc::new(aum_core::db::SqlxJobRepository::new(pool.clone()));
    let _job_errors = Arc::new(aum_core::db::SqlxJobErrorRepository::new(pool.clone()));
    let _embeddings = Arc::new(aum_core::db::SqlxIndexEmbeddingRepository::new(pool));

    print!("{}", aum_core::config::format_config(&config));
}
