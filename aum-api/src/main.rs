//! aum HTTP API server.

use std::sync::Arc;

use tracing::info;

#[tokio::main]
async fn main() {
    let config = aum_core::bootstrap();

    let pool = aum_core::bootstrap_db(&config).await;
    let _jobs = Arc::new(aum_core::db::SqlxJobRepository::new(pool.clone()));
    let _job_errors = Arc::new(aum_core::db::SqlxJobErrorRepository::new(pool.clone()));
    let _embeddings = Arc::new(aum_core::db::SqlxIndexEmbeddingRepository::new(pool));

    info!("aum-api starting");
}
