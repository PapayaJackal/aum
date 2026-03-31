//! aum HTTP API server.

use tracing::info;

fn main() {
    let _config = aum_core::bootstrap();
    info!("aum-api starting");
}
