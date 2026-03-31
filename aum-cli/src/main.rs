//! aum command-line interface.

use tracing::debug;

fn main() {
    let config = aum_core::bootstrap();
    debug!("configuration loaded");
    print!("{}", aum_core::config::format_config(&config));
}
