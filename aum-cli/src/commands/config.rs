//! `aum config` — display the resolved configuration.

use aum_core::config::AumConfig;

pub fn run(config: &AumConfig) {
    print!("{}", aum_core::config::format_config(config));
}
