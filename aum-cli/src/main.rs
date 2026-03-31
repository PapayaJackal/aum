//! aum command-line interface.

fn main() {
    let config = aum_core::config::load_config().unwrap_or_else(|e| {
        eprintln!("error: failed to load config: {e}");
        std::process::exit(1);
    });
    print!("{}", aum_core::config::format_config(&config));
}
