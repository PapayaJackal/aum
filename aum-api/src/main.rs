//! aum HTTP API server binary.

#[tokio::main]
async fn main() {
    let config = aum_core::bootstrap();
    if let Err(e) = aum_api::serve(config).await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}
