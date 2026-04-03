//! aum command-line interface.

use std::sync::Arc;

use clap::{Parser, Subcommand};
use tracing::debug;

mod backend;
mod commands;
mod ingest_common;
mod output;

use backend::create_backend;

#[derive(Parser)]
#[command(name = "aum", about = "Document search engine")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Display the resolved configuration.
    Config,
    /// List all search indices and their document counts.
    Indices,
    /// Create or update a search index.
    Init(commands::init::InitArgs),
    /// Delete a search index and all its tracker data.
    Reset(commands::reset::ResetArgs),
    /// Ingest documents from a directory into a search index.
    Ingest(commands::ingest::IngestArgs),
    /// Resume an interrupted or failed ingest job.
    Resume(commands::resume::ResumeArgs),
    /// Retry failed files from a previous ingest job.
    Retry(commands::retry::RetryArgs),
    /// List ingest and embed jobs.
    Jobs(commands::jobs::JobsArgs),
    /// Show details of a single job.
    Job(commands::job::JobArgs),
    /// Search documents in an index.
    Search(commands::search::SearchArgs),
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("error: {e:#}");
        std::process::exit(1);
    }
}

async fn create_tracker(config: &aum_core::config::AumConfig) -> aum_core::db::JobTracker {
    let pool = aum_core::bootstrap_db(config).await;
    aum_core::db::JobTracker::new(pool)
}

async fn run() -> anyhow::Result<()> {
    let cli = Cli::parse();

    let config = aum_core::bootstrap();
    debug!("configuration loaded");

    match cli.command {
        Commands::Config => {
            commands::config::run(&config);
        }

        Commands::Indices => {
            let backend = create_backend(&config)?;
            commands::indices::run(&backend).await?;
        }

        Commands::Init(args) => {
            let backend = create_backend(&config)?;
            commands::init::run(&args, &config, &backend).await?;
        }

        Commands::Reset(args) => {
            let tracker = create_tracker(&config).await;
            let backend = create_backend(&config)?;
            commands::reset::run(&args, &backend, &tracker).await?;
        }

        Commands::Ingest(args) => {
            let tracker = create_tracker(&config).await;
            let backend = Arc::new(create_backend(&config)?);
            commands::ingest::run(&args, &config, backend, tracker).await?;
        }

        Commands::Resume(args) => {
            let tracker = create_tracker(&config).await;
            let backend = Arc::new(create_backend(&config)?);
            commands::resume::run(&args, &config, backend, tracker).await?;
        }

        Commands::Retry(args) => {
            let tracker = create_tracker(&config).await;
            let backend = Arc::new(create_backend(&config)?);
            commands::retry::run(&args, &config, backend, tracker).await?;
        }

        Commands::Jobs(args) => {
            let tracker = create_tracker(&config).await;
            commands::jobs::run(&args, &tracker).await?;
        }

        Commands::Job(args) => {
            let tracker = create_tracker(&config).await;
            commands::job::run(&args, &tracker).await?;
        }

        Commands::Search(args) => {
            let backend = create_backend(&config)?;
            commands::search::run(&args, &backend).await?;
        }
    }

    Ok(())
}
