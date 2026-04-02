//! Error types for the ingest pipeline.

use std::path::PathBuf;

/// Errors that can occur during an ingest pipeline run.
#[derive(Debug, thiserror::Error)]
pub enum IngestPipelineError {
    /// The source directory does not exist.
    #[error("source directory does not exist: {0}")]
    SourceNotFound(PathBuf),

    /// Failed to canonicalise the source directory path.
    #[error("failed to canonicalise source directory {path}: {source}")]
    Canonicalize {
        /// The path that could not be canonicalised.
        path: PathBuf,
        /// The underlying I/O error.
        source: std::io::Error,
    },

    /// A database operation failed.
    #[error("database error: {0}")]
    Db(#[from] crate::db::DbError),

    /// The pipeline was cancelled before completion.
    #[error("pipeline cancelled")]
    Cancelled,
}
