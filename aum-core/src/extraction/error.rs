//! Error types for the document extraction pipeline.

use std::path::PathBuf;

/// Errors that can occur during document extraction.
#[derive(Debug, thiserror::Error)]
pub enum ExtractionError {
    /// Tika `/rmeta/text` HTTP request failed (connection/transport layer).
    #[error("Tika /rmeta request failed for {path}: {source}")]
    RmetaConnection {
        /// Path of the file being extracted.
        path: PathBuf,
        /// Underlying reqwest error.
        source: reqwest::Error,
    },

    /// Tika `/rmeta/text` returned a non-200 HTTP status.
    #[error("Tika /rmeta returned HTTP {status} for {path}: {body}")]
    RmetaHttp {
        /// Path of the file being extracted.
        path: PathBuf,
        /// HTTP status code returned by Tika.
        status: u16,
        /// First 200 chars of the response body for diagnostics.
        body: String,
    },

    /// Tika `/rmeta/text` response body could not be parsed as JSON.
    #[error("Failed to parse Tika /rmeta JSON for {path}: {source}")]
    RmetaJson {
        /// Path of the file being extracted.
        path: PathBuf,
        /// Underlying JSON parse error.
        source: serde_json::Error,
    },

    /// Tika `/unpack/all` HTTP request failed (connection/transport layer).
    #[error("Tika /unpack request failed for {path}: {source}")]
    UnpackConnection {
        /// Path of the file being unpacked.
        path: PathBuf,
        /// Underlying reqwest error.
        source: reqwest::Error,
    },

    /// Tika `/unpack/all` returned a non-200 (and non-204) HTTP status.
    #[error("Tika /unpack returned HTTP {status} for {path}")]
    UnpackHttp {
        /// Path of the file being unpacked.
        path: PathBuf,
        /// HTTP status code returned by Tika.
        status: u16,
    },

    /// Recursive archive extraction exceeded the configured depth limit.
    #[error("extraction depth limit ({max_depth}) exceeded at {path}")]
    DepthLimitExceeded {
        /// Path of the file that triggered the limit.
        path: PathBuf,
        /// The configured maximum depth.
        max_depth: u32,
    },

    /// Filesystem I/O error during extraction.
    #[error("I/O error for {path}: {source}")]
    Io {
        /// Path involved in the failing operation.
        path: PathBuf,
        /// Underlying I/O error.
        source: std::io::Error,
    },

    /// Zip archive parsing or entry reading error.
    #[error("zip error for {path}: {source}")]
    Zip {
        /// Path of the zip file being read.
        path: PathBuf,
        /// Underlying zip error.
        source: async_zip::error::ZipError,
    },
}

impl ExtractionError {
    /// A short machine-readable error category for this error variant.
    ///
    /// Suitable for recording in the job-error table and for metrics labels.
    #[must_use]
    pub fn error_type(&self) -> &'static str {
        match self {
            Self::RmetaConnection { .. } => "RmetaConnectionError",
            Self::RmetaHttp { .. } => "RmetaHttpError",
            Self::RmetaJson { .. } => "RmetaJsonError",
            Self::UnpackConnection { .. } => "UnpackConnectionError",
            Self::UnpackHttp { .. } => "UnpackHttpError",
            Self::DepthLimitExceeded { .. } => "DepthLimitExceeded",
            Self::Io { .. } => "IoError",
            Self::Zip { .. } => "ZipError",
        }
    }
}
