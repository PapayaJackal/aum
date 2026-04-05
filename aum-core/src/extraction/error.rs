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

    /// Whether this error is likely transient and the request should be retried.
    ///
    /// Connection-level failures and HTTP 5xx responses are considered
    /// retryable — they typically indicate that Tika is temporarily
    /// unavailable (e.g. restarting after hitting `HIT_MAX_FILES`).
    #[must_use]
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::RmetaConnection { .. } | Self::UnpackConnection { .. } => true,
            Self::RmetaHttp { status, .. } | Self::UnpackHttp { status, .. } => *status >= 500,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn connection_errors_are_retryable() -> anyhow::Result<()> {
        // Actually attempt a connection to an unreachable address to get a real reqwest error.
        let Err(err_source) = reqwest::Client::new()
            .get("http://127.0.0.1:1")
            .send()
            .await
        else {
            anyhow::bail!("request to unreachable address should fail");
        };
        let err = ExtractionError::RmetaConnection {
            path: PathBuf::from("test.pdf"),
            source: err_source,
        };
        assert!(err.is_retryable());
        Ok(())
    }

    #[test]
    fn http_5xx_is_retryable() {
        let err = ExtractionError::RmetaHttp {
            path: PathBuf::from("test.pdf"),
            status: 503,
            body: "Service Unavailable".into(),
        };
        assert!(err.is_retryable());

        let err = ExtractionError::UnpackHttp {
            path: PathBuf::from("test.pdf"),
            status: 500,
        };
        assert!(err.is_retryable());
    }

    #[test]
    fn http_4xx_is_not_retryable() {
        let err = ExtractionError::RmetaHttp {
            path: PathBuf::from("test.pdf"),
            status: 422,
            body: "Unprocessable".into(),
        };
        assert!(!err.is_retryable());
    }

    #[test]
    fn non_connection_errors_are_not_retryable() -> anyhow::Result<()> {
        let Err(json_err) = serde_json::from_str::<serde_json::Value>("invalid") else {
            anyhow::bail!("invalid JSON should fail to parse");
        };
        let err = ExtractionError::RmetaJson {
            path: PathBuf::from("test.pdf"),
            source: json_err,
        };
        assert!(!err.is_retryable());

        let err = ExtractionError::DepthLimitExceeded {
            path: PathBuf::from("test.zip"),
            max_depth: 5,
        };
        assert!(!err.is_retryable());

        let err = ExtractionError::Io {
            path: PathBuf::from("test.pdf"),
            source: std::io::Error::new(std::io::ErrorKind::NotFound, "not found"),
        };
        assert!(!err.is_retryable());
        Ok(())
    }
}
