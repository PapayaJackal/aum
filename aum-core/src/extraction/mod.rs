//! Document extraction pipeline: async trait and backend implementations.

pub mod error;
pub mod tika;

pub use error::ExtractionError;
pub use tika::TikaExtractor;

use std::path::Path;
use std::sync::Arc;

use futures::stream::BoxStream;

use crate::models::Document;

/// Metadata key for the human-readable path shown to end users.
///
/// Relative to the ingest source directory.  Set by the extractor for embedded
/// documents; relativised by the ingest pipeline for top-level documents.
pub const AUM_DISPLAY_PATH_KEY: &str = "_aum_display_path";

/// Metadata key for the container file that a document was extracted from.
///
/// For embedded documents only; absent on top-level documents.
pub const AUM_EXTRACTED_FROM_KEY: &str = "_aum_extracted_from";

/// Callback invoked for non-fatal sub-errors encountered during extraction.
///
/// Called with `(file_path, error_type, message)`. Non-fatal errors include
/// empty content from a non-empty file, truncated content, or a failed
/// sub-archive unpack that only affects a subset of embedded documents.
pub type RecordErrorFn = Arc<dyn Fn(&Path, &str, &str) + Send + Sync + 'static>;

/// Async document extractor. Each implementation handles a specific extraction backend.
///
/// Implementations return a [`BoxStream`] of documents, allowing callers to
/// begin processing (e.g. indexing) while extraction of embedded parts is
/// still in progress.
pub trait Extractor: Send + Sync {
    /// Extract text, metadata, and embedded documents from `file_path`.
    ///
    /// Returns a stream of [`Document`]s — one per content part (the container
    /// document plus each embedded document at all recursive depths). Documents
    /// are yielded as they are built, enabling pipeline-style processing.
    ///
    /// Non-fatal sub-errors (e.g. a single failed attachment or truncated
    /// content) are reported via `record_error` rather than propagated.
    /// Fatal errors (e.g. Tika unreachable, unreadable file) terminate the
    /// stream with an `Err` item.
    fn extract<'a>(
        &'a self,
        file_path: &'a Path,
        record_error: Option<&'a RecordErrorFn>,
    ) -> BoxStream<'a, Result<Document, ExtractionError>>;

    /// Return whether this extractor claims to support the given MIME type.
    fn supports(&self, mime_type: &str) -> bool;
}
