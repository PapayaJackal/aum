//! Apache Tika document extraction backend.
//!
//! Uses Tika's HTTP API:
//! - `PUT /rmeta/text` for recursive text and metadata extraction.
//! - `PUT /unpack/all` to retrieve raw embedded files, called recursively on
//!   containers so nested attachments are also available.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use async_zip::tokio::read::fs::ZipFileReader; // crate name: async_zip
use futures::StreamExt as _;
use futures::stream::BoxStream;
use reqwest::header::{ACCEPT, HeaderMap, HeaderValue};
use serde_json::{Map, Value};
use tokio::io::AsyncWriteExt as _;
use tokio_util::compat::FuturesAsyncReadCompatExt as _;
use tracing_futures::Instrument as _;

use serde::Deserializer as _;

use crate::extraction::{ExtractionError, Extractor, RecordErrorFn};
use crate::models::{Document, MetadataValue};

// ---------------------------------------------------------------------------
// Tika metadata key constants
// ---------------------------------------------------------------------------

const TIKA_CONTENT_KEY: &str = "X-TIKA:content";
const EMBEDDED_RESOURCE_PATH_KEY: &str = "X-TIKA:embedded_resource_path";
const RESOURCE_NAME_KEY: &str = "resourceName";
const AUM_DISPLAY_PATH_KEY: &str = "_aum_display_path";
const AUM_EXTRACTED_FROM_KEY: &str = "_aum_extracted_from";
/// Maximum characters of an HTTP error body included in [`ExtractionError::RmetaHttp`].
const RMETA_ERROR_BODY_LIMIT: usize = 512;

fn record_error_metric(error_type: &str) {
    metrics::counter!("aum_extraction_errors_total", "error_type" => error_type.to_owned())
        .increment(1);
}

fn is_tika_internal(key: &str) -> bool {
    matches!(
        key,
        TIKA_CONTENT_KEY
            | EMBEDDED_RESOURCE_PATH_KEY
            | "X-TIKA:content_handler"
            | "X-TIKA:content_handler_type"
            | "X-TIKA:parse_time_millis"
            | "X-TIKA:Parsed-By"
            | "X-TIKA:Parsed-By-Full-Set"
    )
}

fn io_error(path: impl Into<PathBuf>, source: std::io::Error) -> ExtractionError {
    ExtractionError::Io {
        path: path.into(),
        source,
    }
}

// ---------------------------------------------------------------------------
// Pure helpers
// ---------------------------------------------------------------------------

/// Collapse consecutive blank lines down to at most one.
///
/// A line is considered blank if it contains only whitespace (including
/// non-breaking spaces and other Unicode whitespace that Tika may emit).
pub(crate) fn condense_whitespace(text: &str) -> String {
    let mut result = String::with_capacity(text.len());
    let mut blank_run: u32 = 0;
    let mut first = true;
    for line in text.split('\n') {
        if line.trim().is_empty() {
            blank_run += 1;
            if blank_run <= 1 {
                if !first {
                    result.push('\n');
                }
                first = false;
            }
        } else {
            blank_run = 0;
            if !first {
                result.push('\n');
            }
            result.push_str(line);
            first = false;
        }
    }
    result
}

/// Normalise a raw Tika metadata map into the aum domain model.
///
/// Internal `X-TIKA:*` keys are filtered out. JSON arrays become
/// [`MetadataValue::List`]; all other values become [`MetadataValue::Single`].
pub(crate) fn normalize_metadata(raw: &Map<String, Value>) -> HashMap<String, MetadataValue> {
    raw.iter()
        .filter(|(k, _)| !is_tika_internal(k))
        .map(|(k, v)| (k.clone(), to_metadata_value(v)))
        .collect()
}

fn to_metadata_value(v: &Value) -> MetadataValue {
    match v {
        Value::Array(arr) => MetadataValue::List(arr.iter().map(value_to_string).collect()),
        _ => MetadataValue::Single(value_to_string(v)),
    }
}

fn value_to_string(v: &Value) -> String {
    v.as_str().map_or_else(|| v.to_string(), ToOwned::to_owned)
}

/// Return the stable sharded directory for files extracted from a container.
///
/// Shards by the first two pairs of hex digits, namespaced under the index name.
pub(crate) fn container_dir(extract_dir: &Path, index_name: &str, file_path: &Path) -> PathBuf {
    let canonical = file_path
        .canonicalize()
        .unwrap_or_else(|_| file_path.to_path_buf());
    let hash = blake3::hash(canonical.to_string_lossy().as_bytes());
    let hex = hash.to_hex();
    let h = hex.as_str();
    extract_dir
        .join(index_name)
        .join(&h[..2])
        .join(&h[2..4])
        .join(&h[..16])
}

/// Sanitise an archive entry name, preserving directory structure.
///
/// Returns `None` when the name is unsafe (path traversal, null bytes, hidden
/// leaf) so the caller can skip it.
pub(crate) fn safe_archive_path(name: &str) -> Option<PathBuf> {
    if name.is_empty() || name.contains('\x00') || name.starts_with('/') {
        return None;
    }
    let parts: Vec<&str> = name.split('/').filter(|p| *p != ".").collect();
    if parts.is_empty() || parts.contains(&"..") {
        return None;
    }
    if parts.last().is_some_and(|p| p.starts_with('.')) {
        return None;
    }
    Some(parts.iter().collect())
}

/// Identify which embedded-resource-paths are containers (have children).
///
/// A path is a container if any deeper path starts with it as a prefix.
pub(crate) fn find_container_paths<'a>(
    parts: impl Iterator<Item = &'a Map<String, Value>>,
) -> HashSet<String> {
    let mut containers = HashSet::new();
    for part in parts {
        let erp = match part.get(EMBEDDED_RESOURCE_PATH_KEY).and_then(Value::as_str) {
            Some(s) if !s.is_empty() => s,
            _ => continue,
        };
        // Build ancestor paths incrementally instead of re-joining from scratch.
        let segments: Vec<&str> = erp.split('/').filter(|s| !s.is_empty()).collect();
        if segments.len() <= 1 {
            continue;
        }
        let mut prefix = String::with_capacity(erp.len());
        for segment in &segments[..segments.len() - 1] {
            prefix.push('/');
            prefix.push_str(segment);
            containers.insert(prefix.clone());
        }
    }
    containers
}

// ---------------------------------------------------------------------------
// Config & struct
// ---------------------------------------------------------------------------

/// Configuration for a [`TikaExtractor`] instance.
#[derive(Debug, Clone)]
pub struct TikaExtractorConfig {
    /// Base URL of the Tika server (e.g. `http://localhost:9998`).
    pub server_url: String,
    /// Whether to enable OCR for image-based documents.
    pub ocr_enabled: bool,
    /// ISO 639-1 language code for OCR (used when `ocr_enabled` is `true`).
    pub ocr_language: String,
    /// Directory where extracted embedded files are saved.
    pub extract_dir: PathBuf,
    /// Index name, used to namespace extracted files within `extract_dir`.
    pub index_name: String,
    /// Maximum archive nesting depth to recursively unpack; 0 disables recursion.
    pub max_depth: u32,
    /// HTTP request timeout in seconds.
    pub request_timeout_secs: u64,
    /// Maximum character count of document content to retain; 0 means unlimited.
    pub max_content_length: u64,
}

/// Document extractor backed by Apache Tika's HTTP API.
pub struct TikaExtractor {
    client: reqwest::Client,
    config: TikaExtractorConfig,
}

impl TikaExtractor {
    /// Create a new extractor with the given configuration.
    ///
    /// # Errors
    ///
    /// Returns [`ExtractionError::Io`] if the HTTP client cannot be constructed.
    pub fn new(config: TikaExtractorConfig) -> Result<Self, ExtractionError> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.request_timeout_secs))
            .build()
            .map_err(|e| io_error(PathBuf::new(), std::io::Error::other(e)))?;
        Ok(Self { client, config })
    }

    fn tika_url(&self, endpoint: &str) -> String {
        format!(
            "{}/{endpoint}",
            self.config.server_url.trim_end_matches('/')
        )
    }

    fn tika_headers(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        if self.config.ocr_enabled {
            if let Ok(val) = HeaderValue::from_str(&self.config.ocr_language) {
                headers.insert("X-Tika-OCRLanguage", val);
            }
        } else {
            headers.insert("X-Tika-OCRskipOcr", HeaderValue::from_static("true"));
        }
        headers
    }

    // -----------------------------------------------------------------------
    // /rmeta/text
    // -----------------------------------------------------------------------

    /// Call `PUT /rmeta/text` and stream back individual metadata parts.
    ///
    /// Each JSON array element is parsed and yielded individually via a
    /// channel, so memory is bounded to a single part at a time rather
    /// than the entire response. An empty response is normalised to a
    /// single empty part so consumers always receive at least one element.
    fn rmeta<'a>(
        &'a self,
        file_path: &'a Path,
    ) -> BoxStream<'a, Result<Map<String, Value>, ExtractionError>> {
        let span = tracing::debug_span!("rmeta", path = %file_path.display());
        let stream = async_stream::try_stream! {
            let resp = self.send_tika_request(
                file_path,
                "rmeta/text",
                "application/json",
                |path, source| ExtractionError::RmetaConnection { path, source },
                "RmetaConnectionError",
            ).await?;

            let resp = self.check_rmeta_status(file_path, resp).await?;
            let mut rx = Self::spawn_rmeta_parser(file_path, resp);
            let mut got_any = false;
            while let Some(part) = rx.recv().await {
                got_any = true;
                yield part?;
            }
            if !got_any {
                yield Map::new();
            }
        };
        Box::pin(stream.instrument(span))
    }

    /// Spawn a blocking task that streams JSON array elements through a channel.
    fn spawn_rmeta_parser(
        file_path: &Path,
        resp: reqwest::Response,
    ) -> tokio::sync::mpsc::Receiver<Result<Map<String, Value>, ExtractionError>> {
        let path = file_path.to_path_buf();
        let byte_stream = resp
            .bytes_stream()
            .map(|r| r.map_err(std::io::Error::other));
        let sync_reader =
            tokio_util::io::SyncIoBridge::new(tokio_util::io::StreamReader::new(byte_stream));
        let (tx, rx) = tokio::sync::mpsc::channel(16);

        tokio::task::spawn_blocking(move || {
            let mut de = serde_json::Deserializer::from_reader(sync_reader);
            if let Err(e) = de.deserialize_seq(RmetaElementVisitor(&tx)) {
                record_error_metric("RmetaParseError");
                let _ = tx.blocking_send(Err(ExtractionError::RmetaJson { path, source: e }));
            }
        });

        rx
    }

    #[tracing::instrument(skip(self, accept, map_error, error_metric), fields(path = %file_path.display(), endpoint))]
    async fn send_tika_request(
        &self,
        file_path: &Path,
        endpoint: &str,
        accept: &'static str,
        map_error: fn(PathBuf, reqwest::Error) -> ExtractionError,
        error_metric: &str,
    ) -> Result<reqwest::Response, ExtractionError> {
        let file = tokio::fs::File::open(file_path)
            .await
            .map_err(|e| io_error(file_path, e))?;
        let mut headers = self.tika_headers();
        headers.insert(ACCEPT, HeaderValue::from_static(accept));

        self.client
            .put(self.tika_url(endpoint))
            .headers(headers)
            .body(stream_file_body(file))
            .send()
            .await
            .map_err(|e| {
                record_error_metric(error_metric);
                map_error(file_path.to_path_buf(), e)
            })
    }

    async fn check_rmeta_status(
        &self,
        file_path: &Path,
        resp: reqwest::Response,
    ) -> Result<reqwest::Response, ExtractionError> {
        if resp.status() == reqwest::StatusCode::OK {
            return Ok(resp);
        }
        Err(self.rmeta_http_error(file_path, resp.status(), resp).await)
    }

    async fn rmeta_http_error(
        &self,
        file_path: &Path,
        status: reqwest::StatusCode,
        resp: reqwest::Response,
    ) -> ExtractionError {
        let status_u16 = status.as_u16();
        let body: String = resp
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(RMETA_ERROR_BODY_LIMIT)
            .collect();
        record_error_metric(&format!("TikaHTTP{status_u16}"));
        ExtractionError::RmetaHttp {
            path: file_path.to_path_buf(),
            status: status_u16,
            body,
        }
    }

    // -----------------------------------------------------------------------
    // /unpack/all + zip extraction
    // -----------------------------------------------------------------------

    /// Stream `PUT /unpack/all` to a temp zip file, returning it or `None` on 204.
    ///
    /// # Errors
    ///
    /// Returns [`ExtractionError::UnpackConnection`] on transport failure,
    /// [`ExtractionError::UnpackHttp`] on a non-200/204 response, or
    /// [`ExtractionError::Io`] if the temp file cannot be created or written.
    #[tracing::instrument(skip(self), fields(path = %file_path.display()))]
    async fn unpack_raw(
        &self,
        file_path: &Path,
    ) -> Result<Option<tempfile::NamedTempFile>, ExtractionError> {
        let resp = self
            .send_tika_request(
                file_path,
                "unpack/all",
                "application/zip",
                |path, source| ExtractionError::UnpackConnection { path, source },
                "UnpackConnectionError",
            )
            .await?;
        let status = resp.status();

        if status == reqwest::StatusCode::NO_CONTENT {
            return Ok(None);
        }
        if status != reqwest::StatusCode::OK {
            record_error_metric("UnpackHttpError");
            return Err(ExtractionError::UnpackHttp {
                path: file_path.to_path_buf(),
                status: status.as_u16(),
            });
        }

        self.stream_to_tempfile(file_path, resp).await.map(Some)
    }

    #[tracing::instrument(skip(self, resp), fields(path = %file_path.display()))]
    async fn stream_to_tempfile(
        &self,
        file_path: &Path,
        resp: reqwest::Response,
    ) -> Result<tempfile::NamedTempFile, ExtractionError> {
        tokio::fs::create_dir_all(&self.config.extract_dir)
            .await
            .map_err(|e| io_error(self.config.extract_dir.clone(), e))?;

        let tmp = tempfile::Builder::new()
            .suffix(".zip")
            .tempfile_in(&self.config.extract_dir)
            .map_err(|e| io_error(self.config.extract_dir.clone(), e))?;

        let std_file = tmp
            .as_file()
            .try_clone()
            .map_err(|e| io_error(tmp.path(), e))?;
        let mut async_file = tokio::fs::File::from_std(std_file);

        let mut stream = resp.bytes_stream();
        while let Some(chunk) = stream.next().await {
            let bytes = chunk.map_err(|e| ExtractionError::UnpackConnection {
                path: file_path.to_path_buf(),
                source: e,
            })?;
            async_file
                .write_all(&bytes)
                .await
                .map_err(|e| io_error(tmp.path(), e))?;
        }

        Ok(tmp)
    }

    /// Stream zip entries to disk, yielding `(erp, local_path)` for each.
    fn extract_zip_entries<'a>(
        &'a self,
        zip_path: &'a Path,
        dest_dir: &'a Path,
        resolved_dest: &'a Path,
        current_erp: &'a str,
    ) -> BoxStream<'a, Result<(String, PathBuf), ExtractionError>> {
        let span = tracing::debug_span!("extract_zip_entries", path = %zip_path.display());
        let stream = async_stream::try_stream! {
            let reader =
                ZipFileReader::new(zip_path).await.map_err(|e| ExtractionError::Zip {
                    path: zip_path.to_path_buf(),
                    source: e,
                })?;

            let num_entries = reader.file().entries().len();

            for i in 0..num_entries {
                let Some(filename) = read_entry_filename(&reader, i) else { continue };
                if filename.ends_with(".metadata.json") {
                    continue;
                }
                let Some(safe_rel) = safe_archive_path(&filename) else {
                    continue;
                };
                let att_path = dest_dir.join(&safe_rel);
                if !prepare_entry_parent(&att_path, resolved_dest).await {
                    tracing::warn!(
                        name = %filename,
                        dest_dir = %dest_dir.display(),
                        "path traversal blocked"
                    );
                    continue;
                }
                self.write_zip_entry(&reader, i, zip_path, &att_path).await?;

                let child_erp = format!("{current_erp}/{filename}");
                tracing::debug!(
                    attachment = %att_path.display(),
                    erp = %child_erp,
                    "saved attachment"
                );
                yield (child_erp, att_path);
            }
        };
        Box::pin(stream.instrument(span))
    }

    async fn write_zip_entry(
        &self,
        reader: &ZipFileReader,
        index: usize,
        zip_path: &Path,
        att_path: &Path,
    ) -> Result<(), ExtractionError> {
        let entry_reader =
            reader
                .reader_without_entry(index)
                .await
                .map_err(|e| ExtractionError::Zip {
                    path: zip_path.to_path_buf(),
                    source: e,
                })?;
        let mut out_file = tokio::fs::File::create(att_path)
            .await
            .map_err(|e| io_error(att_path, e))?;
        let mut compat_reader = entry_reader.compat();
        tokio::io::copy(&mut compat_reader, &mut out_file)
            .await
            .map_err(|e| io_error(att_path, e))?;
        Ok(())
    }

    /// Recursively unpack embedded files, streaming `(erp, local_path)` pairs.
    ///
    /// Each extracted entry is yielded immediately. Entries identified as
    /// containers (via `container_paths`) are recursively unpacked and their
    /// sub-entries yielded in-line.
    ///
    /// [`ExtractionError::DepthLimitExceeded`] is propagated. Other errors
    /// from recursive sub-archives are logged and the sub-archive is skipped.
    fn unpack_recursive<'a>(
        &'a self,
        file_path: &'a Path,
        container_paths: Arc<HashSet<String>>,
        depth: u32,
        current_erp: String,
    ) -> BoxStream<'a, Result<(String, PathBuf), ExtractionError>> {
        let span = tracing::debug_span!(
            "unpack_recursive",
            path = %file_path.display(),
            depth,
        );
        let stream = async_stream::try_stream! {
            if depth > self.config.max_depth {
                Err(ExtractionError::DepthLimitExceeded {
                    path: file_path.to_path_buf(),
                    max_depth: self.config.max_depth,
                })?;
            }
            let Some(tmp) = self.unpack_raw(file_path).await? else {
                return;
            };
            let (dest_dir, resolved_dest) = self.prepare_dest_dir(file_path).await?;

            // Yield each entry directly from the zip stream, only collecting
            // the subset that needs recursive unpacking.
            let mut containers_to_recurse: Vec<(String, PathBuf)> = Vec::new();
            {
                let mut entry_stream = self.extract_zip_entries(
                    tmp.path(), &dest_dir, &resolved_dest, &current_erp,
                );
                while let Some(entry) = entry_stream.next().await {
                    let (erp, path) = entry?;
                    yield (erp.clone(), path.clone());
                    if container_paths.contains(&erp) {
                        containers_to_recurse.push((erp, path));
                    }
                }
            }
            drop(tmp);

            for (child_erp, att_path) in containers_to_recurse {
                let mut sub = self.unpack_recursive(
                    &att_path,
                    Arc::clone(&container_paths),
                    depth + 1,
                    child_erp.clone(),
                );
                while let Some(entry) = sub.next().await {
                    match entry {
                        Ok(pair) => yield pair,
                        Err(e @ ExtractionError::DepthLimitExceeded { .. }) => Err(e)?,
                        Err(e) => {
                            tracing::warn!(
                                attachment = %att_path.display(),
                                container = %file_path.display(),
                                erp = %child_erp,
                                error = %e,
                                "recursive unpack failed for attachment"
                            );
                            break;
                        }
                    }
                }
            }
        };
        Box::pin(stream.instrument(span))
    }

    async fn prepare_dest_dir(
        &self,
        file_path: &Path,
    ) -> Result<(PathBuf, PathBuf), ExtractionError> {
        let dest_dir = container_dir(&self.config.extract_dir, &self.config.index_name, file_path);
        tokio::fs::create_dir_all(&dest_dir)
            .await
            .map_err(|e| io_error(dest_dir.clone(), e))?;
        let resolved = dest_dir
            .canonicalize()
            .map_err(|e| io_error(dest_dir.clone(), e))?;
        Ok((dest_dir, resolved))
    }

    // -----------------------------------------------------------------------
    // extract() helpers
    // -----------------------------------------------------------------------

    fn truncate_raw_content(&self, raw: String) -> (String, usize, bool) {
        let max = usize::try_from(self.config.max_content_length).unwrap_or(usize::MAX);
        if max == 0 {
            return (raw, 0, false);
        }
        match raw.char_indices().nth(max) {
            None => (raw, 0, false),
            Some((byte_end, _)) => {
                // Count remaining chars beyond the truncation point instead of
                // re-scanning the entire string.
                let remaining = raw[byte_end..].chars().count();
                let original_chars = max + remaining;
                (raw[..byte_end].to_owned(), original_chars, true)
            }
        }
    }

    fn build_embedded_metadata(
        file_path: &Path,
        part: &Map<String, Value>,
        i: usize,
        attachment_map: &HashMap<String, PathBuf>,
        metadata: &mut HashMap<String, MetadataValue>,
    ) -> PathBuf {
        let erp = part
            .get(EMBEDDED_RESOURCE_PATH_KEY)
            .and_then(Value::as_str)
            .unwrap_or("");
        let resource_name = resolve_resource_name(part, erp, i);
        let source = attachment_map
            .get(erp)
            .cloned()
            .unwrap_or_else(|| file_path.to_path_buf());

        let display = if erp.is_empty() {
            file_path.join(&resource_name)
        } else {
            file_path.join(erp.trim_start_matches('/'))
        };
        metadata.insert(
            AUM_DISPLAY_PATH_KEY.to_owned(),
            MetadataValue::Single(display.to_string_lossy().into_owned()),
        );

        let extracted_from = resolve_extracted_from(file_path, erp);
        metadata.insert(
            AUM_EXTRACTED_FROM_KEY.to_owned(),
            MetadataValue::Single(extracted_from.to_string_lossy().into_owned()),
        );

        source
    }

    /// Consume the unpack stream, building the attachment map.
    ///
    /// Returns `(attachment_map, unpack_failed)`. On non-depth errors the
    /// stream is abandoned and embedded documents should be dropped.
    async fn collect_unpack_stream(
        file_path: &Path,
        mut stream: BoxStream<'_, Result<(String, PathBuf), ExtractionError>>,
        embedded_count: usize,
        record_error: Option<&RecordErrorFn>,
    ) -> Result<(HashMap<String, PathBuf>, bool), ExtractionError> {
        let mut attachment_map = HashMap::new();
        while let Some(entry) = stream.next().await {
            match entry {
                Ok((erp, path)) => {
                    attachment_map.insert(erp, path);
                }
                Err(e @ ExtractionError::DepthLimitExceeded { .. }) => return Err(e),
                Err(e) => {
                    tracing::warn!(
                        path = %file_path.display(),
                        embedded_count,
                        error = %e,
                        "unpack failed, dropping embedded documents"
                    );
                    record_error_metric("UnpackError");
                    if let Some(cb) = record_error {
                        cb(
                            file_path,
                            "UnpackError",
                            &format!("failed to unpack {embedded_count} embedded documents: {e}"),
                        );
                    }
                    return Ok((HashMap::new(), true));
                }
            }
        }
        Ok((attachment_map, false))
    }

    fn report_truncation(
        &self,
        source: &Path,
        original_chars: usize,
        record_error: Option<&RecordErrorFn>,
    ) {
        let limit = self.config.max_content_length;
        metrics::counter!("aum_docs_truncated_total").increment(1);
        tracing::warn!(
            path = %source.display(),
            original_chars,
            truncated_chars = limit,
            "content truncated"
        );
        if let Some(cb) = record_error {
            cb(
                source,
                "ContentTruncated",
                &format!(
                    "content truncated from {original_chars} to {limit} chars \
                     (exceeded ingest_max_content_length limit)"
                ),
            );
        }
    }

    async fn check_empty_extraction(doc: &Document) -> bool {
        if !doc.content.is_empty() {
            return false;
        }
        tokio::fs::metadata(&doc.source_path)
            .await
            .map(|m| m.len() > 0)
            .unwrap_or(false)
    }

    fn report_empty_extractions(
        file_path: &Path,
        count: usize,
        record_error: Option<&RecordErrorFn>,
    ) {
        record_error_metric("EmptyExtraction");
        tracing::warn!(path = %file_path.display(), count, "empty extractions");
        if let Some(cb) = record_error {
            cb(
                file_path,
                "EmptyExtraction",
                &format!(
                    "Tika produced no text for {count} document(s) from {}",
                    file_path.display()
                ),
            );
        }
    }
}

// ---------------------------------------------------------------------------
// Free helpers used by build_embedded_metadata
// ---------------------------------------------------------------------------

fn resolve_resource_name(part: &Map<String, Value>, erp: &str, index: usize) -> String {
    part.get(RESOURCE_NAME_KEY)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .or_else(|| erp.rsplit('/').next().filter(|s| !s.is_empty()))
        .map_or_else(|| format!("embedded-{index}"), ToOwned::to_owned)
}

fn resolve_extracted_from(file_path: &Path, erp: &str) -> PathBuf {
    if !erp.is_empty() && erp.trim_matches('/').contains('/') {
        let parent_erp = erp.rsplit_once('/').map_or("", |(p, _)| p);
        file_path.join(parent_erp.trim_start_matches('/'))
    } else {
        file_path.to_path_buf()
    }
}

/// Serde visitor that sends each JSON array element through a channel.
///
/// Used by [`TikaExtractor::spawn_rmeta_parser`] to stream Tika's
/// `/rmeta/text` response one part at a time instead of deserialising the
/// entire array into memory.
struct RmetaElementVisitor<'a>(
    &'a tokio::sync::mpsc::Sender<Result<Map<String, Value>, ExtractionError>>,
);

impl<'de> serde::de::Visitor<'de> for RmetaElementVisitor<'_> {
    type Value = ();

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON array of metadata objects")
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<(), A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        while let Some(element) = seq.next_element::<Map<String, Value>>()? {
            if self.0.blocking_send(Ok(element)).is_err() {
                break; // receiver dropped
            }
        }
        Ok(())
    }
}

fn stream_file_body(file: tokio::fs::File) -> reqwest::Body {
    reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file))
}

fn read_entry_filename(reader: &ZipFileReader, i: usize) -> Option<String> {
    reader
        .file()
        .entries()
        .get(i)?
        .filename()
        .as_str()
        .ok()
        .map(ToOwned::to_owned)
}

/// Create the parent directory for `att_path` and verify it stays within `resolved_dest`.
///
/// Returns `false` if the parent cannot be created or if canonicalization reveals the
/// resolved path escapes the destination (symlink attack). Both failure modes result in
/// the entry being skipped.
async fn prepare_entry_parent(att_path: &Path, resolved_dest: &Path) -> bool {
    let Some(parent) = att_path.parent() else {
        return false;
    };
    if tokio::fs::create_dir_all(parent).await.is_err() {
        return false;
    }
    parent
        .canonicalize()
        .map(|p| p.starts_with(resolved_dest))
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Extractor impl
// ---------------------------------------------------------------------------

impl Extractor for TikaExtractor {
    fn extract<'a>(
        &'a self,
        file_path: &'a Path,
        record_error: Option<&'a RecordErrorFn>,
    ) -> BoxStream<'a, Result<Document, ExtractionError>> {
        let span = tracing::info_span!("extract", path = %file_path.display());
        let stream = async_stream::try_stream! {
            tracing::debug!("extracting document");
            let start = Instant::now();
            let record_duration = || {
                metrics::histogram!("aum_extraction_duration_seconds")
                    .record(start.elapsed().as_secs_f64());
            };

            // Stream rmeta: yield the container document immediately, then
            // collect embedded part metadata for container-path analysis.
            let mut rmeta_stream = self.rmeta(file_path);
            let first_part = rmeta_stream.next().await
                .unwrap_or(Ok(Map::new()))
                .inspect_err(|_| record_duration())?;

            let mut embedded_parts: Vec<Map<String, Value>> = Vec::new();
            while let Some(part) = rmeta_stream.next().await {
                embedded_parts.push(part.inspect_err(|_| record_duration())?);
            }

            let has_embedded = !embedded_parts.is_empty();
            let mut empty_extractions: usize = 0;

            // Yield container document before unpacking starts.
            let container_doc = self.build_one_document(
                file_path, &first_part, 0, &HashMap::new(), record_error,
            );
            if Self::check_empty_extraction(&container_doc).await {
                empty_extractions += 1;
            }
            yield container_doc;

            if has_embedded {
                let container_paths =
                    Arc::new(find_container_paths(embedded_parts.iter()));
                let unpack_stream = self.unpack_recursive(
                    file_path, container_paths, 0, String::new(),
                );
                let (attachment_map, unpack_failed) =
                    Self::collect_unpack_stream(
                        file_path, unpack_stream, embedded_parts.len(), record_error,
                    )
                    .await
                    .inspect_err(|_| record_duration())?;

                if !unpack_failed {
                    for (i, part) in embedded_parts.drain(..).enumerate() {
                        let doc = self.build_one_document(
                            file_path, &part, i + 1, &attachment_map, record_error,
                        );
                        if Self::check_empty_extraction(&doc).await {
                            empty_extractions += 1;
                        }
                        yield doc;
                    }
                }
            }

            record_duration();

            if empty_extractions > 0 {
                Self::report_empty_extractions(
                    file_path, empty_extractions, record_error,
                );
            }

            let total_parts = if has_embedded { 1 + embedded_parts.len() } else { 1 };
            tracing::info!(
                parts = total_parts,
                embedded = has_embedded,
                "extracted document"
            );
        };
        Box::pin(stream.instrument(span))
    }

    fn supports(&self, _mime_type: &str) -> bool {
        true
    }
}

impl TikaExtractor {
    fn build_one_document(
        &self,
        file_path: &Path,
        part: &Map<String, Value>,
        index: usize,
        attachment_map: &HashMap<String, PathBuf>,
        record_error: Option<&RecordErrorFn>,
    ) -> Document {
        let raw = part
            .get(TIKA_CONTENT_KEY)
            .and_then(Value::as_str)
            .unwrap_or("")
            .trim()
            .to_owned();

        let (raw, original_chars, truncated) = self.truncate_raw_content(raw);
        let content = condense_whitespace(&raw);
        let mut metadata = normalize_metadata(part);

        let source = if index == 0 {
            file_path.to_path_buf()
        } else {
            Self::build_embedded_metadata(file_path, part, index, attachment_map, &mut metadata)
        };

        if truncated {
            self.report_truncation(&source, original_chars, record_error);
        }

        Document {
            source_path: source,
            content,
            metadata,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde_json::json;
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use futures::TryStreamExt as _;

    use super::*;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_extractor(tmp: &TempDir, server_url: &str) -> TikaExtractor {
        TikaExtractor::new(TikaExtractorConfig {
            server_url: server_url.to_owned(),
            ocr_enabled: false,
            ocr_language: "eng".to_owned(),
            extract_dir: tmp.path().join("extracted"),
            index_name: "test".to_owned(),
            max_depth: 5,
            request_timeout_secs: 10,
            max_content_length: 0,
        })
        .expect("client build")
    }

    async fn make_zip_bytes(files: &[(&str, &[u8])]) -> Vec<u8> {
        use async_zip::base::write::ZipFileWriter;
        use async_zip::{Compression, ZipEntryBuilder};

        let tmp = tempfile::NamedTempFile::new().expect("tempfile");
        let std_file = tmp.as_file().try_clone().expect("clone");
        let tokio_file = tokio::fs::File::from_std(std_file);
        let mut writer = ZipFileWriter::with_tokio(tokio_file);
        for (name, data) in files {
            let entry = ZipEntryBuilder::new((*name).into(), Compression::Stored).build();
            writer
                .write_entry_whole(entry, data)
                .await
                .expect("write entry");
        }
        writer.close().await.expect("close zip");
        tokio::fs::read(tmp.path()).await.expect("read zip")
    }

    type ErrorLog = Arc<Mutex<Vec<(PathBuf, String, String)>>>;

    fn make_error_log() -> (ErrorLog, RecordErrorFn) {
        let log: ErrorLog = Arc::new(Mutex::new(Vec::new()));
        let log_cb = Arc::clone(&log);
        let cb: RecordErrorFn = Arc::new(move |p, et, msg| {
            log_cb
                .lock()
                .expect("lock")
                .push((p.to_path_buf(), et.to_owned(), msg.to_owned()));
        });
        (log, cb)
    }

    // -----------------------------------------------------------------------
    // Pure function tests
    // -----------------------------------------------------------------------

    #[test]
    fn condense_whitespace_collapses_excess_blank_lines() {
        assert_eq!(condense_whitespace("a\n\n\n\nb"), "a\n\nb");
    }

    #[test]
    fn condense_whitespace_keeps_one_blank_line() {
        let s = "a\n\nb";
        assert_eq!(condense_whitespace(s), s);
    }

    #[test]
    fn condense_whitespace_space_only_lines_are_blank() {
        assert_eq!(condense_whitespace("a\n   \n   \nb"), "a\n\nb");
    }

    #[test]
    fn condense_whitespace_non_breaking_space_is_blank() {
        assert_eq!(condense_whitespace("a\n\u{a0}\n\u{a0}\nb"), "a\n\nb");
    }

    #[test]
    fn condense_whitespace_empty() {
        assert_eq!(condense_whitespace(""), "");
    }

    #[test]
    fn safe_archive_path_simple() {
        assert_eq!(
            safe_archive_path("file.txt"),
            Some(PathBuf::from("file.txt"))
        );
    }

    #[test]
    fn safe_archive_path_nested() {
        assert_eq!(
            safe_archive_path("a/b/c.txt"),
            Some(PathBuf::from("a/b/c.txt"))
        );
    }

    #[test]
    fn safe_archive_path_strips_dot_component() {
        assert_eq!(
            safe_archive_path("./file.txt"),
            Some(PathBuf::from("file.txt"))
        );
    }

    #[test]
    fn safe_archive_path_rejects_double_dot() {
        assert!(safe_archive_path("../escape.txt").is_none());
    }

    #[test]
    fn safe_archive_path_rejects_double_dot_nested() {
        assert!(safe_archive_path("a/../../escape.txt").is_none());
    }

    #[test]
    fn safe_archive_path_rejects_absolute() {
        assert!(safe_archive_path("/etc/passwd").is_none());
    }

    #[test]
    fn safe_archive_path_rejects_null_byte() {
        assert!(safe_archive_path("fi\x00le.txt").is_none());
    }

    #[test]
    fn safe_archive_path_rejects_hidden_leaf() {
        assert!(safe_archive_path(".hidden").is_none());
        assert!(safe_archive_path("subdir/.hidden").is_none());
    }

    #[test]
    fn find_container_paths_flat_archive() {
        let parts = vec![{
            let mut m = Map::new();
            m.insert(EMBEDDED_RESOURCE_PATH_KEY.to_owned(), json!("/file.txt"));
            m
        }];
        assert!(find_container_paths(parts.iter()).is_empty());
    }

    #[test]
    fn find_container_paths_single_level() {
        let parts = vec![
            {
                let mut m = Map::new();
                m.insert(EMBEDDED_RESOURCE_PATH_KEY.to_owned(), json!("/archive.zip"));
                m
            },
            {
                let mut m = Map::new();
                m.insert(
                    EMBEDDED_RESOURCE_PATH_KEY.to_owned(),
                    json!("/archive.zip/doc.pdf"),
                );
                m
            },
        ];
        let c = find_container_paths(parts.iter());
        assert!(c.contains("/archive.zip"));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn find_container_paths_deep_nesting() {
        let parts = vec![{
            let mut m = Map::new();
            m.insert(
                EMBEDDED_RESOURCE_PATH_KEY.to_owned(),
                json!("/a.zip/b.tar/c.txt"),
            );
            m
        }];
        let c = find_container_paths(parts.iter());
        assert!(c.contains("/a.zip"));
        assert!(c.contains("/a.zip/b.tar"));
        assert_eq!(c.len(), 2);
    }

    #[test]
    fn normalize_metadata_strips_internal_keys() {
        let mut raw = Map::new();
        raw.insert(TIKA_CONTENT_KEY.to_owned(), json!("text"));
        raw.insert("X-TIKA:Parsed-By".to_owned(), json!(["p"]));
        raw.insert("dc:title".to_owned(), json!("Doc"));
        let meta = normalize_metadata(&raw);
        assert!(!meta.contains_key(TIKA_CONTENT_KEY));
        assert!(!meta.contains_key("X-TIKA:Parsed-By"));
        assert!(meta.contains_key("dc:title"));
    }

    #[test]
    fn normalize_metadata_array_to_list() {
        let mut raw = Map::new();
        raw.insert("tags".to_owned(), json!(["a", "b"]));
        let meta = normalize_metadata(&raw);
        assert_eq!(
            meta["tags"],
            MetadataValue::List(vec!["a".into(), "b".into()])
        );
    }

    #[test]
    fn normalize_metadata_scalar_to_single() {
        let mut raw = Map::new();
        raw.insert("dc:title".to_owned(), json!("Hello"));
        raw.insert("pages".to_owned(), json!(42));
        let meta = normalize_metadata(&raw);
        assert_eq!(meta["dc:title"], MetadataValue::Single("Hello".into()));
        assert_eq!(meta["pages"], MetadataValue::Single("42".into()));
    }

    // -----------------------------------------------------------------------
    // HTTP integration tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn extract_simple_document() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([{
                "X-TIKA:content": "  Hello world  ",
                "dc:title": "My Doc",
            }])))
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("doc.pdf");
        tokio::fs::write(&source, b"pdf").await.expect("write");

        let extractor = make_extractor(&tmp, &server.uri());
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].content, "Hello world");
        assert_eq!(docs[0].source_path, source);
        assert!(!docs[0].metadata.contains_key("X-TIKA:content"));

        // No /unpack/all call for a simple document.
        assert_eq!(server.received_requests().await.expect("reqs").len(), 1);
    }

    #[tokio::test]
    async fn extract_empty_rmeta_gives_one_empty_doc() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([])))
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("empty.pdf");
        tokio::fs::write(&source, b"").await.expect("write");

        let extractor = make_extractor(&tmp, &server.uri());
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        assert_eq!(docs.len(), 1);
        assert!(docs[0].content.is_empty());
    }

    #[tokio::test]
    async fn extract_rmeta_http_error() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(500).set_body_string("error"))
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("doc.pdf");
        tokio::fs::write(&source, b"x").await.expect("write");

        let extractor = make_extractor(&tmp, &server.uri());
        let result: Result<Vec<_>, _> = extractor.extract(&source, None).try_collect().await;
        assert!(matches!(
            result,
            Err(ExtractionError::RmetaHttp { status: 500, .. })
        ));
    }

    #[tokio::test]
    async fn extract_internal_keys_stripped() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([{
                "X-TIKA:content": "text",
                "X-TIKA:content_handler": "h",
                "X-TIKA:parse_time_millis": "10",
                "dc:title": "kept",
            }])))
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("doc.pdf");
        tokio::fs::write(&source, b"x").await.expect("write");

        let extractor = make_extractor(&tmp, &server.uri());
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        assert!(!docs[0].metadata.contains_key("X-TIKA:content_handler"));
        assert!(!docs[0].metadata.contains_key("X-TIKA:parse_time_millis"));
        assert!(docs[0].metadata.contains_key("dc:title"));
    }

    #[tokio::test]
    async fn extract_content_truncated() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(&json!([{"X-TIKA:content": "abcdefghij1234567890"}])),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("doc.txt");
        tokio::fs::write(&source, b"x").await.expect("write");

        let (log, cb) = make_error_log();
        let extractor = TikaExtractor::new(TikaExtractorConfig {
            max_content_length: 10,
            server_url: server.uri(),
            ..make_extractor(&tmp, &server.uri()).config
        })
        .expect("client");

        let docs = extractor
            .extract(&source, Some(&cb))
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");
        assert_eq!(docs[0].content, "abcdefghij");

        let errors = log.lock().expect("lock");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].1, "ContentTruncated");
    }

    #[tokio::test]
    async fn extract_content_truncation_is_char_safe() {
        // 7 multibyte chars, limit 3 → "こんに"
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(&json!([{"X-TIKA:content": "こんにちは世界"}])),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("doc.txt");
        tokio::fs::write(&source, b"x").await.expect("write");

        let extractor = TikaExtractor::new(TikaExtractorConfig {
            max_content_length: 3,
            server_url: server.uri(),
            ..make_extractor(&tmp, &server.uri()).config
        })
        .expect("client");

        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");
        assert_eq!(docs[0].content, "こんに");
    }

    #[tokio::test]
    async fn extract_with_embedded_calls_unpack() {
        let zip_bytes = make_zip_bytes(&[("attach.txt", b"attachment content")]).await;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([
                {"X-TIKA:content": "email body"},
                {
                    "X-TIKA:content": "attachment text",
                    "X-TIKA:embedded_resource_path": "/attach.txt",
                    "resourceName": "attach.txt",
                }
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack/all"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(zip_bytes)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("email.eml");
        tokio::fs::write(&source, b"raw email")
            .await
            .expect("write");

        let extractor = make_extractor(&tmp, &server.uri());
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].content, "email body");
        assert_eq!(docs[1].content, "attachment text");
        // Embedded doc source is the extracted file, not the container.
        assert!(docs[1].source_path.exists());
        assert_ne!(docs[1].source_path, source);
    }

    #[tokio::test]
    async fn extract_unpack_204_keeps_both_parts() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([
                {"X-TIKA:content": "email body"},
                {"X-TIKA:content": "part", "X-TIKA:embedded_resource_path": "/part.txt"},
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack/all"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("email.eml");
        tokio::fs::write(&source, b"raw").await.expect("write");

        let extractor = make_extractor(&tmp, &server.uri());
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        // Both parts returned; embedded falls back to container path.
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[1].source_path, source);
    }

    #[tokio::test]
    async fn extract_unpack_failure_drops_embedded() {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([
                {"X-TIKA:content": "container"},
                {"X-TIKA:content": "e1", "X-TIKA:embedded_resource_path": "/a.txt"},
                {"X-TIKA:content": "e2", "X-TIKA:embedded_resource_path": "/b.txt"},
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack/all"))
            .respond_with(ResponseTemplate::new(500).set_body_string("error"))
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("archive.zip");
        tokio::fs::write(&source, b"data").await.expect("write");

        let (log, cb) = make_error_log();
        let extractor = make_extractor(&tmp, &server.uri());
        let docs = extractor
            .extract(&source, Some(&cb))
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].content, "container");

        let errors = log.lock().expect("lock");
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].1, "UnpackError");
        assert!(errors[0].2.contains("2 embedded"));
    }

    #[tokio::test]
    async fn extract_depth_limit_exceeded() {
        let inner_zip = make_zip_bytes(&[("inner.txt", b"deep")]).await;
        let outer_zip = make_zip_bytes(&[("inner.zip", &inner_zip)]).await;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([
                {"X-TIKA:content": "outer"},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/inner.zip"},
                {"X-TIKA:content": "deep", "X-TIKA:embedded_resource_path": "/inner.zip/inner.txt"},
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack/all"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(outer_zip)
                    .insert_header("Content-Type", "application/zip"),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack/all"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(make_zip_bytes(&[("inner.txt", b"deep")]).await)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("outer.zip");
        tokio::fs::write(&source, b"data").await.expect("write");

        let extractor = TikaExtractor::new(TikaExtractorConfig {
            max_depth: 0,
            server_url: server.uri(),
            ..make_extractor(&tmp, &server.uri()).config
        })
        .expect("client");

        let result: Result<Vec<_>, _> = extractor.extract(&source, None).try_collect().await;
        assert!(
            matches!(result, Err(ExtractionError::DepthLimitExceeded { .. })),
            "expected DepthLimitExceeded, got {result:?}"
        );
    }

    #[tokio::test]
    async fn extract_empty_file_no_error() {
        // Zero-byte file: empty content but no EmptyExtraction error.
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(&json!([{"X-TIKA:content": ""}])),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("empty.txt");
        tokio::fs::write(&source, b"").await.expect("write");

        let (log, cb) = make_error_log();
        let extractor = make_extractor(&tmp, &server.uri());
        extractor
            .extract(&source, Some(&cb))
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        assert!(log.lock().expect("lock").is_empty());
    }

    #[tokio::test]
    async fn extract_many_empty_parts_single_error() {
        let zip_bytes = make_zip_bytes(&[("a.bin", b"a"), ("b.bin", b"b"), ("c.bin", b"c")]).await;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([
                {"X-TIKA:content": ""},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/a.bin"},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/b.bin"},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/c.bin"},
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack/all"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(zip_bytes)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("archive.zip");
        tokio::fs::write(&source, b"data").await.expect("write");

        let (log, cb) = make_error_log();
        let extractor = make_extractor(&tmp, &server.uri());
        extractor
            .extract(&source, Some(&cb))
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        let errors = log.lock().expect("lock");
        let empty_errors: Vec<_> = errors.iter().filter(|e| e.1 == "EmptyExtraction").collect();
        assert_eq!(empty_errors.len(), 1);
    }

    #[tokio::test]
    async fn extract_subdir_structure_preserved() {
        let zip_bytes = make_zip_bytes(&[("subdir/file.txt", b"nested")]).await;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([
                {"X-TIKA:content": "container"},
                {
                    "X-TIKA:content": "nested",
                    "X-TIKA:embedded_resource_path": "/subdir/file.txt",
                }
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack/all"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(zip_bytes)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("archive.zip");
        tokio::fs::write(&source, b"data").await.expect("write");

        let extractor = make_extractor(&tmp, &server.uri());
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        assert_eq!(docs.len(), 2);
        assert!(docs[1].source_path.exists());
        assert!(docs[1].source_path.to_string_lossy().contains("subdir"));
    }

    #[tokio::test]
    async fn extract_display_path_and_extracted_from_set() {
        let zip_bytes = make_zip_bytes(&[("attach.pdf", b"content")]).await;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(&json!([
                {"X-TIKA:content": "email"},
                {
                    "X-TIKA:content": "attach",
                    "X-TIKA:embedded_resource_path": "/attach.pdf",
                    "resourceName": "attach.pdf",
                }
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack/all"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(zip_bytes)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new().expect("tempdir");
        let source = tmp.path().join("email.eml");
        tokio::fs::write(&source, b"raw").await.expect("write");

        let extractor = make_extractor(&tmp, &server.uri());
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await
            .expect("extract");

        let meta = &docs[1].metadata;
        let MetadataValue::Single(display) = &meta[AUM_DISPLAY_PATH_KEY] else {
            panic!("expected Single for {AUM_DISPLAY_PATH_KEY}");
        };
        assert!(display.contains("email.eml") && display.contains("attach.pdf"));

        let MetadataValue::Single(extracted_from) = &meta[AUM_EXTRACTED_FROM_KEY] else {
            panic!("expected Single for {AUM_EXTRACTED_FROM_KEY}");
        };
        assert!(extracted_from.contains("email.eml"));
    }
}
