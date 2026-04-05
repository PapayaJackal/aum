//! Apache Tika document extraction backend.
//!
//! Uses Tika's HTTP API:
//! - `PUT /rmeta/text` for recursive text and metadata extraction.
//! - `PUT /unpack` to retrieve direct-child embedded files; called recursively
//!   on each extracted entry so nested attachments are also available.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
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

use crate::extraction::{
    AUM_DISPLAY_PATH_KEY, AUM_EXTRACTED_FROM_KEY, ExtractionError, Extractor, RecordErrorFn,
};
use crate::models::{Document, MetadataValue};

// ---------------------------------------------------------------------------
// Tika metadata key constants
// ---------------------------------------------------------------------------

const TIKA_CONTENT_KEY: &str = "X-TIKA:content";
const EMBEDDED_RESOURCE_PATH_KEY: &str = "X-TIKA:embedded_resource_path";
const RESOURCE_NAME_KEY: &str = "resourceName";
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

/// Sanitise a zip entry name, extracting just the leaf filename.
///
/// Returns `None` when the name is unsafe (empty, null bytes, path traversal,
/// hidden file) so the caller can skip it.  With `/unpack` (direct children
/// only) entries should already be leaf names, but we strip any directory
/// components defensively.
pub(crate) fn safe_entry_name(name: &str) -> Option<&str> {
    if name.is_empty() || name.contains('\x00') {
        return None;
    }
    // Take only the last path component — `/unpack` should return leaf names
    // but some Tika versions include directory prefixes.
    let leaf = name
        .rsplit('/')
        .find(|s| !s.is_empty() && *s != "." && *s != "..")?;
    if leaf.starts_with('.') {
        return None;
    }
    Some(leaf)
}

/// Identify which embedded-resource-paths are containers (have children).
///
/// A path is a container if any deeper path starts with it as a prefix.
/// Used to avoid pointless `/unpack` round-trips on leaf entries.
pub(crate) fn find_container_paths<'a>(
    all_erps: impl IntoIterator<Item = &'a str>,
) -> HashSet<String> {
    let mut containers = HashSet::new();
    for erp in all_erps {
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
    // /unpack + zip extraction
    // -----------------------------------------------------------------------

    /// Stream `PUT /unpack` to a temp zip file, returning it or `None` on 204.
    ///
    /// Unlike `/unpack/all` which flattens the entire tree, `/unpack` returns
    /// only direct children.  We call it recursively on each extracted entry
    /// to reach nested attachments.
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
                "unpack",
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
        async_file
            .shutdown()
            .await
            .map_err(|e| io_error(tmp.path(), e))?;

        Ok(tmp)
    }

    /// Stream zip entries to disk, yielding `(erp, local_path)` for each.
    ///
    /// Because we use `/unpack` (direct children only), entries are leaf
    /// filenames — no nested paths and therefore no file/directory conflicts.
    ///
    /// Tika's `/unpack` may name entries differently from the ERPs reported
    /// by `/rmeta/text`.  In particular, unnamed MIME parts get `embedded-N`
    /// ERPs in rmeta but show up as `{N-1}.ext` in the unpack zip.  The
    /// `erp_to_idx` map is consulted to resolve the canonical ERP for each
    /// entry, trying both the literal leaf name and the `embedded-N` fallback.
    fn extract_zip_entries<'a>(
        &'a self,
        zip_path: &'a Path,
        dest_dir: &'a Path,
        current_erp: &'a str,
        erp_to_idx: &'a HashMap<String, usize>,
    ) -> BoxStream<'a, Result<(String, PathBuf), ExtractionError>> {
        let span = tracing::debug_span!("extract_zip_entries", path = %zip_path.display());
        let stream = async_stream::try_stream! {
            let reader =
                ZipFileReader::new(zip_path).await.map_err(|e| ExtractionError::Zip {
                    path: zip_path.to_path_buf(),
                    source: e,
                })?;

            let num_entries = reader.file().entries().len();

            // Running count of real (non-skipped) entries, used to map
            // Tika's `N.ext` zip names back to `embedded-{N+1}` ERPs.
            let mut entry_ordinal: u32 = 0;

            for i in 0..num_entries {
                let entry = &reader.file().entries()[i];
                if entry.dir().unwrap_or(false) {
                    continue;
                }
                // Reject symlinks: Unix mode type bits 0o120000 indicate a symbolic link.
                if let Some(mode) = entry.unix_permissions()
                    && mode & 0o170_000 == 0o120_000
                {
                    tracing::warn!(index = i, "skipping symlink zip entry");
                    continue;
                }
                let Some(raw_name) = read_entry_filename(&reader, i) else { continue };
                if raw_name.ends_with(".metadata.json")
                    || raw_name == "__TEXT__"
                    || raw_name == "__METADATA__"
                {
                    continue;
                }
                let Some(leaf) = safe_entry_name(&raw_name) else {
                    continue;
                };
                let att_path = dest_dir.join(leaf);
                self.write_zip_entry(&reader, i, zip_path, &att_path).await?;

                // Defense-in-depth: async_zip may materialise a symlink if the
                // entry's external attributes are crafted despite the pre-write
                // mode-bits check above.
                let meta = tokio::fs::symlink_metadata(&att_path)
                    .await
                    .map_err(|e| io_error(&att_path, e))?;
                if !meta.is_file() {
                    tracing::warn!(path = %att_path.display(), "extracted entry is not a regular file, removing");
                    let _ = tokio::fs::remove_file(&att_path).await;
                    continue;
                }

                // Try the literal name first, then the embedded-N fallback
                // for Tika's unnamed MIME part naming convention.
                let literal_erp = format!("{current_erp}/{leaf}");
                let child_erp = if erp_to_idx.contains_key(&literal_erp) {
                    literal_erp
                } else {
                    let fallback = format!("{current_erp}/embedded-{}", entry_ordinal + 1);
                    if erp_to_idx.contains_key(&fallback) {
                        fallback
                    } else {
                        literal_erp
                    }
                };
                entry_ordinal += 1;

                tracing::info!(
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
    /// Each extracted entry is yielded immediately.  Entries known to be
    /// containers (from the rmeta-derived `container_paths` set) are then
    /// recursively unpacked and their sub-entries yielded in-line.  Leaf
    /// entries are skipped to avoid unnecessary `/unpack` round-trips.
    ///
    /// [`ExtractionError::DepthLimitExceeded`] is propagated. Other errors
    /// from recursive sub-archives are logged and the sub-archive is skipped.
    fn unpack_recursive<'a>(
        &'a self,
        file_path: &'a Path,
        erp_to_idx: &'a HashMap<String, usize>,
        container_paths: &'a HashSet<String>,
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
            let dest_dir = self.prepare_dest_dir(file_path).await?;

            // Collect direct children, then recurse into known containers.
            let mut containers_to_recurse: Vec<(String, PathBuf)> = Vec::new();
            {
                let mut entry_stream = self.extract_zip_entries(
                    tmp.path(), &dest_dir, &current_erp, erp_to_idx,
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
                    erp_to_idx,
                    container_paths,
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

    async fn prepare_dest_dir(&self, file_path: &Path) -> Result<PathBuf, ExtractionError> {
        let dest_dir = container_dir(&self.config.extract_dir, &self.config.index_name, file_path);
        tokio::fs::create_dir_all(&dest_dir)
            .await
            .map_err(|e| io_error(dest_dir.clone(), e))?;
        Ok(dest_dir)
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
        attachment_source: Option<&Path>,
        metadata: &mut HashMap<String, MetadataValue>,
    ) -> PathBuf {
        let erp = part
            .get(EMBEDDED_RESOURCE_PATH_KEY)
            .and_then(Value::as_str)
            .unwrap_or("");
        let resource_name = resolve_resource_name(part, erp, i);
        let source = attachment_source.map_or_else(|| file_path.to_path_buf(), Path::to_path_buf);

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

/// Extract the display path from a document's metadata, if present.
fn doc_display_path(doc: &Document) -> &str {
    doc.metadata
        .get(AUM_DISPLAY_PATH_KEY)
        .and_then(|v| match v {
            MetadataValue::Single(s) => Some(s.as_str()),
            MetadataValue::List(_) => None,
        })
        .unwrap_or("")
}

// ---------------------------------------------------------------------------
// Extractor impl
// ---------------------------------------------------------------------------

impl Extractor for TikaExtractor {
    #[allow(clippy::too_many_lines)]
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
                file_path, &first_part, 0, None, record_error,
            ).await;
            if Self::check_empty_extraction(&container_doc).await {
                empty_extractions += 1;
            }
            yield container_doc;

            if has_embedded {
                // Index embedded parts by their embedded-resource-path so we
                // can yield documents incrementally as unpack entries arrive
                // instead of collecting the entire attachment map first.
                let mut erp_to_idx: HashMap<String, usize> =
                    HashMap::with_capacity(embedded_parts.len());
                for (i, part) in embedded_parts.iter().enumerate() {
                    if let Some(erp) = part
                        .get(EMBEDDED_RESOURCE_PATH_KEY)
                        .and_then(Value::as_str)
                    {
                        erp_to_idx.insert(erp.to_owned(), i);
                    }
                }

                let container_paths = find_container_paths(erp_to_idx.keys().map(String::as_str));

                let mut yielded = vec![false; embedded_parts.len()];
                let mut unpack_failed = false;
                let embedded_count = embedded_parts.len();

                {
                    let mut unpack_stream = self.unpack_recursive(
                        file_path, &erp_to_idx, &container_paths, 0, String::new(),
                    );
                    while let Some(entry) = unpack_stream.next().await {
                        match entry {
                            Ok((erp, local_path)) => {
                                if let Some(&idx) = erp_to_idx.get(&erp)
                                    && !yielded[idx]
                                {
                                    yielded[idx] = true;
                                    let doc = self.build_one_document(
                                        file_path, &embedded_parts[idx],
                                        idx + 1, Some(&local_path), record_error,
                                    ).await;
                                    let empty = Self::check_empty_extraction(&doc).await;
                                    if empty {
                                        empty_extractions += 1;
                                    }
                                    let display_path = doc_display_path(&doc);
                                    tracing::info!(
                                        attachment = display_path,
                                        content_chars = doc.content.len(),
                                        empty,
                                        "indexing attachment"
                                    );
                                    yield doc;
                                }
                            }
                            Err(e @ ExtractionError::DepthLimitExceeded { .. }) => {
                                record_duration();
                                Err(e)?;
                            }
                            Err(e) => {
                                tracing::warn!(
                                    path = %file_path.display(),
                                    embedded_count,
                                    error = %e,
                                    "unpack failed, dropping remaining embedded documents"
                                );
                                record_error_metric("UnpackError");
                                if let Some(cb) = record_error {
                                    cb(
                                        file_path,
                                        "UnpackError",
                                        &format!(
                                            "failed to unpack {embedded_count} embedded documents: {e}"
                                        ),
                                    );
                                }
                                unpack_failed = true;
                                break;
                            }
                        }
                    }
                }

                // Yield remaining embedded parts whose erp didn't match any
                // unpack entry (they fall back to the container as source).
                if !unpack_failed {
                    for (i, part) in embedded_parts.iter().enumerate() {
                        if !yielded[i] {
                            let doc = self.build_one_document(
                                file_path, part, i + 1, None, record_error,
                            ).await;
                            let empty = Self::check_empty_extraction(&doc).await;
                            if empty {
                                empty_extractions += 1;
                            }
                            let display_path = doc
                                .metadata
                                .get(AUM_DISPLAY_PATH_KEY)
                                .and_then(|v| match v {
                                    MetadataValue::Single(s) => Some(s.as_str()),
                                    MetadataValue::List(_) => None,
                                })
                                .unwrap_or("");
                            tracing::info!(
                                attachment = display_path,
                                content_chars = doc.content.len(),
                                empty,
                                "indexing attachment"
                            );
                            yield doc;
                        }
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
    async fn build_one_document(
        &self,
        file_path: &Path,
        part: &Map<String, Value>,
        index: usize,
        attachment_source: Option<&Path>,
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
            Self::build_embedded_metadata(file_path, part, index, attachment_source, &mut metadata)
        };

        if truncated {
            self.report_truncation(&source, original_chars, record_error);
        }

        // Inject file size from the filesystem so it's available as metadata regardless
        // of whether Tika includes a Content-Length field in its rmeta output.
        if let Ok(fs_meta) = tokio::fs::metadata(&source).await {
            metadata
                .entry("Content-Length".to_owned())
                .or_insert_with(|| MetadataValue::Single(fs_meta.len().to_string()));
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
    use std::sync::{Arc, Mutex};

    use anyhow::Context as _;
    use serde_json::json;
    use tempfile::TempDir;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use futures::TryStreamExt as _;

    use super::*;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn make_extractor(tmp: &TempDir, server_url: &str) -> anyhow::Result<TikaExtractor> {
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
        .context("make_extractor")
    }

    async fn make_zip_bytes(files: &[(&str, &[u8])]) -> anyhow::Result<Vec<u8>> {
        use async_zip::base::write::ZipFileWriter;
        use async_zip::{Compression, ZipEntryBuilder};

        let tmp = tempfile::NamedTempFile::new().context("tempfile")?;
        let std_file = tmp.as_file().try_clone().context("clone")?;
        let tokio_file = tokio::fs::File::from_std(std_file);
        let mut writer = ZipFileWriter::with_tokio(tokio_file);
        for (name, data) in files {
            let entry = ZipEntryBuilder::new((*name).into(), Compression::Stored).build();
            writer
                .write_entry_whole(entry, data)
                .await
                .context("write entry")?;
        }
        writer.close().await.context("close zip")?;
        tokio::fs::read(tmp.path()).await.context("read zip")
    }

    type ErrorLog = Arc<Mutex<Vec<(PathBuf, String, String)>>>;

    fn make_error_log() -> (ErrorLog, RecordErrorFn) {
        let log: ErrorLog = Arc::new(Mutex::new(Vec::new()));
        let log_cb = Arc::clone(&log);
        let cb: RecordErrorFn = Arc::new(move |p, et, msg| {
            log_cb
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
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
    fn safe_entry_name_simple() {
        assert_eq!(safe_entry_name("file.txt"), Some("file.txt"));
    }

    #[test]
    fn safe_entry_name_strips_directory() {
        assert_eq!(safe_entry_name("subdir/file.txt"), Some("file.txt"));
        assert_eq!(safe_entry_name("a/b/c.txt"), Some("c.txt"));
    }

    #[test]
    fn safe_entry_name_strips_dot_component() {
        assert_eq!(safe_entry_name("./file.txt"), Some("file.txt"));
    }

    #[test]
    fn safe_entry_name_rejects_double_dot_only() {
        assert!(safe_entry_name("..").is_none());
    }

    #[test]
    fn safe_entry_name_rejects_null_byte() {
        assert!(safe_entry_name("fi\x00le.txt").is_none());
    }

    #[test]
    fn safe_entry_name_rejects_hidden() {
        assert!(safe_entry_name(".hidden").is_none());
        assert!(safe_entry_name("subdir/.hidden").is_none());
    }

    #[test]
    fn safe_entry_name_rejects_empty() {
        assert!(safe_entry_name("").is_none());
    }

    #[test]
    fn find_container_paths_flat_archive() {
        assert!(find_container_paths(["/file.txt"]).is_empty());
    }

    #[test]
    fn find_container_paths_single_level() {
        let c = find_container_paths(["/archive.zip", "/archive.zip/doc.pdf"]);
        assert!(c.contains("/archive.zip"));
        assert_eq!(c.len(), 1);
    }

    #[test]
    fn find_container_paths_deep_nesting() {
        let c = find_container_paths(["/a.zip/b.tar/c.txt"]);
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
    async fn extract_simple_document() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
                "X-TIKA:content": "  Hello world  ",
                "dc:title": "My Doc",
            }])))
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("doc.pdf");
        tokio::fs::write(&source, b"pdf").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].content, "Hello world");
        assert_eq!(docs[0].source_path, source);
        assert!(!docs[0].metadata.contains_key("X-TIKA:content"));

        // No /unpack call for a simple document.
        let reqs = server
            .received_requests()
            .await
            .context("request recording disabled")?;
        assert_eq!(reqs.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn extract_empty_rmeta_gives_one_empty_doc() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([])))
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("empty.pdf");
        tokio::fs::write(&source, b"").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(docs.len(), 1);
        assert!(docs[0].content.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn extract_rmeta_http_error() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(500).set_body_string("error"))
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("doc.pdf");
        tokio::fs::write(&source, b"x").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let result: Result<Vec<_>, _> = extractor.extract(&source, None).try_collect().await;
        assert!(matches!(
            result,
            Err(ExtractionError::RmetaHttp { status: 500, .. })
        ));
        Ok(())
    }

    #[tokio::test]
    async fn extract_internal_keys_stripped() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{
                "X-TIKA:content": "text",
                "X-TIKA:content_handler": "h",
                "X-TIKA:parse_time_millis": "10",
                "dc:title": "kept",
            }])))
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("doc.pdf");
        tokio::fs::write(&source, b"x").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;

        assert!(!docs[0].metadata.contains_key("X-TIKA:content_handler"));
        assert!(!docs[0].metadata.contains_key("X-TIKA:parse_time_millis"));
        assert!(docs[0].metadata.contains_key("dc:title"));
        Ok(())
    }

    #[tokio::test]
    async fn extract_content_truncated() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!([{"X-TIKA:content": "abcdefghij1234567890"}])),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("doc.txt");
        tokio::fs::write(&source, b"x").await?;

        let (log, cb) = make_error_log();
        let extractor = TikaExtractor::new(TikaExtractorConfig {
            max_content_length: 10,
            server_url: server.uri(),
            ..make_extractor(&tmp, &server.uri())?.config
        })?;

        let docs = extractor
            .extract(&source, Some(&cb))
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(docs[0].content, "abcdefghij");

        let errors = log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].1, "ContentTruncated");
        Ok(())
    }

    #[tokio::test]
    async fn extract_content_truncation_is_char_safe() -> anyhow::Result<()> {
        // 7 multibyte chars, limit 3 → "こんに"
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!([{"X-TIKA:content": "こんにちは世界"}])),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("doc.txt");
        tokio::fs::write(&source, b"x").await?;

        let extractor = TikaExtractor::new(TikaExtractorConfig {
            max_content_length: 3,
            server_url: server.uri(),
            ..make_extractor(&tmp, &server.uri())?.config
        })?;

        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;
        assert_eq!(docs[0].content, "こんに");
        Ok(())
    }

    #[tokio::test]
    async fn extract_with_embedded_calls_unpack() -> anyhow::Result<()> {
        let zip_bytes = make_zip_bytes(&[("attach.txt", b"attachment content")]).await?;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
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
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(zip_bytes)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("email.eml");
        tokio::fs::write(&source, b"raw email").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(docs.len(), 2);
        assert_eq!(docs[0].content, "email body");
        assert_eq!(docs[1].content, "attachment text");
        // Embedded doc source is the extracted file, not the container.
        assert!(docs[1].source_path.exists());
        assert_ne!(docs[1].source_path, source);
        Ok(())
    }

    #[tokio::test]
    async fn extract_unpack_204_keeps_both_parts() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"X-TIKA:content": "email body"},
                {"X-TIKA:content": "part", "X-TIKA:embedded_resource_path": "/part.txt"},
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(ResponseTemplate::new(204))
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("email.eml");
        tokio::fs::write(&source, b"raw").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;

        // Both parts returned; embedded falls back to container path.
        assert_eq!(docs.len(), 2);
        assert_eq!(docs[1].source_path, source);
        Ok(())
    }

    #[tokio::test]
    async fn extract_unpack_failure_drops_embedded() -> anyhow::Result<()> {
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"X-TIKA:content": "container"},
                {"X-TIKA:content": "e1", "X-TIKA:embedded_resource_path": "/a.txt"},
                {"X-TIKA:content": "e2", "X-TIKA:embedded_resource_path": "/b.txt"},
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(ResponseTemplate::new(500).set_body_string("error"))
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("archive.zip");
        tokio::fs::write(&source, b"data").await?;

        let (log, cb) = make_error_log();
        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, Some(&cb))
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(docs.len(), 1);
        assert_eq!(docs[0].content, "container");

        let errors = log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert_eq!(errors.len(), 1);
        assert_eq!(errors[0].1, "UnpackError");
        assert!(errors[0].2.contains("2 embedded"));
        Ok(())
    }

    #[tokio::test]
    async fn extract_depth_limit_exceeded() -> anyhow::Result<()> {
        // Outer /unpack returns inner.zip as a direct child.
        let outer_zip = make_zip_bytes(&[("inner.zip", b"fake zip data")]).await?;
        // Inner /unpack on inner.zip returns inner.txt.
        let inner_zip = make_zip_bytes(&[("inner.txt", b"deep")]).await?;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"X-TIKA:content": "outer"},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/inner.zip"},
                {"X-TIKA:content": "deep", "X-TIKA:embedded_resource_path": "/inner.zip/inner.txt"},
            ])))
            .mount(&server)
            .await;
        // First /unpack (outer) returns inner.zip.
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(outer_zip)
                    .insert_header("Content-Type", "application/zip"),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Second /unpack (inner.zip) returns inner.txt.
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(inner_zip)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("outer.zip");
        tokio::fs::write(&source, b"data").await?;

        let extractor = TikaExtractor::new(TikaExtractorConfig {
            max_depth: 0,
            server_url: server.uri(),
            ..make_extractor(&tmp, &server.uri())?.config
        })?;

        let result: Result<Vec<_>, _> = extractor.extract(&source, None).try_collect().await;
        assert!(
            matches!(result, Err(ExtractionError::DepthLimitExceeded { .. })),
            "expected DepthLimitExceeded, got {result:?}"
        );
        Ok(())
    }

    #[tokio::test]
    async fn extract_empty_file_no_error() -> anyhow::Result<()> {
        // Zero-byte file: empty content but no EmptyExtraction error.
        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([{"X-TIKA:content": ""}])))
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("empty.txt");
        tokio::fs::write(&source, b"").await?;

        let (log, cb) = make_error_log();
        let extractor = make_extractor(&tmp, &server.uri())?;
        extractor
            .extract(&source, Some(&cb))
            .try_collect::<Vec<_>>()
            .await?;

        assert!(
            log.lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_empty()
        );
        Ok(())
    }

    #[tokio::test]
    async fn extract_many_empty_parts_single_error() -> anyhow::Result<()> {
        let zip_bytes =
            make_zip_bytes(&[("a.bin", b"a"), ("b.bin", b"b"), ("c.bin", b"c")]).await?;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"X-TIKA:content": ""},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/a.bin"},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/b.bin"},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/c.bin"},
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(zip_bytes)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("archive.zip");
        tokio::fs::write(&source, b"data").await?;

        let (log, cb) = make_error_log();
        let extractor = make_extractor(&tmp, &server.uri())?;
        extractor
            .extract(&source, Some(&cb))
            .try_collect::<Vec<_>>()
            .await?;

        let errors = log
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let empty_errors: Vec<_> = errors.iter().filter(|e| e.1 == "EmptyExtraction").collect();
        assert_eq!(empty_errors.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn extract_attachment_source_path_is_extracted_file() -> anyhow::Result<()> {
        let zip_bytes = make_zip_bytes(&[("file.txt", b"nested")]).await?;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"X-TIKA:content": "container"},
                {
                    "X-TIKA:content": "nested",
                    "X-TIKA:embedded_resource_path": "/file.txt",
                }
            ])))
            .mount(&server)
            .await;
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(zip_bytes)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("archive.zip");
        tokio::fs::write(&source, b"data").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;

        assert_eq!(docs.len(), 2);
        assert!(docs[1].source_path.exists());
        assert_ne!(docs[1].source_path, source);
        let name = docs[1]
            .source_path
            .file_name()
            .context("should have filename")?
            .to_string_lossy();
        assert_eq!(name, "file.txt");
        Ok(())
    }

    #[tokio::test]
    async fn extract_display_path_and_extracted_from_set() -> anyhow::Result<()> {
        let zip_bytes = make_zip_bytes(&[("attach.pdf", b"content")]).await?;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
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
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(zip_bytes)
                    .insert_header("Content-Type", "application/zip"),
            )
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("email.eml");
        tokio::fs::write(&source, b"raw").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;

        let meta = &docs[1].metadata;
        let MetadataValue::Single(display) = &meta[AUM_DISPLAY_PATH_KEY] else {
            panic!("expected Single for {AUM_DISPLAY_PATH_KEY}");
        };
        assert!(display.contains("email.eml") && display.contains("attach.pdf"));

        let MetadataValue::Single(extracted_from) = &meta[AUM_EXTRACTED_FROM_KEY] else {
            panic!("expected Single for {AUM_EXTRACTED_FROM_KEY}");
        };
        assert!(extracted_from.contains("email.eml"));
        Ok(())
    }

    /// With `/unpack` (direct children only), nested containers are handled
    /// by recursive unpack calls.  The outer email yields `attach.eml` as a
    /// direct child, and a second `/unpack` call on that file yields
    /// `document.pdf`.
    #[tokio::test]
    async fn extract_nested_container_recursive_unpack() -> anyhow::Result<()> {
        // Outer /unpack returns only the direct child.
        let outer_zip = make_zip_bytes(&[("attach.eml", b"inner email" as &[u8])]).await?;
        // Inner /unpack on attach.eml returns its child.
        let inner_zip = make_zip_bytes(&[("document.pdf", b"pdf content")]).await?;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"X-TIKA:content": "outer email"},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/attach.eml"},
                {
                    "X-TIKA:content": "pdf text",
                    "X-TIKA:embedded_resource_path": "/attach.eml/document.pdf",
                },
            ])))
            .mount(&server)
            .await;
        // First /unpack call (for outer email) returns attach.eml.
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(outer_zip)
                    .insert_header("Content-Type", "application/zip"),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Second /unpack call (for attach.eml) returns document.pdf.
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(inner_zip)
                    .insert_header("Content-Type", "application/zip"),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("email.eml");
        tokio::fs::write(&source, b"raw").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;

        // All three documents should be produced: container + 2 attachments.
        assert_eq!(docs.len(), 3, "expected container + 2 attachments");

        // Both attachments must have their own file on disk — neither should
        // fall back to the container's source_path.
        assert!(
            docs[1].source_path.exists(),
            "attach.eml must exist on disk"
        );
        assert_ne!(
            docs[1].source_path, source,
            "attach.eml source must not be the container"
        );
        assert!(
            docs[2].source_path.exists(),
            "document.pdf must exist on disk"
        );
        assert_ne!(
            docs[2].source_path, source,
            "document.pdf source must not be the container"
        );
        Ok(())
    }

    /// Reproduce real-world scenario: email with attachments named `embedded-1`
    /// and `embedded-2` (Tika default names for unnamed MIME parts), each
    /// containing a nested PDF.
    #[tokio::test]
    async fn extract_email_with_embedded_default_names() -> anyhow::Result<()> {
        // Outer /unpack returns two direct children.
        let outer_zip = make_zip_bytes(&[
            ("embedded-1", b"mime part 1" as &[u8]),
            ("embedded-2", b"mime part 2"),
        ])
        .await?;
        // Inner /unpack on embedded-1 returns invoice.pdf.
        let inner_zip_1 = make_zip_bytes(&[("invoice.pdf", b"pdf1")]).await?;
        // Inner /unpack on embedded-2 returns report.pdf.
        let inner_zip_2 = make_zip_bytes(&[("report.pdf", b"pdf2")]).await?;

        let server = MockServer::start().await;
        Mock::given(method("PUT"))
            .and(path("/rmeta/text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!([
                {"X-TIKA:content": "email body"},
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/embedded-1"},
                {
                    "X-TIKA:content": "invoice text",
                    "X-TIKA:embedded_resource_path": "/embedded-1/invoice.pdf",
                },
                {"X-TIKA:content": "", "X-TIKA:embedded_resource_path": "/embedded-2"},
                {
                    "X-TIKA:content": "report text",
                    "X-TIKA:embedded_resource_path": "/embedded-2/report.pdf",
                },
            ])))
            .mount(&server)
            .await;

        // First /unpack (outer email) returns embedded-1 and embedded-2.
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(outer_zip)
                    .insert_header("Content-Type", "application/zip"),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Second /unpack (embedded-1) returns invoice.pdf.
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(inner_zip_1)
                    .insert_header("Content-Type", "application/zip"),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;
        // Third /unpack (embedded-2) returns report.pdf.
        Mock::given(method("PUT"))
            .and(path("/unpack"))
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_bytes(inner_zip_2)
                    .insert_header("Content-Type", "application/zip"),
            )
            .up_to_n_times(1)
            .mount(&server)
            .await;

        let tmp = TempDir::new()?;
        let source = tmp.path().join("email.eml");
        tokio::fs::write(&source, b"raw email").await?;

        let extractor = make_extractor(&tmp, &server.uri())?;
        let docs = extractor
            .extract(&source, None)
            .try_collect::<Vec<_>>()
            .await?;

        // 5 documents: container + 2 intermediate + 2 PDFs.
        assert_eq!(
            docs.len(),
            5,
            "expected 1 container + 2 intermediate + 2 PDFs"
        );

        // All embedded documents must have their own file on disk.
        for (i, doc) in docs.iter().enumerate().skip(1) {
            assert!(
                doc.source_path.exists(),
                "doc[{i}] source_path must exist on disk"
            );
            assert_ne!(
                doc.source_path, source,
                "doc[{i}] source must not be the container"
            );
        }
        Ok(())
    }
}
