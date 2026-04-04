use aum_macros::{ConfigDefault, ConfigDocs, ConfigValues};
use figment::{
    Figment,
    providers::{Env, Format, Serialized, Toml},
};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::path::PathBuf;

// ---------------------------------------------------------------------------
// Config documentation
// ---------------------------------------------------------------------------

/// Documentation entry for a single configuration option.
#[derive(Debug, Clone, Copy)]
pub struct ConfigDoc {
    /// Field name within its section struct.
    pub name: &'static str,
    /// Corresponding environment variable (AUM_{SECTION}__{FIELD}, uppercase).
    pub env_var: &'static str,
    /// String representation of the default value.
    pub default: &'static str,
    /// Human-readable description of what this option controls.
    pub description: &'static str,
    /// Section name (e.g. "meilisearch"), or "" for top-level fields.
    pub section: &'static str,
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// Backend used to generate text embeddings for semantic search.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum EmbeddingsBackend {
    /// Use a locally-running Ollama instance.
    #[default]
    Ollama,
    /// Use an OpenAI-compatible embeddings API.
    #[serde(rename = "openai")]
    OpenAi,
}

impl fmt::Display for EmbeddingsBackend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EmbeddingsBackend::Ollama => write!(f, "ollama"),
            EmbeddingsBackend::OpenAi => write!(f, "openai"),
        }
    }
}

/// The search backend to use for indexing and querying documents.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum SearchBackendType {
    /// Use Meilisearch as the search backend.
    #[default]
    Meilisearch,
    /// Use Elasticsearch as the search backend.
    Elasticsearch,
}

impl fmt::Display for SearchBackendType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SearchBackendType::Meilisearch => write!(f, "meilisearch"),
            SearchBackendType::Elasticsearch => write!(f, "elasticsearch"),
        }
    }
}

impl std::str::FromStr for SearchBackendType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "meilisearch" => Ok(Self::Meilisearch),
            "elasticsearch" => Ok(Self::Elasticsearch),
            other => Err(format!(
                "unknown backend '{other}'; valid values: meilisearch, elasticsearch"
            )),
        }
    }
}

/// Minimum severity level for emitted log messages.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
pub enum LogLevel {
    /// Verbose debug output.
    #[serde(rename = "DEBUG")]
    Debug,
    /// Informational messages (default).
    #[default]
    #[serde(rename = "INFO")]
    Info,
    /// Warnings about unexpected but recoverable conditions.
    #[serde(rename = "WARNING")]
    Warn,
    /// Errors that require attention.
    #[serde(rename = "ERROR")]
    Error,
}

impl fmt::Display for LogLevel {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogLevel::Debug => write!(f, "DEBUG"),
            LogLevel::Info => write!(f, "INFO"),
            LogLevel::Warn => write!(f, "WARNING"),
            LogLevel::Error => write!(f, "ERROR"),
        }
    }
}

/// Output format for log messages.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    /// Human-readable text output (default).
    #[default]
    Console,
    /// Structured JSON output, suitable for log aggregation pipelines.
    Json,
}

impl fmt::Display for LogFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LogFormat::Console => write!(f, "console"),
            LogFormat::Json => write!(f, "json"),
        }
    }
}

// ---------------------------------------------------------------------------
// Instance sub-structs (used inside section configs)
// ---------------------------------------------------------------------------

/// A single Apache Tika server instance used for document text extraction.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TikaInstance {
    /// Base URL of the Tika server (e.g. `http://tika:9998`).
    pub url: String,
    /// Maximum number of concurrent requests to this instance. Must be at least 1.
    #[serde(default = "TikaInstance::default_concurrency")]
    pub concurrency: u32,
}

impl TikaInstance {
    const fn default_concurrency() -> u32 {
        1
    }
}

impl Default for TikaInstance {
    fn default() -> Self {
        Self {
            url: String::new(),
            concurrency: Self::default_concurrency(),
        }
    }
}

impl fmt::Display for TikaInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{url = \"{}\", concurrency = {}}}",
            self.url, self.concurrency
        )
    }
}

/// A single embedder server instance used for generating text embeddings.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmbedderInstance {
    /// Base URL of the embedder server.
    pub url: String,
    /// Maximum number of concurrent requests to this instance.
    #[serde(default = "EmbedderInstance::default_concurrency")]
    pub concurrency: u32,
}

impl EmbedderInstance {
    const fn default_concurrency() -> u32 {
        1
    }
}

impl Default for EmbedderInstance {
    fn default() -> Self {
        Self {
            url: String::new(),
            concurrency: Self::default_concurrency(),
        }
    }
}

impl fmt::Display for EmbedderInstance {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{{url = \"{}\", concurrency = {}}}",
            self.url, self.concurrency
        )
    }
}

// ---------------------------------------------------------------------------
// Section config structs
// ---------------------------------------------------------------------------

/// Configuration for the Meilisearch search backend.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "meilisearch"]
pub struct MeilisearchConfig {
    /// URL of the Meilisearch instance.
    #[config_default = "http://localhost:7700"]
    pub url: String,
    /// Meilisearch master API key.
    #[config_default = ""]
    pub api_key: String,
    /// Ratio of semantic to keyword score in hybrid search (0.0–1.0).
    #[config_default = "0.5"]
    pub semantic_ratio: f32,
    /// Number of words to include in highlighted excerpt snippets.
    #[config_default = "50"]
    pub crop_length: u32,
}

/// Configuration for the Elasticsearch search backend.
#[cfg(feature = "elasticsearch")]
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "elasticsearch"]
pub struct ElasticsearchConfig {
    /// URL of the Elasticsearch instance.
    #[config_default = "http://localhost:9200"]
    pub url: String,
    /// Use Reciprocal Rank Fusion (RRF) for hybrid search instead of combined scoring.
    #[config_default = "false"]
    pub rrf: bool,
    /// Maximum analyzed offset for highlighting, in bytes.
    #[config_default = "10000000"]
    pub max_highlight_offset: u64,
}

/// Configuration for Apache Tika document extraction.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "tika"]
pub struct TikaConfig {
    /// List of Tika server instances for parallel extraction. If empty, falls back to `server_url`.
    #[config_default = "[]"]
    pub instances: Vec<TikaInstance>,
    /// URL of the fallback Tika server (used when instances is empty).
    #[config_default = "http://localhost:9998"]
    pub server_url: String,
    /// Per-request timeout for Tika extraction, in seconds.
    #[config_default = "300"]
    pub request_timeout: u32,
    /// Enable OCR for image-based documents (requires Tika to be built with Tesseract).
    #[config_default = "false"]
    pub ocr_enabled: bool,
    /// Tesseract language code(s) to use for OCR (e.g. "eng", "eng+fra").
    #[config_default = "eng"]
    pub ocr_language: String,
}

/// Configuration for text embedding generation used in semantic search.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "embeddings"]
pub struct EmbeddingsConfig {
    /// Enable semantic vector embeddings for hybrid search.
    #[config_default = "false"]
    pub enabled: bool,
    /// Embeddings backend to use: "ollama" or "openai".
    #[config_default = "ollama"]
    pub backend: EmbeddingsBackend,
    /// Name of the embedding model to use.
    #[config_default = "snowflake-arctic-embed2"]
    pub model: String,
    /// Dimension of the embedding vectors produced by the model.
    #[config_default = "1024"]
    pub dimension: u32,
    /// Number of text chunks to embed in a single batch request.
    #[config_default = "8"]
    pub batch_size: u32,
    /// Maximum token context length supported by the embedding model.
    #[config_default = "8192"]
    pub context_length: u32,
    /// Number of tokens to overlap between consecutive chunks.
    #[config_default = "200"]
    pub chunk_overlap: u32,
    /// Prefix prepended to query strings before embedding (model-specific).
    #[config_default = "query: "]
    pub query_prefix: String,
    /// List of embedder instances for parallel embedding. If empty, falls back to `ollama_url` or `api_url`.
    #[config_default = "[]"]
    pub instances: Vec<EmbedderInstance>,
    /// Base URL of the Ollama server (used when backend = "ollama").
    #[config_default = "http://localhost:11434"]
    pub ollama_url: String,
    /// Base URL of the OpenAI-compatible embeddings API (used when backend = "openai").
    #[config_default = ""]
    pub api_url: String,
    /// API key for the OpenAI-compatible embeddings API.
    #[config_default = ""]
    pub api_key: String,
}

/// Configuration for local data storage.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "data"]
pub struct DataConfig {
    /// Directory where aum stores its database and extracted files.
    #[config_default = "data"]
    pub dir: PathBuf,
}

/// Configuration for the HTTP API server.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "server"]
pub struct ServerConfig {
    /// Public base URL of the aum server (used in generated links).
    #[config_default = "http://localhost:8000"]
    pub base_url: String,
    /// Host address to bind the API server to.
    #[config_default = "0.0.0.0"]
    pub host: String,
    /// Port to bind the API server to.
    #[config_default = "8000"]
    pub port: u16,
    /// Enable the OpenAPI documentation UI at `/docs`.
    #[allow(clippy::doc_markdown)]
    #[config_default = "false"]
    pub enable_docs: bool,
    /// List of allowed CORS origins (e.g. `["https://app.example.com"]`).
    #[config_default = "[]"]
    pub cors_origins: Vec<String>,
}

/// Configuration for the Prometheus metrics endpoint.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "prometheus"]
pub struct PrometheusConfig {
    /// Enable the Prometheus metrics endpoint.
    #[config_default = "false"]
    pub enabled: bool,
    /// Port to expose Prometheus metrics on.
    #[config_default = "9090"]
    pub port: u16,
}

/// Configuration for authentication and access control.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "auth"]
pub struct AuthConfig {
    /// Disable authentication and allow all requests without credentials.
    #[config_default = "false"]
    pub public_mode: bool,
}

/// Configuration for log output.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "log"]
pub struct LoggingConfig {
    /// Minimum log level to emit: "DEBUG", "INFO", "WARNING", or "ERROR".
    #[config_default = "INFO"]
    pub level: LogLevel,
    /// Log output format: "console" (human-readable) or "json" (structured).
    #[config_default = "console"]
    pub format: LogFormat,
}

/// Configuration for the database connection.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "database"]
pub struct DatabaseConfig {
    #[allow(clippy::doc_markdown)] // SQLite is a proper noun
    /// Database connection URL. Supports `sqlite:`, `postgres://`, and `mysql://` schemes.
    /// If empty, defaults to a SQLite database (`aum.db`) in the data directory.
    #[config_default = ""]
    pub url: String,
    /// Maximum number of connections in the database pool.
    #[config_default = "16"]
    pub max_connections: u32,
}

/// Configuration for the document ingest pipeline.
#[derive(Debug, Clone, Serialize, Deserialize, ConfigDocs, ConfigDefault, ConfigValues)]
#[serde(default)]
#[config_section = "ingest"]
pub struct IngestConfig {
    /// Number of documents to process per database transaction during ingest.
    #[config_default = "1000"]
    pub batch_size: u32,
    /// Maximum number of concurrent ingest worker tasks.
    #[config_default = "<number of logical CPUs>"]
    #[config_default_expr = "::std::thread::available_parallelism().map(|n| n.get() as u32).unwrap_or(4)"]
    pub max_workers: u32,
    /// Maximum depth to recurse into nested archives during extraction.
    #[config_default = "5"]
    pub max_extract_depth: u32,
    /// Maximum character count of document text content to store and index; 0 means unlimited.
    #[config_default = "100000"]
    pub max_content_length: u64,
}

// ---------------------------------------------------------------------------
// Main config struct
// ---------------------------------------------------------------------------

/// Top-level configuration for the aum server.
///
/// Loaded from (lowest to highest priority): compiled-in defaults → `aum.toml` → `AUM_*` environment variables.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct AumConfig {
    /// Local data storage settings.
    pub data: DataConfig,
    /// Log output settings.
    pub log: LoggingConfig,
    /// Prometheus metrics endpoint settings.
    pub prometheus: PrometheusConfig,
    /// Which search backend to use: "meilisearch" or "elasticsearch".
    pub search_backend: SearchBackendType,
    /// Meilisearch connection settings.
    #[cfg(feature = "meilisearch")]
    pub meilisearch: MeilisearchConfig,
    /// Elasticsearch connection settings.
    #[cfg(feature = "elasticsearch")]
    pub elasticsearch: ElasticsearchConfig,
    /// Database connection settings.
    pub database: DatabaseConfig,
    /// Document ingest pipeline settings.
    pub ingest: IngestConfig,
    /// Apache Tika extraction settings.
    pub tika: TikaConfig,
    /// Text embedding settings for semantic search.
    pub embeddings: EmbeddingsConfig,
    /// HTTP API server settings.
    pub server: ServerConfig,
    /// Authentication and access control settings.
    pub auth: AuthConfig,
}

// ---------------------------------------------------------------------------
// Derived property methods
// ---------------------------------------------------------------------------

impl AumConfig {
    /// Returns an iterator over all config documentation entries across all sections, in order.
    pub fn config_docs() -> impl Iterator<Item = &'static ConfigDoc> {
        let mut sections: Vec<&[ConfigDoc]> = vec![
            DataConfig::config_docs(),
            DatabaseConfig::config_docs(),
            LoggingConfig::config_docs(),
            PrometheusConfig::config_docs(),
        ];
        #[cfg(feature = "meilisearch")]
        sections.push(MeilisearchConfig::config_docs());
        #[cfg(feature = "elasticsearch")]
        sections.push(ElasticsearchConfig::config_docs());
        sections.extend([
            IngestConfig::config_docs(),
            TikaConfig::config_docs(),
            EmbeddingsConfig::config_docs(),
            ServerConfig::config_docs(),
            AuthConfig::config_docs(),
        ]);
        sections.into_iter().flat_map(|s| s.iter())
    }

    /// Returns the database connection URL to use.
    ///
    /// If `database.url` is empty, returns a `sqlite:` URL pointing to `aum.db`
    /// in the configured data directory.
    #[must_use]
    pub fn database_url(&self) -> String {
        if self.database.url.is_empty() {
            format!("sqlite:{}", self.data.dir.join("aum.db").display())
        } else {
            self.database.url.clone()
        }
    }

    /// Returns the path to the directory where extracted document content is stored.
    #[must_use]
    pub fn extract_dir(&self) -> PathBuf {
        self.data.dir.join("extracted")
    }

    /// Returns the path to the directory where ingest lock files are stored.
    #[must_use]
    pub fn lock_dir(&self) -> PathBuf {
        self.data.dir.join("locks")
    }

    /// Returns the effective Tika instances to use.
    ///
    /// If `tika.instances` is empty, falls back to a single instance using
    /// `tika.server_url` with concurrency set to `ingest.max_workers` so that
    /// one slow extraction cannot starve the entire worker pool.
    #[must_use]
    pub fn effective_tika_instances(&self) -> Vec<TikaInstance> {
        if self.tika.instances.is_empty() {
            vec![TikaInstance {
                url: self.tika.server_url.clone(),
                concurrency: self.ingest.max_workers,
            }]
        } else {
            self.tika.instances.clone()
        }
    }

    /// Returns the effective embedder instances to use.
    ///
    /// If `embeddings.instances` is empty, falls back to a single instance
    /// using `embeddings.ollama_url` (Ollama) or `embeddings.api_url` (OpenAI).
    #[allow(clippy::doc_markdown)]
    #[must_use]
    pub fn effective_embedder_instances(&self) -> Vec<EmbedderInstance> {
        if !self.embeddings.instances.is_empty() {
            return self.embeddings.instances.clone();
        }
        let url = match self.embeddings.backend {
            EmbeddingsBackend::Ollama => self.embeddings.ollama_url.clone(),
            EmbeddingsBackend::OpenAi => self.embeddings.api_url.clone(),
        };
        vec![EmbedderInstance {
            url,
            ..Default::default()
        }]
    }
}

// ---------------------------------------------------------------------------
// Config loading
// ---------------------------------------------------------------------------

/// Loads configuration from `aum.toml` (if present) and `AUM_*` environment variables.
///
/// # Errors
/// Returns a `figment::Error` if any env var or TOML value cannot be deserialized into `AumConfig`.
#[allow(clippy::result_large_err)]
pub fn load_config() -> Result<AumConfig, figment::Error> {
    load_config_from("aum.toml")
}

/// Loads configuration from the given TOML file path and `AUM_*` environment variables.
///
/// If the file does not exist it is silently ignored and defaults are used.
///
/// # Errors
/// Returns a `figment::Error` if any env var or TOML value cannot be deserialized into `AumConfig`.
#[allow(clippy::result_large_err)]
pub fn load_config_from(toml_path: &str) -> Result<AumConfig, figment::Error> {
    Figment::new()
        .merge(Serialized::defaults(AumConfig::default()))
        .merge(Toml::file(toml_path))
        .merge(Env::prefixed("AUM_").split("__"))
        .extract()
}

// ---------------------------------------------------------------------------
// Config display
// ---------------------------------------------------------------------------

/// Formats the effective configuration as documented environment variable assignments.
///
/// Each field is shown as `ENV_VAR=current_value` preceded by its description.
#[must_use]
pub fn format_config(config: &AumConfig) -> String {
    fn write_section(out: &mut String, docs: &[ConfigDoc], values: Vec<String>) {
        use std::fmt::Write as _;
        for (doc, val) in docs.iter().zip(values) {
            let _ = write!(out, "# {}\n{}={}\n", doc.description, doc.env_var, val);
        }
    }

    let mut out = String::new();
    write_section(
        &mut out,
        DataConfig::config_docs(),
        config.data.config_values(),
    );
    write_section(
        &mut out,
        DatabaseConfig::config_docs(),
        config.database.config_values(),
    );
    write_section(
        &mut out,
        LoggingConfig::config_docs(),
        config.log.config_values(),
    );
    write_section(
        &mut out,
        PrometheusConfig::config_docs(),
        config.prometheus.config_values(),
    );
    #[cfg(feature = "meilisearch")]
    write_section(
        &mut out,
        MeilisearchConfig::config_docs(),
        config.meilisearch.config_values(),
    );
    #[cfg(feature = "elasticsearch")]
    write_section(
        &mut out,
        ElasticsearchConfig::config_docs(),
        config.elasticsearch.config_values(),
    );
    write_section(
        &mut out,
        IngestConfig::config_docs(),
        config.ingest.config_values(),
    );
    write_section(
        &mut out,
        TikaConfig::config_docs(),
        config.tika.config_values(),
    );
    write_section(
        &mut out,
        EmbeddingsConfig::config_docs(),
        config.embeddings.config_values(),
    );
    write_section(
        &mut out,
        ServerConfig::config_docs(),
        config.server.config_values(),
    );
    write_section(
        &mut out,
        AuthConfig::config_docs(),
        config.auth.config_values(),
    );
    out
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use figment::{
        Figment,
        providers::{Format, Serialized, Toml as FigmentToml},
    };
    use std::sync::Mutex;

    static ENV_MUTEX: Mutex<()> = Mutex::new(());

    fn config_from_env(pairs: &[(&str, &str)]) -> anyhow::Result<AumConfig> {
        let _guard = ENV_MUTEX.lock().map_err(|e| anyhow::anyhow!("{e}"))?;
        for (k, v) in pairs {
            unsafe { std::env::set_var(k, v) };
        }
        let result = Figment::new()
            .merge(Serialized::defaults(AumConfig::default()))
            .merge(Env::prefixed("AUM_").split("__"))
            .extract()?;
        for (k, _) in pairs {
            unsafe { std::env::remove_var(k) };
        }
        Ok(result)
    }

    // --- Figment pipeline sanity ---

    /// A completely empty config source (no TOML, no env) must successfully
    /// produce an `AumConfig` and preserve every default value from the Default
    /// impl — this is what consumers actually rely on at startup.
    #[test]
    fn test_empty_source_roundtrips_defaults() -> anyhow::Result<()> {
        let expected = AumConfig::default();
        let actual: AumConfig = Figment::new()
            .merge(Serialized::defaults(AumConfig::default()))
            .extract()?;

        #[cfg(feature = "meilisearch")]
        {
            assert_eq!(actual.meilisearch.url, expected.meilisearch.url);
            assert!(
                (actual.meilisearch.semantic_ratio - expected.meilisearch.semantic_ratio).abs()
                    < f32::EPSILON
            );
            assert_eq!(
                actual.meilisearch.crop_length,
                expected.meilisearch.crop_length
            );
        }
        assert_eq!(actual.tika.server_url, expected.tika.server_url);
        assert_eq!(actual.tika.request_timeout, expected.tika.request_timeout);
        assert_eq!(actual.embeddings.model, expected.embeddings.model);
        assert_eq!(actual.embeddings.dimension, expected.embeddings.dimension);
        assert_eq!(
            actual.embeddings.query_prefix,
            expected.embeddings.query_prefix
        );
        assert_eq!(actual.embeddings.ollama_url, expected.embeddings.ollama_url);
        assert_eq!(actual.server.port, expected.server.port);
        assert_eq!(actual.prometheus.port, expected.prometheus.port);
        assert_eq!(actual.ingest.batch_size, expected.ingest.batch_size);
        assert_eq!(
            actual.ingest.max_content_length,
            expected.ingest.max_content_length
        );
        assert_eq!(actual.log.level, LogLevel::Info);
        assert_eq!(actual.log.format, LogFormat::Console);
        Ok(())
    }

    #[test]
    fn test_ingest_max_workers_is_positive() {
        assert!(AumConfig::default().ingest.max_workers >= 1);
    }

    // --- Env var overlay ---

    #[test]
    #[cfg(feature = "meilisearch")]
    fn test_env_overlay_string() -> anyhow::Result<()> {
        let cfg = config_from_env(&[("AUM_MEILISEARCH__URL", "http://env-meili:7700")])?;
        assert_eq!(cfg.meilisearch.url, "http://env-meili:7700");
        assert!((cfg.meilisearch.semantic_ratio - 0.5_f32).abs() < f32::EPSILON); // untouched default
        Ok(())
    }

    #[test]
    fn test_env_overlay_bool() -> anyhow::Result<()> {
        let cfg = config_from_env(&[("AUM_TIKA__OCR_ENABLED", "true")])?;
        assert!(cfg.tika.ocr_enabled);
        assert!(!cfg.embeddings.enabled); // untouched default
        Ok(())
    }

    #[test]
    fn test_env_overlay_integer() -> anyhow::Result<()> {
        let cfg = config_from_env(&[("AUM_SERVER__PORT", "9001")])?;
        assert_eq!(cfg.server.port, 9001);
        assert_eq!(cfg.prometheus.port, 9090); // untouched default
        Ok(())
    }

    #[test]
    fn test_env_overlay_has_lower_priority_than_toml() -> anyhow::Result<()> {
        let _guard = ENV_MUTEX.lock().map_err(|e| anyhow::anyhow!("{e}"))?;
        unsafe { std::env::set_var("AUM_SERVER__PORT", "1234") };
        let result: AumConfig = Figment::new()
            .merge(Serialized::defaults(AumConfig::default()))
            .merge(FigmentToml::string("[server]\nport = 9999"))
            .merge(Env::prefixed("AUM_").split("__"))
            .extract()?;
        unsafe { std::env::remove_var("AUM_SERVER__PORT") };
        assert_eq!(result.server.port, 1234);
        Ok(())
    }

    // --- Missing TOML file ---

    #[test]
    fn test_missing_toml_file_does_not_error() -> anyhow::Result<()> {
        let cfg: AumConfig = Figment::new()
            .merge(Serialized::defaults(AumConfig::default()))
            .merge(FigmentToml::file("__nonexistent_aum_config__.toml"))
            .extract()?;
        #[cfg(feature = "meilisearch")]
        assert_eq!(cfg.meilisearch.url, "http://localhost:7700");
        assert_eq!(cfg.server.port, 8000);
        assert_eq!(cfg.data.dir, PathBuf::from("data"));
        Ok(())
    }

    // --- Derived methods ---

    #[test]
    fn test_database_url_default_derives_from_data_dir() {
        let mut cfg = AumConfig::default();
        assert_eq!(cfg.database_url(), "sqlite:data/aum.db");
        assert_eq!(cfg.extract_dir(), PathBuf::from("data/extracted"));

        cfg.data.dir = "/opt/aum".into();
        assert_eq!(cfg.database_url(), "sqlite:/opt/aum/aum.db");
        assert_eq!(cfg.extract_dir(), PathBuf::from("/opt/aum/extracted"));
    }

    #[test]
    fn test_database_url_explicit_overrides_data_dir() {
        let mut cfg = AumConfig::default();
        cfg.database.url = "postgres://user:pass@db:5432/aum".into();
        assert_eq!(cfg.database_url(), "postgres://user:pass@db:5432/aum");
    }

    // --- effective_tika_instances ---

    #[test]
    fn test_effective_tika_fallback_uses_server_url() {
        let mut cfg = AumConfig::default();
        cfg.tika.server_url = "http://custom-tika:9998".into();
        let instances = cfg.effective_tika_instances();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].url, "http://custom-tika:9998");
        assert_eq!(instances[0].concurrency, cfg.ingest.max_workers);
    }

    #[test]
    fn test_effective_tika_explicit_instances_returned_unchanged() {
        let mut cfg = AumConfig::default();
        cfg.tika.instances = vec![
            TikaInstance {
                url: "http://t1:9998".into(),
                concurrency: 2,
            },
            TikaInstance {
                url: "http://t2:9998".into(),
                concurrency: 4,
            },
        ];
        let instances = cfg.effective_tika_instances();
        assert_eq!(instances[0].concurrency, 2);
        assert_eq!(instances[1].concurrency, 4);
    }

    // --- effective_embedder_instances ---

    #[test]
    fn test_effective_embedder_fallback_ollama() {
        let mut cfg = AumConfig::default();
        cfg.embeddings.ollama_url = "http://custom-ollama:11434".into();
        let instances = cfg.effective_embedder_instances();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].url, "http://custom-ollama:11434");
        assert_eq!(instances[0].concurrency, 1);
    }

    #[test]
    fn test_effective_embedder_fallback_openai() {
        let mut cfg = AumConfig::default();
        cfg.embeddings.backend = EmbeddingsBackend::OpenAi;
        cfg.embeddings.api_url = "https://api.openai.com/v1".into();
        let instances = cfg.effective_embedder_instances();
        assert_eq!(instances.len(), 1);
        assert_eq!(instances[0].url, "https://api.openai.com/v1");
    }

    #[test]
    fn test_effective_embedder_explicit_overrides_fallback() {
        let mut cfg = AumConfig::default();
        cfg.embeddings.instances = vec![
            EmbedderInstance {
                url: "http://e1:11434".into(),
                concurrency: 2,
            },
            EmbedderInstance {
                url: "http://e2:11434".into(),
                concurrency: 3,
            },
        ];
        let instances = cfg.effective_embedder_instances();
        assert_eq!(instances.len(), 2);
        assert_eq!(instances[0].url, "http://e1:11434");
        assert_eq!(instances[1].concurrency, 3);
    }

    // --- Display impls ---

    #[test]
    fn test_display_impls() {
        assert_eq!(EmbeddingsBackend::Ollama.to_string(), "ollama");
        assert_eq!(EmbeddingsBackend::OpenAi.to_string(), "openai");
        assert_eq!(LogLevel::Debug.to_string(), "DEBUG");
        assert_eq!(LogLevel::Info.to_string(), "INFO");
        assert_eq!(LogLevel::Warn.to_string(), "WARNING");
        assert_eq!(LogLevel::Error.to_string(), "ERROR");
        assert_eq!(LogFormat::Console.to_string(), "console");
        assert_eq!(LogFormat::Json.to_string(), "json");
        assert_eq!(
            TikaInstance {
                url: "http://tika:9998".into(),
                concurrency: 2
            }
            .to_string(),
            r#"{url = "http://tika:9998", concurrency = 2}"#
        );
        assert_eq!(
            EmbedderInstance {
                url: "http://ollama:11434".into(),
                concurrency: 1
            }
            .to_string(),
            r#"{url = "http://ollama:11434", concurrency = 1}"#
        );
    }

    // --- load_config / load_config_from ---

    #[test]
    fn test_load_config_from_missing_file_returns_defaults() -> anyhow::Result<()> {
        let _guard = ENV_MUTEX.lock().map_err(|e| anyhow::anyhow!("{e}"))?;
        let cfg = load_config_from("__nonexistent__.toml")?;
        #[cfg(feature = "meilisearch")]
        assert_eq!(cfg.meilisearch.url, "http://localhost:7700");
        assert_eq!(cfg.server.port, 8000);
        Ok(())
    }

    #[test]
    fn test_load_config_succeeds_without_aum_toml() -> anyhow::Result<()> {
        let _guard = ENV_MUTEX.lock().map_err(|e| anyhow::anyhow!("{e}"))?;
        let cfg = load_config()?;
        assert_eq!(cfg.data.dir, PathBuf::from("data"));
        Ok(())
    }

    // --- format_config ---

    #[test]
    fn test_format_config_contains_env_vars() {
        let cfg = AumConfig::default();
        let out = format_config(&cfg);
        #[cfg(feature = "meilisearch")]
        assert!(out.contains("AUM_MEILISEARCH__URL=http://localhost:7700"));
        assert!(out.contains("AUM_SERVER__PORT=8000"));
        assert!(out.contains("AUM_DATA__DIR=data"));
        assert!(out.contains("AUM_PROMETHEUS__ENABLED=false"));
        assert!(out.contains("AUM_DATABASE__URL="));
        assert!(out.contains("AUM_DATABASE__MAX_CONNECTIONS=16"));
    }

    #[test]
    fn test_format_config_contains_descriptions() {
        let out = format_config(&AumConfig::default());
        // Every env var line should be preceded by a comment line
        for line in out.lines() {
            if line.starts_with("AUM_") {
                let var = line.split('=').next().unwrap_or("");
                assert!(
                    out.contains("\n# "),
                    "no description comment found before {var}"
                );
            }
        }
        assert!(out.contains("# "));
    }

    // --- Generated config docs ---

    #[test]
    fn test_config_docs_all_env_vars_have_aum_prefix() {
        for doc in AumConfig::config_docs() {
            assert!(
                doc.env_var.starts_with("AUM_"),
                "{} env_var does not start with AUM_",
                doc.name
            );
        }
    }

    #[test]
    fn test_config_docs_no_empty_descriptions() {
        for doc in AumConfig::config_docs() {
            assert!(
                !doc.description.is_empty(),
                "{} has empty description",
                doc.name
            );
        }
    }

    #[test]
    fn test_config_docs_section_names_match_toml_keys() {
        let mut expected_sections: Vec<&str> = vec![
            "data",
            "database",
            "log",
            "prometheus",
            "ingest",
            "tika",
            "embeddings",
            "server",
            "auth",
        ];
        #[cfg(feature = "meilisearch")]
        expected_sections.push("meilisearch");
        #[cfg(feature = "elasticsearch")]
        expected_sections.push("elasticsearch");
        for doc in AumConfig::config_docs() {
            assert!(
                expected_sections.contains(&doc.section),
                "unexpected section '{}' for field '{}'",
                doc.section,
                doc.name
            );
        }
    }
}
