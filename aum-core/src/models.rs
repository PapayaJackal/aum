//! Core domain models for the aum document search server.

use std::collections::HashMap;
use std::path::PathBuf;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// Serde helper: ISO 8601 round-trip for DateTime<Utc> stored as SQLite TEXT
// ---------------------------------------------------------------------------

mod iso8601 {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(dt: &DateTime<Utc>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&dt.to_rfc3339())
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        DateTime::parse_from_rfc3339(&s)
            .map(|dt| dt.with_timezone(&Utc))
            .map_err(serde::de::Error::custom)
    }

    pub mod option {
        use chrono::{DateTime, Utc};
        use serde::{Deserialize, Deserializer, Serializer};

        #[allow(clippy::ref_option)]
        pub fn serialize<S>(dt: &Option<DateTime<Utc>>, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
        {
            match dt {
                Some(dt) => serializer.serialize_some(&dt.to_rfc3339()),
                None => serializer.serialize_none(),
            }
        }

        pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<DateTime<Utc>>, D::Error>
        where
            D: Deserializer<'de>,
        {
            let s: Option<String> = Option::deserialize(deserializer)?;
            match s {
                None => Ok(None),
                Some(s) => DateTime::parse_from_rfc3339(&s)
                    .map(|dt| Some(dt.with_timezone(&Utc)))
                    .map_err(serde::de::Error::custom),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Serde helper: PathBuf with normalized forward slashes
// ---------------------------------------------------------------------------
//
// The default serde PathBuf impl calls to_str(), which on Windows produces
// backslash-separated paths. Normalizing to forward slashes on both ends
// ensures consistent round-trips when paths cross OS boundaries or are
// stored in SQLite TEXT columns.

mod path_serde {
    use std::path::{Path, PathBuf};

    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(path: &Path, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = path.to_string_lossy();
        if s.contains('\\') {
            serializer.serialize_str(&s.replace('\\', "/"))
        } else {
            serializer.serialize_str(&s)
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<PathBuf, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        if s.contains('\\') {
            Ok(PathBuf::from(s.replace('\\', "/")))
        } else {
            Ok(PathBuf::from(s))
        }
    }
}

// ---------------------------------------------------------------------------
// Enums
// ---------------------------------------------------------------------------

/// The current lifecycle state of an ingest job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "TEXT", rename_all = "lowercase")]
pub enum JobStatus {
    /// Job is queued but has not started.
    #[default]
    Pending,
    /// Job is actively running.
    Running,
    /// Job finished successfully.
    Completed,
    /// Job terminated due to an error.
    Failed,
    /// Job was interrupted before completion.
    Interrupted,
}

impl std::str::FromStr for JobStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "pending" => Ok(Self::Pending),
            "running" => Ok(Self::Running),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "interrupted" => Ok(Self::Interrupted),
            other => Err(format!(
                "unknown status '{other}'; valid values: pending, running, completed, failed, interrupted"
            )),
        }
    }
}

/// The type of work performed by an ingest job.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default, sqlx::Type)]
#[serde(rename_all = "lowercase")]
#[sqlx(type_name = "TEXT", rename_all = "lowercase")]
pub enum JobType {
    /// Document extraction and indexing.
    #[default]
    Ingest,
    /// Embedding generation for existing documents.
    Embed,
}

// ---------------------------------------------------------------------------
// ErrorFilter
// ---------------------------------------------------------------------------

/// Filter for querying failed file paths by error type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorFilter<'a> {
    /// Return all failed paths regardless of error type.
    All,
    /// Return only paths with this specific error type.
    Only(&'a str),
    /// Return paths excluding this specific error type.
    Exclude(&'a str),
}

// ---------------------------------------------------------------------------
// EmbeddingModelInfo
// ---------------------------------------------------------------------------

/// Metadata about the embedding model used for a search index.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EmbeddingModelInfo {
    /// Embedding model name (e.g. `snowflake-arctic-embed2`).
    pub model: String,
    /// Backend provider (e.g. `ollama`, `openai`).
    pub backend: String,
    /// Vector dimension (e.g. 768, 1024).
    pub dimension: i64,
}

// ---------------------------------------------------------------------------
// MetadataValue
// ---------------------------------------------------------------------------

/// A metadata value that is either a single string or a list of strings.
///
/// Matches the Python type `str | list[str]` used in Tika metadata dictionaries.
/// Serializes as an untagged union: a plain JSON string or a JSON array of strings.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MetadataValue {
    /// A single string value.
    Single(String),
    /// A list of string values.
    List(Vec<String>),
}

// ---------------------------------------------------------------------------
// Document
// ---------------------------------------------------------------------------

/// A document extracted from the filesystem and ready for indexing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Document {
    /// Absolute path to the source file on disk.
    #[serde(with = "path_serde")]
    pub source_path: PathBuf,
    /// Extracted text content of the document.
    pub content: String,
    /// Key-value metadata from the extraction pipeline (e.g. Tika).
    /// Values may be a single string or a list of strings.
    pub metadata: HashMap<String, MetadataValue>,
}

// ---------------------------------------------------------------------------
// IngestError
// ---------------------------------------------------------------------------

/// An error that occurred while ingesting a single file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestError {
    /// Path to the file that caused the error.
    #[serde(with = "path_serde")]
    pub file_path: PathBuf,
    /// Short machine-readable error category (e.g. `"EmptyExtraction"`).
    pub error_type: String,
    /// Human-readable error message.
    pub message: String,
    /// UTC timestamp when the error was recorded.
    #[serde(with = "iso8601")]
    pub timestamp: DateTime<Utc>,
}

// ---------------------------------------------------------------------------
// JobProgress
// ---------------------------------------------------------------------------

/// Counters for tracking ingest/embed job progress.
///
/// Used by [`crate::db::repository::JobRepository::update_progress`] to
/// atomically overwrite all counters in a single call.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct JobProgress {
    /// Files successfully extracted so far.
    pub extracted: i64,
    /// Files successfully processed (embedded/indexed) so far.
    pub processed: i64,
    /// Files that failed processing.
    pub failed: i64,
    /// Files that produced empty content.
    pub empty: i64,
    /// Files skipped (already up to date).
    pub skipped: i64,
}

// ---------------------------------------------------------------------------
// IngestJob
// ---------------------------------------------------------------------------

/// An ingest job record as stored in the SQLite `jobs` table.
#[allow(clippy::doc_markdown)] // "SQLite" is a proper noun, not a code item
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestJob {
    /// Unique job identifier (UUID string).
    pub job_id: String,
    /// Directory being ingested.
    #[serde(with = "path_serde")]
    pub source_dir: PathBuf,
    /// Name of the search index being populated.
    #[serde(default = "IngestJob::default_index_name")]
    pub index_name: String,
    /// Type of work this job performs.
    ///
    /// Uses `Default` so that rows from older DB schemas (missing the column)
    /// deserialize gracefully as `JobType::Ingest`.
    #[serde(default)]
    pub job_type: JobType,
    /// Current lifecycle state.
    pub status: JobStatus,
    /// Total number of files discovered.
    #[serde(default)]
    pub total_files: i64,
    /// Files successfully extracted so far.
    #[serde(default)]
    pub extracted: i64,
    /// Files successfully processed (embedded/indexed) so far.
    #[serde(default)]
    pub processed: i64,
    /// Files that failed processing.
    #[serde(default)]
    pub failed: i64,
    /// Files that produced empty content.
    #[serde(default)]
    pub empty: i64,
    /// Files skipped (already up to date).
    #[serde(default)]
    pub skipped: i64,
    /// Per-file errors collected during this job.
    ///
    /// Populated explicitly when loading from the DB with `include_errors = true`;
    /// empty otherwise.
    #[serde(default)]
    pub errors: Vec<IngestError>,
    /// UTC timestamp when this job was created.
    #[serde(with = "iso8601")]
    pub created_at: DateTime<Utc>,
    /// UTC timestamp when this job finished, if it has finished.
    #[serde(with = "iso8601::option")]
    pub finished_at: Option<DateTime<Utc>>,
}

impl IngestJob {
    fn default_index_name() -> String {
        "aum".to_owned()
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use chrono::TimeZone as _;

    use super::*;

    #[test]
    fn job_status_serde_roundtrip() -> anyhow::Result<()> {
        let cases = [
            (JobStatus::Pending, "\"pending\""),
            (JobStatus::Running, "\"running\""),
            (JobStatus::Completed, "\"completed\""),
            (JobStatus::Failed, "\"failed\""),
            (JobStatus::Interrupted, "\"interrupted\""),
        ];
        for (status, expected_json) in cases {
            let json = serde_json::to_string(&status)?;
            assert_eq!(json, expected_json);
            let roundtripped: JobStatus = serde_json::from_str(&json)?;
            assert_eq!(roundtripped, status);
        }
        Ok(())
    }

    #[test]
    fn job_type_serde_roundtrip() -> anyhow::Result<()> {
        let cases = [
            (JobType::Ingest, "\"ingest\""),
            (JobType::Embed, "\"embed\""),
        ];
        for (job_type, expected_json) in cases {
            let json = serde_json::to_string(&job_type)?;
            assert_eq!(json, expected_json);
            let roundtripped: JobType = serde_json::from_str(&json)?;
            assert_eq!(roundtripped, job_type);
        }
        Ok(())
    }

    #[test]
    fn metadata_value_single_roundtrip() -> anyhow::Result<()> {
        let val = MetadataValue::Single("hello".to_owned());
        let json = serde_json::to_string(&val)?;
        assert_eq!(json, "\"hello\"");
        let roundtripped: MetadataValue = serde_json::from_str(&json)?;
        assert_eq!(roundtripped, val);
        Ok(())
    }

    #[test]
    fn metadata_value_list_roundtrip() -> anyhow::Result<()> {
        let val = MetadataValue::List(vec!["a".to_owned(), "b".to_owned()]);
        let json = serde_json::to_string(&val)?;
        assert_eq!(json, "[\"a\",\"b\"]");
        let roundtripped: MetadataValue = serde_json::from_str(&json)?;
        assert_eq!(roundtripped, val);
        Ok(())
    }

    #[test]
    fn iso8601_datetime_roundtrip() -> anyhow::Result<()> {
        #[derive(Serialize, Deserialize)]
        struct Wrapper {
            #[serde(with = "super::iso8601")]
            ts: DateTime<Utc>,
        }
        let dt = Utc
            .with_ymd_and_hms(2024, 6, 15, 12, 34, 56)
            .single()
            .ok_or_else(|| anyhow::anyhow!("invalid datetime"))?;
        let json = serde_json::to_string(&Wrapper { ts: dt })?;
        let roundtripped: Wrapper = serde_json::from_str(&json)?;
        assert_eq!(roundtripped.ts, dt);
        Ok(())
    }

    #[test]
    fn iso8601_option_some_and_none() -> anyhow::Result<()> {
        #[derive(Serialize, Deserialize)]
        struct Wrapper {
            #[serde(with = "super::iso8601::option")]
            ts: Option<DateTime<Utc>>,
        }
        let dt = Utc
            .with_ymd_and_hms(2024, 1, 1, 0, 0, 0)
            .single()
            .ok_or_else(|| anyhow::anyhow!("invalid datetime"))?;

        let some = Wrapper { ts: Some(dt) };
        let json = serde_json::to_string(&some)?;
        let roundtripped: Wrapper = serde_json::from_str(&json)?;
        assert_eq!(roundtripped.ts, Some(dt));

        let none = Wrapper { ts: None };
        let json = serde_json::to_string(&none)?;
        let roundtripped: Wrapper = serde_json::from_str(&json)?;
        assert_eq!(roundtripped.ts, None);
        Ok(())
    }

    #[test]
    fn path_serde_forward_slashes_on_serialize() -> anyhow::Result<()> {
        #[derive(Serialize, Deserialize)]
        struct Wrapper {
            #[serde(with = "super::path_serde")]
            p: PathBuf,
        }
        let w = Wrapper {
            p: PathBuf::from("C:\\Users\\foo\\bar.pdf"),
        };
        let json = serde_json::to_string(&w)?;
        assert_eq!(json, r#"{"p":"C:/Users/foo/bar.pdf"}"#);
        Ok(())
    }

    #[test]
    fn path_serde_backslashes_normalized_on_deserialize() -> anyhow::Result<()> {
        #[derive(Serialize, Deserialize)]
        struct Wrapper {
            #[serde(with = "super::path_serde")]
            p: PathBuf,
        }
        let w: Wrapper = serde_json::from_str(r#"{"p":"C:\\Users\\foo\\bar.pdf"}"#)?;
        assert_eq!(w.p, PathBuf::from("C:/Users/foo/bar.pdf"));
        Ok(())
    }

    #[test]
    fn path_serde_rejects_non_string() {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Wrapper {
            #[serde(with = "super::path_serde")]
            p: PathBuf,
        }
        let result: Result<Wrapper, _> = serde_json::from_str(r#"{"p": 42}"#);
        assert!(result.is_err());
    }

    #[test]
    fn path_serde_unix_paths_unchanged() -> anyhow::Result<()> {
        #[derive(Serialize, Deserialize)]
        struct Wrapper {
            #[serde(with = "super::path_serde")]
            p: PathBuf,
        }
        let w = Wrapper {
            p: PathBuf::from("/var/data/docs/file.pdf"),
        };
        let json = serde_json::to_string(&w)?;
        assert_eq!(json, r#"{"p":"/var/data/docs/file.pdf"}"#);
        let rt: Wrapper = serde_json::from_str(&json)?;
        assert_eq!(rt.p, w.p);
        Ok(())
    }

    #[test]
    fn iso8601_rejects_non_string() {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Wrapper {
            #[serde(with = "super::iso8601")]
            ts: DateTime<Utc>,
        }
        let result: Result<Wrapper, _> = serde_json::from_str(r#"{"ts": 42}"#);
        assert!(result.is_err());
    }

    #[test]
    fn iso8601_option_rejects_non_string_non_null() {
        #[derive(Deserialize)]
        #[allow(dead_code)]
        struct Wrapper {
            #[serde(with = "super::iso8601::option")]
            ts: Option<DateTime<Utc>>,
        }
        let result: Result<Wrapper, _> = serde_json::from_str(r#"{"ts": 42}"#);
        assert!(result.is_err());
    }

    #[test]
    fn ingest_job_missing_fields_use_defaults() -> anyhow::Result<()> {
        let json = r#"{
            "job_id": "abc",
            "source_dir": "/data",
            "status": "pending",
            "created_at": "2024-01-01T00:00:00+00:00",
            "finished_at": null
        }"#;
        let job: IngestJob = serde_json::from_str(json)?;
        assert_eq!(job.job_type, JobType::Ingest);
        assert_eq!(job.index_name, "aum");
        assert_eq!(job.total_files, 0);
        assert!(job.errors.is_empty());
        assert!(job.finished_at.is_none());
        Ok(())
    }
}
