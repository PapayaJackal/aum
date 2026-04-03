//! Metadata extraction and document body construction for Meilisearch.

use std::collections::HashMap;

use serde_json::{Map, Value};

use crate::extraction::{AUM_DISPLAY_PATH_KEY, AUM_EXTRACTED_FROM_KEY};
use crate::models::{Document, MetadataValue};
use crate::search::utils::{extract_email, normalize_message_id};

// ---------------------------------------------------------------------------
// Metadata source key mappings (Meilisearch-specific)
// ---------------------------------------------------------------------------

/// Maps Meilisearch indexed field names to lists of candidate Tika metadata
/// keys, tried in priority order (first match wins).
static META_SOURCE_KEYS: &[(&str, &[&str])] = &[
    ("content_type", &["Content-Type"]),
    (
        "creator",
        &[
            "dc:creator",
            "xmp:dc:creator",
            "Author",
            "meta:author",
            "creator",
        ],
    ),
    (
        "created",
        &[
            "dcterms:created",
            "Creation-Date",
            "meta:creation-date",
            "created",
            "date",
        ],
    ),
    (
        "modified",
        &[
            "dcterms:modified",
            "Last-Modified",
            "meta:save-date",
            "modified",
        ],
    ),
    ("file_size", &["Content-Length"]),
    (
        "message_id",
        &["Message:Raw-Header:Message-ID", "Message-ID"],
    ),
    (
        "in_reply_to",
        &["Message:Raw-Header:In-Reply-To", "In-Reply-To"],
    ),
    (
        "references",
        &["Message:Raw-Header:References", "References"],
    ),
];

/// Email header keys whose values are collected into `email_addresses`.
static EMAIL_HEADER_KEYS: &[&str] = &["Message-From", "Message-To", "Message-CC"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Retrieve the first matching value for a list of candidate keys.
fn first_match<'a>(
    metadata: &'a HashMap<String, MetadataValue>,
    keys: &[&str],
) -> Option<&'a MetadataValue> {
    keys.iter().find_map(|k| metadata.get(*k))
}

/// Convert a `MetadataValue` to a single string (takes first element of lists).
fn as_single_string(val: &MetadataValue) -> Option<String> {
    match val {
        MetadataValue::Single(s) => Some(s.clone()),
        MetadataValue::List(v) => v.first().cloned(),
    }
}

/// Convert a `MetadataValue` to a list of strings.
fn as_string_list(val: &MetadataValue) -> Vec<String> {
    match val {
        MetadataValue::Single(s) => vec![s.clone()],
        MetadataValue::List(v) => v.clone(),
    }
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Curated intermediate metadata extracted from raw Tika metadata.
#[derive(Debug, Default)]
pub(super) struct IndexedMeta {
    pub content_type: Option<String>,
    pub creator: Option<String>,
    pub created: Option<String>,
    pub created_year: Option<i64>,
    pub modified: Option<String>,
    pub file_size: Option<String>,
    pub message_id: Option<String>,
    pub in_reply_to: Option<String>,
    pub references: Vec<String>,
    pub email_addresses: Vec<String>,
}

/// Extract curated metadata fields from raw Tika metadata.
pub(super) fn extract_indexed_meta(metadata: &HashMap<String, MetadataValue>) -> IndexedMeta {
    let mut out = IndexedMeta::default();

    // Resolve each field from candidate keys in priority order.
    for (field, keys) in META_SOURCE_KEYS {
        let Some(val) = first_match(metadata, keys) else {
            continue;
        };
        match *field {
            "content_type" => out.content_type = as_single_string(val),
            "creator" => out.creator = as_single_string(val),
            "created" => {
                let s = as_single_string(val);
                out.created_year = parse_year(s.as_deref());
                out.created = s;
            }
            "modified" => out.modified = as_single_string(val),
            "file_size" => out.file_size = as_single_string(val),
            "message_id" => {
                out.message_id = as_single_string(val).map(|s| normalize_message_id(&s));
            }
            "in_reply_to" => {
                out.in_reply_to = as_single_string(val).map(|s| normalize_message_id(&s));
            }
            "references" => {
                out.references = as_string_list(val)
                    .iter()
                    .flat_map(|s| s.split_whitespace().map(normalize_message_id))
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            _ => {}
        }
    }

    let mut seen = std::collections::HashSet::new();
    for key in EMAIL_HEADER_KEYS {
        let Some(val) = metadata.get(*key) else {
            continue;
        };
        for raw in as_string_list(val) {
            if let Some(addr) = extract_email(&raw)
                && seen.insert(addr.clone())
            {
                out.email_addresses.push(addr);
            }
        }
    }

    out
}

/// Parse the four-digit year from a date string (ISO 8601 or similar).
fn parse_year(s: Option<&str>) -> Option<i64> {
    let s = s?;
    s.get(..4)?.parse::<i64>().ok()
}

/// Build the flat Meilisearch document field map from curated metadata.
pub(super) fn build_flat_meta(meta: &IndexedMeta) -> Map<String, Value> {
    let mut m = Map::new();

    if let Some(v) = &meta.content_type {
        m.insert("meta_content_type".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.creator {
        m.insert("meta_creator".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.created {
        m.insert("meta_created".into(), Value::String(v.clone()));
    }
    if let Some(v) = meta.created_year {
        m.insert("meta_created_year".into(), Value::Number(v.into()));
    }
    if let Some(v) = &meta.modified {
        m.insert("meta_modified".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.file_size {
        m.insert("meta_file_size".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.message_id {
        m.insert("meta_message_id".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.in_reply_to {
        m.insert("meta_in_reply_to".into(), Value::String(v.clone()));
    }
    if !meta.references.is_empty() {
        m.insert(
            "meta_references".into(),
            Value::Array(meta.references.iter().cloned().map(Value::String).collect()),
        );
    }
    if !meta.email_addresses.is_empty() {
        m.insert(
            "meta_email_addresses".into(),
            Value::Array(
                meta.email_addresses
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }
    m
}

/// Build the full JSON document body for Meilisearch indexing.
pub(super) fn build_doc_body(doc_id: &str, document: &Document) -> Value {
    let meta = extract_indexed_meta(&document.metadata);
    let mut flat = build_flat_meta(&meta);

    let display_path = document
        .metadata
        .get(AUM_DISPLAY_PATH_KEY)
        .and_then(as_single_string)
        .unwrap_or_else(|| document.source_path.to_string_lossy().into_owned());

    let extracted_from = document
        .metadata
        .get(AUM_EXTRACTED_FROM_KEY)
        .and_then(as_single_string)
        .unwrap_or_default();

    flat.insert("id".into(), Value::String(doc_id.to_owned()));
    flat.insert("display_path".into(), Value::String(display_path));
    flat.insert("extracted_from".into(), Value::String(extracted_from));
    flat.insert("content".into(), Value::String(document.content.clone()));
    flat.insert("has_embeddings".into(), Value::Bool(false));

    Value::Object(flat)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::*;
    use crate::models::MetadataValue;

    fn meta(pairs: &[(&str, &str)]) -> HashMap<String, MetadataValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), MetadataValue::Single(v.to_string())))
            .collect()
    }

    #[test]
    fn extract_creator_from_author_key() {
        let m = meta(&[("Author", "Alice")]);
        let out = extract_indexed_meta(&m);
        assert_eq!(out.creator.as_deref(), Some("Alice"));
    }

    #[test]
    fn extract_created_year() {
        let m = meta(&[("dcterms:created", "2023-06-15T10:00:00Z")]);
        let out = extract_indexed_meta(&m);
        assert_eq!(out.created_year, Some(2023));
    }

    #[test]
    fn extract_message_id_normalised() {
        let m = meta(&[("Message:Raw-Header:Message-ID", " <abc@example.com> ")]);
        let out = extract_indexed_meta(&m);
        assert_eq!(out.message_id.as_deref(), Some("abc@example.com"));
    }

    #[test]
    fn extract_email_addresses_deduped() {
        let mut md: HashMap<String, MetadataValue> = HashMap::new();
        md.insert(
            "Message-From".into(),
            MetadataValue::Single("Alice <alice@example.com>".into()),
        );
        md.insert(
            "Message-To".into(),
            MetadataValue::List(vec![
                "bob@example.com".into(),
                "Alice <alice@example.com>".into(),
            ]),
        );
        let out = extract_indexed_meta(&md);
        assert_eq!(out.email_addresses.len(), 2);
        assert!(
            out.email_addresses
                .contains(&"alice@example.com".to_owned())
        );
        assert!(out.email_addresses.contains(&"bob@example.com".to_owned()));
    }

    #[test]
    fn build_flat_meta_omits_empty_fields() {
        let m = IndexedMeta {
            content_type: Some("application/pdf".into()),
            ..Default::default()
        };
        let flat = build_flat_meta(&m);
        assert!(flat.contains_key("meta_content_type"));
        assert!(!flat.contains_key("meta_creator"));
        assert!(!flat.contains_key("meta_message_id"));
    }

    #[test]
    fn build_doc_body_has_required_fields() -> anyhow::Result<()> {
        use std::path::PathBuf;
        let doc = Document {
            source_path: PathBuf::from("/tmp/test.pdf"),
            content: "hello world".into(),
            metadata: HashMap::new(),
        };
        let body = build_doc_body("doc1", &doc);
        let obj = body.as_object().context("body should be object")?;
        assert_eq!(obj["id"], Value::String("doc1".into()));
        assert_eq!(obj["content"], Value::String("hello world".into()));
        assert_eq!(obj["has_embeddings"], Value::Bool(false));
        Ok(())
    }
}
