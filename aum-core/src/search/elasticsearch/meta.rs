//! Elasticsearch-specific document body construction.
//!
//! Metadata extraction is handled by [`crate::search::meta`]; this module
//! converts the resulting [`IndexedMeta`] into the nested `meta` object layout
//! that Elasticsearch expects.

use serde_json::{Map, Value, json};

use crate::extraction::{AUM_DISPLAY_PATH_KEY, AUM_EXTRACTED_FROM_KEY};
use crate::models::Document;
use crate::search::meta::{IndexedMeta, as_single_string, extract_indexed_meta};

// ---------------------------------------------------------------------------
// ES-specific meta builder
// ---------------------------------------------------------------------------

/// Build the nested `meta` object for Elasticsearch from curated metadata.
///
/// Elasticsearch supports nested objects natively, so we use `meta.created`,
/// `meta.content_type`, etc. (unlike Meilisearch which uses flat `meta_*` keys).
/// `file_size` is stored as a long integer when parseable.
pub(super) fn build_nested_meta(meta: &IndexedMeta) -> Map<String, Value> {
    let mut m = Map::new();

    if let Some(v) = &meta.content_type {
        m.insert("content_type".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.creator {
        m.insert("creator".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.created {
        m.insert("created".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.modified {
        m.insert("modified".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.file_size {
        // Try to parse as integer; fall back to omitting the field on failure
        // so the long mapping doesn't reject the document.
        if let Ok(n) = v.parse::<i64>() {
            m.insert(
                "file_size".into(),
                Value::Number(serde_json::Number::from(n)),
            );
        }
    }
    if let Some(v) = &meta.message_id {
        m.insert("message_id".into(), Value::String(v.clone()));
    }
    if let Some(v) = &meta.in_reply_to {
        m.insert("in_reply_to".into(), Value::String(v.clone()));
    }
    if !meta.references.is_empty() {
        m.insert(
            "references".into(),
            Value::Array(meta.references.iter().cloned().map(Value::String).collect()),
        );
    }
    if !meta.email_addresses.is_empty() {
        m.insert(
            "email_addresses".into(),
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

/// Build the full JSON document body for Elasticsearch indexing.
///
/// Returns `(doc_id, body)` where `body` is ready to be passed as the document
/// source in a bulk index operation.
pub(super) fn build_doc_body(doc_id: &str, document: &Document) -> (String, Value) {
    let meta = extract_indexed_meta(&document.metadata);
    let nested_meta = build_nested_meta(&meta);

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

    // Serialize the full raw metadata blob (stored but not indexed).
    // MetadataValue derives Serialize with #[serde(untagged)] so this
    // produces the same JSON as the manual conversion.
    let raw_metadata =
        serde_json::to_value(&document.metadata).unwrap_or(Value::Object(Map::new()));

    let body = json!({
        "source_path":    document.source_path.to_string_lossy(),
        "display_path":   display_path,
        "extracted_from": extracted_from,
        "content":        document.content,
        "metadata":       raw_metadata,
        "meta":           Value::Object(nested_meta),
        "has_embeddings": false,
    });

    (doc_id.to_owned(), body)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use anyhow::Context as _;

    use super::*;
    use crate::models::{Document, MetadataValue};

    fn meta(pairs: &[(&str, &str)]) -> HashMap<String, MetadataValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), MetadataValue::Single(v.to_string())))
            .collect()
    }

    #[test]
    fn build_nested_meta_omits_empty_fields() {
        let m = IndexedMeta {
            content_type: Some("application/pdf".into()),
            ..Default::default()
        };
        let nested = build_nested_meta(&m);
        assert!(nested.contains_key("content_type"));
        assert!(!nested.contains_key("creator"));
        assert!(!nested.contains_key("file_size"));
    }

    #[test]
    fn build_nested_meta_parses_file_size() {
        let m = IndexedMeta {
            file_size: Some("12345".into()),
            ..Default::default()
        };
        let nested = build_nested_meta(&m);
        assert_eq!(
            nested.get("file_size").and_then(|v| v.as_i64()),
            Some(12345)
        );
    }

    #[test]
    fn build_nested_meta_drops_unparseable_file_size() {
        let m = IndexedMeta {
            file_size: Some("not-a-number".into()),
            ..Default::default()
        };
        let nested = build_nested_meta(&m);
        assert!(!nested.contains_key("file_size"));
    }

    #[test]
    fn build_doc_body_has_required_fields() -> anyhow::Result<()> {
        let doc = Document {
            source_path: PathBuf::from("/tmp/test.pdf"),
            content: "hello world".into(),
            metadata: HashMap::new(),
        };
        let (id, body) = build_doc_body("doc1", &doc);
        let obj = body.as_object().context("body should be object")?;
        assert_eq!(id, "doc1");
        assert_eq!(obj["content"], Value::String("hello world".into()));
        assert_eq!(obj["has_embeddings"], Value::Bool(false));
        assert!(obj.contains_key("meta"));
        assert!(obj.contains_key("metadata"));
        Ok(())
    }

    #[test]
    fn build_doc_body_nested_meta_content_type() -> anyhow::Result<()> {
        let mut md = meta(&[("Content-Type", "application/pdf")]);
        let doc = Document {
            source_path: PathBuf::from("/tmp/test.pdf"),
            content: String::new(),
            metadata: md,
        };
        let (_, body) = build_doc_body("doc1", &doc);
        let meta_obj = body
            .as_object()
            .and_then(|o| o.get("meta"))
            .and_then(|v| v.as_object())
            .context("meta should be object")?;
        assert_eq!(
            meta_obj.get("content_type").and_then(|v| v.as_str()),
            Some("application/pdf")
        );
        Ok(())
    }
}
