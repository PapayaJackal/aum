//! Meilisearch-specific document body construction.
//!
//! Metadata extraction is handled by [`crate::search::meta`]; this module
//! converts the resulting [`IndexedMeta`] into the flat field layout that
//! Meilisearch expects (all fields at the top level with a `meta_` prefix).

use serde_json::{Map, Value};

use crate::extraction::{AUM_DISPLAY_PATH_KEY, AUM_EXTRACTED_FROM_KEY};
use crate::models::Document;
use crate::search::meta::{IndexedMeta, as_single_string, extract_indexed_meta};

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Build the flat Meilisearch document field map from curated metadata.
///
/// All indexed meta fields use a `meta_` prefix and live at the top level of
/// the document object (Meilisearch does not support nested faceting).
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
    if !meta.email_from.is_empty() {
        m.insert(
            "meta_email_from".into(),
            Value::Array(meta.email_from.iter().cloned().map(Value::String).collect()),
        );
    }
    if !meta.email_to.is_empty() {
        m.insert(
            "meta_email_to".into(),
            Value::Array(meta.email_to.iter().cloned().map(Value::String).collect()),
        );
    }
    if !meta.email_cc.is_empty() {
        m.insert(
            "meta_email_cc".into(),
            Value::Array(meta.email_cc.iter().cloned().map(Value::String).collect()),
        );
    }
    if !meta.email_bcc.is_empty() {
        m.insert(
            "meta_email_bcc".into(),
            Value::Array(meta.email_bcc.iter().cloned().map(Value::String).collect()),
        );
    }
    if let Some(v) = &meta.email_subject {
        m.insert("meta_email_subject".into(), Value::String(v.clone()));
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
    flat.insert(
        "source_path".into(),
        Value::String(document.source_path.to_string_lossy().into_owned()),
    );
    flat.insert("display_path".into(), Value::String(display_path));
    flat.insert("extracted_from".into(), Value::String(extracted_from));
    flat.insert("content".into(), Value::String(document.content.clone()));
    flat.insert("has_embeddings".into(), Value::Bool(false));
    // Opt out of the userProvided embedder requirement — vectors are written
    // later by `aum embed`. Without this Meilisearch rejects the entire batch.
    flat.insert("_vectors".into(), serde_json::json!({ "default": null }));

    Value::Object(flat)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use anyhow::Context as _;

    use super::*;
    use crate::models::MetadataValue;

    fn meta_map(pairs: &[(&str, &str)]) -> HashMap<String, MetadataValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), MetadataValue::Single(v.to_string())))
            .collect()
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
        assert_eq!(obj["_vectors"], serde_json::json!({ "default": null }));
        Ok(())
    }

    #[test]
    fn content_type_from_extraction() {
        let md = meta_map(&[("Content-Type", "text/html; charset=UTF-8")]);
        let meta = extract_indexed_meta(&md);
        let flat = build_flat_meta(&meta);
        // The shared extractor already strips params
        assert_eq!(
            flat.get("meta_content_type").and_then(|v| v.as_str()),
            Some("text/html")
        );
    }
}
