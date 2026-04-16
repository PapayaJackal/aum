//! `OpenSearch`-specific document body construction.
//!
//! Metadata extraction is handled by [`crate::search::meta`]; this module
//! converts the resulting [`IndexedMeta`] into the nested `meta` object layout
//! that `OpenSearch` expects.

use serde_json::{Map, Value, json};

use crate::extraction::{AUM_DISPLAY_PATH_KEY, AUM_EXTRACTED_FROM_KEY};
use crate::models::Document;
use crate::search::meta::{
    IndexedMeta, as_single_string, document_type_label, extract_indexed_meta,
};

// ---------------------------------------------------------------------------
// OpenSearch-specific meta builder
// ---------------------------------------------------------------------------

#[allow(clippy::doc_markdown)]
/// Build the nested `meta` object for OpenSearch from curated metadata.
///
/// OpenSearch supports nested objects natively, so we use `meta.created`,
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
    for (key, list) in [
        ("email_from", &meta.email_from),
        ("email_to", &meta.email_to),
        ("email_cc", &meta.email_cc),
        ("email_bcc", &meta.email_bcc),
    ] {
        if !list.is_empty() {
            m.insert(
                key.into(),
                Value::Array(list.iter().cloned().map(Value::String).collect()),
            );
        }
    }
    if let Some(v) = &meta.email_subject {
        m.insert("email_subject".into(), Value::String(v.clone()));
    }
    m
}

#[allow(clippy::doc_markdown)]
/// Build the full JSON document body for OpenSearch indexing.
///
/// Returns `(doc_id, body)` where `body` is ready to be passed as the document
/// source in a bulk index operation.
pub(super) fn build_doc_body(doc_id: &str, document: &Document) -> (String, Value) {
    let meta = extract_indexed_meta(&document.metadata);
    let mut nested_meta = build_nested_meta(&meta);

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

    nested_meta.insert(
        "document_type".into(),
        Value::String(document_type_label(&extracted_from).into()),
    );

    // Raw Tika metadata for display in the web UI, minus internal `_aum_*`
    // keys (display_path / extracted_from are already top-level fields) and
    // anything that would collide with the canonical `meta.*` keys.
    let raw_metadata = serialize_raw_metadata(document);

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

/// Serialize `document.metadata` to JSON, skipping internal `_aum_*` keys so
/// they never reach the search index or downstream API responses.
fn serialize_raw_metadata(document: &Document) -> Value {
    let mut out = Map::new();
    for (k, v) in &document.metadata {
        if k.starts_with("_aum_") {
            continue;
        }
        let serialized = serde_json::to_value(v).unwrap_or(Value::Null);
        out.insert(k.clone(), serialized);
    }
    Value::Object(out)
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
    use crate::search::constants::{DOC_TYPE_ATTACHMENT, DOC_TYPE_PARENT};

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
        assert_eq!(nested.get("file_size").and_then(Value::as_i64), Some(12345));
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
    fn build_doc_body_excludes_internal_aum_keys() -> anyhow::Result<()> {
        let doc = Document {
            source_path: PathBuf::from("/tmp/test.pdf"),
            content: String::new(),
            metadata: meta(&[
                ("_aum_display_path", "some/display/path.pdf"),
                ("_aum_extracted_from", "parent.eml"),
                ("pdf:num_pages", "12"),
            ]),
        };
        let (_, body) = build_doc_body("doc1", &doc);
        let obj = body.as_object().context("body should be object")?;
        let metadata_obj = obj
            .get("metadata")
            .and_then(Value::as_object)
            .context("metadata should be object")?;
        assert!(!metadata_obj.contains_key("_aum_display_path"));
        assert!(!metadata_obj.contains_key("_aum_extracted_from"));
        // Non-internal Tika fields are preserved so the UI can display them.
        assert!(metadata_obj.contains_key("pdf:num_pages"));
        Ok(())
    }

    #[test]
    fn build_nested_meta_includes_email_display_fields() {
        let m = IndexedMeta {
            email_from: vec!["Alice <alice@example.com>".into()],
            email_to: vec!["bob@example.com".into()],
            email_subject: Some("Hello".into()),
            ..Default::default()
        };
        let nested = build_nested_meta(&m);
        assert_eq!(
            nested
                .get("email_from")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_str()),
            Some("Alice <alice@example.com>")
        );
        assert_eq!(
            nested
                .get("email_to")
                .and_then(|v| v.as_array())
                .and_then(|a| a.first())
                .and_then(|v| v.as_str()),
            Some("bob@example.com")
        );
        assert_eq!(
            nested.get("email_subject").and_then(|v| v.as_str()),
            Some("Hello")
        );
    }

    #[test]
    fn parent_document_type_when_no_extracted_from() -> anyhow::Result<()> {
        let doc = Document {
            source_path: PathBuf::from("/tmp/test.pdf"),
            content: String::new(),
            metadata: HashMap::new(),
        };
        let (_, body) = build_doc_body("doc1", &doc);
        let meta_obj = body
            .as_object()
            .and_then(|o| o.get("meta"))
            .and_then(|v| v.as_object())
            .context("meta should be object")?;
        assert_eq!(
            meta_obj.get("document_type").and_then(|v| v.as_str()),
            Some(DOC_TYPE_PARENT)
        );
        Ok(())
    }

    #[test]
    fn attachment_document_type_when_extracted_from_set() -> anyhow::Result<()> {
        let doc = Document {
            source_path: PathBuf::from("/tmp/email.eml/attachment.pdf"),
            content: String::new(),
            metadata: meta(&[("_aum_extracted_from", "email.eml")]),
        };
        let (_, body) = build_doc_body("doc2", &doc);
        let meta_obj = body
            .as_object()
            .and_then(|o| o.get("meta"))
            .and_then(|v| v.as_object())
            .context("meta should be object")?;
        assert_eq!(
            meta_obj.get("document_type").and_then(|v| v.as_str()),
            Some(DOC_TYPE_ATTACHMENT)
        );
        Ok(())
    }

    #[test]
    fn build_doc_body_nested_meta_content_type() -> anyhow::Result<()> {
        let md = meta(&[("Content-Type", "application/pdf")]);
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
