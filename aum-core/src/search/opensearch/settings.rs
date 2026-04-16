//! `OpenSearch` index mapping builders.

use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Meta field type registry
// ---------------------------------------------------------------------------

#[allow(clippy::doc_markdown)]
/// Expected OpenSearch type for every field in the `meta` object.
///
/// This is the single source of truth: [`build_index_body`] generates the
/// mapping from it, and [`super::mapping_matches`] validates live indexes against it.
/// Add a new entry here when adding a field — both mapping creation and
/// staleness detection will pick it up automatically.
pub(super) static META_FIELD_TYPES: &[(&str, &str)] = &[
    ("content_type", "keyword"),
    ("creator", "keyword"),
    ("created", "date"),
    ("modified", "date"),
    ("file_size", "long"),
    ("email_addresses", "keyword"),
    ("message_id", "keyword"),
    ("in_reply_to", "keyword"),
    ("references", "keyword"),
    ("document_type", "keyword"),
];

// ---------------------------------------------------------------------------
// Mapping builder
// ---------------------------------------------------------------------------

#[allow(clippy::doc_markdown)]
/// Build the full OpenSearch index creation body (settings + mappings).
///
/// Includes optional `chunks` nested field for dense vector storage when
/// `vector_dimension` is provided, and a `has_embeddings` boolean field that
/// lets us efficiently query for documents that still need embedding.
pub(super) fn build_index_body(vector_dimension: Option<u32>, max_highlight_offset: u64) -> Value {
    // Build `meta` properties from the registry so the mapping stays in sync
    // with the field-type list used by `mapping_matches`.
    let mut meta_props = serde_json::Map::new();
    for (field, es_type) in META_FIELD_TYPES {
        let def = match *es_type {
            "date" => json!({
                "type": "date",
                "format": "strict_date_optional_time||epoch_millis",
                "ignore_malformed": true
            }),
            _ => json!({ "type": es_type }),
        };
        meta_props.insert((*field).to_owned(), def);
    }
    let meta_properties = Value::Object(meta_props);

    let mut properties = json!({
        "source_path": { "type": "keyword" },
        "display_path": {
            "type": "text",
            "analyzer": "standard",
            "fields": { "keyword": { "type": "keyword" } }
        },
        "extracted_from": { "type": "keyword" },
        "content":        { "type": "text", "analyzer": "standard" },
        // Raw Tika metadata blob — stored in _source for display in the web UI
        // but not indexed (so its inner keys don't count against the field-count
        // limit regardless of how many properties Tika emits).
        "metadata":       { "type": "object", "enabled": false },
        "has_embeddings": { "type": "boolean" },
        "meta": {
            "type": "object",
            "dynamic": false,
            "properties": meta_properties
        },
    });

    let mut settings = json!({
        "highlight.max_analyzed_offset": max_highlight_offset,
    });

    if let Some(dim) = vector_dimension {
        properties["chunks"] = json!({
            "type": "nested",
            "properties": {
                "embedding": {
                    "type": "knn_vector",
                    "dimension": dim,
                    "method": {
                        "engine": "lucene",
                        "name": "hnsw",
                        "space_type": "cosinesimil"
                    }
                }
            }
        });
        settings["index.knn"] = json!(true);
    }

    json!({
        "settings": settings,
        "mappings": {
            "properties": properties
        }
    })
}
