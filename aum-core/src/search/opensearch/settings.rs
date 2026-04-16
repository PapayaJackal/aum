//! `OpenSearch` index mapping builders.

use serde_json::{Value, json};

// ---------------------------------------------------------------------------
// Custom analyzers
// ---------------------------------------------------------------------------

/// Name of the custom analyzer applied to `display_path`.
///
/// Needed because the built-in `standard` analyzer preserves characters like
/// `_` and `.` inside filename tokens (so `my_file.pdf` stays a single token)
/// *and* keeps runs of letters and digits together (so `REPORT12345` stays a
/// single token and `report` won't match it). Meilisearch splits on both
/// punctuation and letter/digit transitions; this analyzer does the same by
/// chaining a pattern tokeniser with `word_delimiter_graph`.
pub(super) const PATH_ANALYZER: &str = "aum_path_analyzer";

const PATH_TOKENIZER: &str = "aum_path_tokenizer";
const PATH_WORD_DELIMITER: &str = "aum_path_word_delimiter";

/// Regex fed to the `pattern` tokeniser: split on any run of characters that
/// is neither a Unicode letter nor a Unicode digit. The Java regex engine on
/// the server side sees `[^\p{L}\p{N}]+` once JSON-escaping is reversed.
const PATH_ANALYZER_PATTERN: &str = r"[^\p{L}\p{N}]+";

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
            "analyzer": PATH_ANALYZER,
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
        "analysis": {
            "analyzer": {
                PATH_ANALYZER: {
                    "type": "custom",
                    "tokenizer": PATH_TOKENIZER,
                    // `word_delimiter_graph` must run before `lowercase` so it
                    // can see case boundaries in mixed-case filenames.
                    "filter": [PATH_WORD_DELIMITER, "lowercase"],
                }
            },
            "tokenizer": {
                PATH_TOKENIZER: {
                    "type": "pattern",
                    "pattern": PATH_ANALYZER_PATTERN,
                }
            },
            "filter": {
                PATH_WORD_DELIMITER: {
                    "type": "word_delimiter_graph",
                    // Split runs of letters and digits (REPORT12345 ->
                    // REPORT + 12345) and case transitions (MyReport ->
                    // My + Report) so that users can match filename fragments.
                    "split_on_numerics": true,
                    "split_on_case_change": true,
                    "generate_word_parts": true,
                    "generate_number_parts": true,
                    "preserve_original": false,
                }
            }
        },
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

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::*;

    #[test]
    fn display_path_mapping_uses_custom_analyzer_with_keyword_subfield() -> anyhow::Result<()> {
        let body = build_index_body(None, 1_000_000);
        let analyzer = body
            .pointer("/mappings/properties/display_path/analyzer")
            .and_then(Value::as_str)
            .context("missing display_path analyzer")?;
        assert_eq!(analyzer, PATH_ANALYZER);
        let kw_type = body
            .pointer("/mappings/properties/display_path/fields/keyword/type")
            .and_then(Value::as_str)
            .context("missing display_path.keyword subfield")?;
        assert_eq!(kw_type, "keyword");
        Ok(())
    }

    #[test]
    fn path_analyzer_wires_tokenizer_and_filters() -> anyhow::Result<()> {
        let body = build_index_body(None, 1_000_000);
        let analyzer = body
            .pointer(&format!("/settings/analysis/analyzer/{PATH_ANALYZER}"))
            .and_then(Value::as_object)
            .context("path analyzer missing from settings")?;
        assert_eq!(
            analyzer.get("tokenizer").and_then(Value::as_str),
            Some(PATH_TOKENIZER)
        );
        let filters: Vec<&str> = analyzer
            .get("filter")
            .and_then(Value::as_array)
            .context("missing filter chain")?
            .iter()
            .filter_map(Value::as_str)
            .collect();
        // Filter order is load-bearing: `word_delimiter_graph` must run before
        // `lowercase` so case transitions are still visible.
        assert_eq!(filters, vec![PATH_WORD_DELIMITER, "lowercase"]);
        Ok(())
    }

    #[test]
    fn word_delimiter_filter_splits_letters_and_digits() -> anyhow::Result<()> {
        let body = build_index_body(None, 1_000_000);
        let filter = body
            .pointer(&format!("/settings/analysis/filter/{PATH_WORD_DELIMITER}"))
            .and_then(Value::as_object)
            .context("word delimiter filter missing from settings")?;
        assert_eq!(
            filter.get("split_on_numerics").and_then(Value::as_bool),
            Some(true),
            "split_on_numerics is required so that `REPORT12345` tokenises \
             into `report` and `12345`, letting a query for `report` hit a \
             file named `REPORT12345.pdf`"
        );
        assert_eq!(
            filter.get("split_on_case_change").and_then(Value::as_bool),
            Some(true)
        );
        Ok(())
    }
}
