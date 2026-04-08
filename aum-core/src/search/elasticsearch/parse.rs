//! Parsing Elasticsearch search hits into domain types.

use std::collections::HashMap;

use serde_json::Value;

use crate::search::constants::{DATE_FACETS, FACET_FILE_TYPE, MIMETYPE_ALIASES};
use crate::search::types::SearchResult;
use crate::search::utils::string_field;

use super::query::ES_FACET_META_KEYS;

// ---------------------------------------------------------------------------
// Hit parsing
// ---------------------------------------------------------------------------

/// Convert a raw Elasticsearch search hit into a [`SearchResult`].
///
/// Returns `None` if the hit is missing required fields (`_id`, `_source`).
pub(super) fn parse_hit(hit: &Value, index_name: &str) -> Option<SearchResult> {
    let obj = hit.as_object()?;
    let doc_id = obj.get("_id")?.as_str()?.to_owned();
    let source = obj.get("_source")?.as_object()?;

    let source_path = string_field(source, "source_path");
    let display_path = string_field(source, "display_path");
    let extracted_from = string_field(source, "extracted_from");
    let score = obj.get("_score").and_then(Value::as_f64).unwrap_or(0.0);

    // Extract snippet from content highlight, or fall back to first 200 chars.
    let highlight = obj.get("highlight");
    let snippet = highlight
        .and_then(|h| h.get("content"))
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .map_or_else(
            || {
                source
                    .get("content")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .chars()
                    .take(200)
                    .collect()
            },
            str::to_owned,
        );

    let display_path_highlighted = highlight
        .and_then(|h| h.get("display_path"))
        .and_then(|v| v.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();

    // Build metadata map from the raw `metadata` blob.
    let mut metadata: HashMap<String, Value> = source
        .get("metadata")
        .and_then(|v| v.as_object())
        .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();

    // Inject facet-friendly keys from the typed `meta` object so the UI can
    // use them for facet display and filtering.
    inject_facet_meta(&mut metadata, source);

    Some(SearchResult {
        doc_id,
        source_path,
        display_path,
        display_path_highlighted,
        score,
        snippet,
        extracted_from,
        metadata,
        index: index_name.to_owned(),
    })
}

/// Parse hits from a full Elasticsearch search response.
///
/// Returns `(results, total_hits)`.
pub(super) fn parse_hits(resp: &Value) -> (Vec<SearchResult>, u64) {
    let hits_obj = resp.get("hits").and_then(|v| v.as_object());

    let total = hits_obj
        .and_then(|h| h.get("total"))
        .and_then(|t| {
            // ES returns `{ "value": N, "relation": "eq" }` for total.
            if let Some(obj) = t.as_object() {
                obj.get("value").and_then(Value::as_u64)
            } else {
                t.as_u64()
            }
        })
        .unwrap_or(0);

    let results = hits_obj
        .and_then(|h| h.get("hits"))
        .and_then(|v| v.as_array())
        .map(|hits| {
            hits.iter()
                .filter_map(|h| {
                    let index = h
                        .get("_index")
                        .and_then(|v| v.as_str())
                        .unwrap_or("")
                        .to_owned();
                    parse_hit(h, &index)
                })
                .collect()
        })
        .unwrap_or_default();

    (results, total)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Inject facet display values from the `meta` nested object into `metadata`.
///
/// The UI expects facet values under their display label (e.g. `"File Type"`,
/// `"Created"`). We extract the raw values from `meta.*` and normalise them:
/// - MIME types are aliased to short labels (e.g. `"application/pdf"` → `"PDF"`)
/// - Date values are truncated to the four-digit year
fn inject_facet_meta(
    metadata: &mut HashMap<String, Value>,
    source: &serde_json::Map<String, Value>,
) {
    let Some(meta) = source.get("meta").and_then(|v| v.as_object()) else {
        return;
    };

    for (label, meta_key) in ES_FACET_META_KEYS {
        let Some(val) = meta.get(*meta_key) else {
            continue;
        };

        let normalised: Value = if *label == FACET_FILE_TYPE {
            match val.as_str() {
                Some(mime) => {
                    let alias = MIMETYPE_ALIASES.get(mime).copied().unwrap_or(mime);
                    Value::String(alias.to_owned())
                }
                None => continue,
            }
        } else if DATE_FACETS.contains(label) {
            match val.as_str() {
                Some(date) if date.len() >= 4 => Value::String(date[..4].to_owned()),
                _ => continue,
            }
        } else {
            val.clone()
        };

        metadata.insert((*label).to_owned(), normalised);
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use anyhow::Context as _;
    use serde_json::json;

    use crate::search::constants::{FACET_CREATED, FACET_FILE_TYPE};

    use super::*;

    #[test]
    fn parse_basic_hit() -> anyhow::Result<()> {
        let hit = json!({
            "_id": "doc1",
            "_index": "aum",
            "_score": 0.9,
            "_source": {
                "display_path": "docs/foo.pdf",
                "extracted_from": "",
                "content": "hello world",
                "has_embeddings": false,
                "metadata": {},
                "meta": {}
            }
        });
        let result = parse_hit(&hit, "aum").context("should parse hit")?;
        assert_eq!(result.doc_id, "doc1");
        assert_eq!(result.display_path, "docs/foo.pdf");
        assert!((result.score - 0.9).abs() < f64::EPSILON);
        assert_eq!(result.index, "aum");
        Ok(())
    }

    #[test]
    fn parse_hit_uses_content_highlight() -> anyhow::Result<()> {
        let hit = json!({
            "_id": "doc2",
            "_index": "idx",
            "_score": 0.5,
            "_source": {
                "display_path": "a.txt",
                "extracted_from": "",
                "content": "full content here",
                "metadata": {},
                "meta": {}
            },
            "highlight": {
                "content": ["<mark>highlighted</mark> content"],
                "display_path": ["<mark>a</mark>.txt"]
            }
        });
        let result = parse_hit(&hit, "idx").context("should parse hit")?;
        assert_eq!(result.snippet, "<mark>highlighted</mark> content");
        assert_eq!(result.display_path_highlighted, "<mark>a</mark>.txt");
        Ok(())
    }

    #[test]
    fn parse_hit_injects_file_type_alias() -> anyhow::Result<()> {
        let hit = json!({
            "_id": "doc3",
            "_index": "aum",
            "_score": 1.0,
            "_source": {
                "display_path": "x.pdf",
                "extracted_from": "",
                "content": "",
                "metadata": {},
                "meta": { "content_type": "application/pdf", "created": "2023-01-01T00:00:00Z" }
            }
        });
        let result = parse_hit(&hit, "aum").context("should parse hit")?;
        assert_eq!(
            result
                .metadata
                .get(FACET_FILE_TYPE)
                .and_then(|v| v.as_str()),
            Some("PDF")
        );
        assert_eq!(
            result.metadata.get(FACET_CREATED).and_then(|v| v.as_str()),
            Some("2023")
        );
        Ok(())
    }

    #[test]
    fn parse_hits_total_and_results() {
        let resp = json!({
            "hits": {
                "total": { "value": 2, "relation": "eq" },
                "hits": [
                    {
                        "_id": "a", "_index": "idx", "_score": 1.0,
                        "_source": { "display_path": "a", "extracted_from": "", "content": "", "metadata": {}, "meta": {} }
                    },
                    {
                        "_id": "b", "_index": "idx", "_score": 0.5,
                        "_source": { "display_path": "b", "extracted_from": "", "content": "", "metadata": {}, "meta": {} }
                    }
                ]
            }
        });
        let (results, total) = parse_hits(&resp);
        assert_eq!(total, 2);
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].doc_id, "a");
        assert_eq!(results[1].doc_id, "b");
    }
}
