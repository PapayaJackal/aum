//! Parsing `OpenSearch` search hits into domain types.

use std::collections::HashMap;

use serde_json::Value;

use crate::search::types::SearchResult;
use crate::search::utils::string_field;

// ---------------------------------------------------------------------------
// Hit parsing
// ---------------------------------------------------------------------------

#[allow(clippy::doc_markdown)]
/// Convert a raw OpenSearch search hit into a [`SearchResult`].
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

    // Start from the raw Tika metadata blob so the web UI can display every
    // field Tika extracted, then layer the curated `meta.*` object on top so
    // canonical keys (`content_type`, `created`, `email_subject`, ...) that
    // the API and UI look up are always present with normalised names. The
    // overlay also wins on conflicts, which matters for e.g. `content_type`
    // where Tika emits `Content-Type` with an MIME param suffix. Internal
    // `_aum_*` keys are filtered out at index time in `build_doc_body`.
    let mut metadata: HashMap<String, Value> = source
        .get("metadata")
        .and_then(|v| v.as_object())
        .map(|o| o.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .unwrap_or_default();
    if let Some(meta_obj) = source.get("meta").and_then(|v| v.as_object()) {
        for (k, v) in meta_obj {
            metadata.insert(k.clone(), v.clone());
        }
    }

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

#[allow(clippy::doc_markdown)]
/// Parse hits from a full OpenSearch search response.
///
/// Returns `(results, total_hits)`.
pub(super) fn parse_hits(resp: &Value) -> (Vec<SearchResult>, u64) {
    let hits_obj = resp.get("hits").and_then(|v| v.as_object());

    let total = hits_obj
        .and_then(|h| h.get("total"))
        .and_then(|t| {
            // OpenSearch returns `{ "value": N, "relation": "eq" }` for total.
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
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use anyhow::Context as _;
    use serde_json::json;

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
    fn parse_hit_exposes_meta_under_canonical_keys() -> anyhow::Result<()> {
        let hit = json!({
            "_id": "doc3",
            "_index": "aum",
            "_score": 1.0,
            "_source": {
                "display_path": "x.pdf",
                "extracted_from": "",
                "content": "",
                "metadata": {},
                "meta": {
                    "content_type": "application/pdf",
                    "created": "2023-01-01T00:00:00Z",
                    "email_subject": "Hello",
                    "email_from": ["Alice <alice@example.com>"],
                }
            }
        });
        let result = parse_hit(&hit, "aum").context("should parse hit")?;
        assert_eq!(
            result.metadata.get("content_type").and_then(|v| v.as_str()),
            Some("application/pdf")
        );
        assert_eq!(
            result.metadata.get("created").and_then(|v| v.as_str()),
            Some("2023-01-01T00:00:00Z")
        );
        assert_eq!(
            result
                .metadata
                .get("email_subject")
                .and_then(|v| v.as_str()),
            Some("Hello")
        );
        Ok(())
    }

    #[test]
    fn parse_hit_merges_raw_tika_with_canonical_meta() -> anyhow::Result<()> {
        let hit = json!({
            "_id": "doc3a",
            "_index": "aum",
            "_score": 1.0,
            "_source": {
                "display_path": "x.pdf",
                "extracted_from": "",
                "content": "",
                "metadata": {
                    "pdf:num_pages": "12",
                    "xmp:CreatorTool": "Adobe Acrobat",
                    "Content-Type": "application/pdf; charset=binary"
                },
                "meta": {
                    "content_type": "application/pdf"
                }
            }
        });
        let result = parse_hit(&hit, "aum").context("should parse hit")?;
        // Raw Tika fields are preserved for UI display.
        assert_eq!(
            result
                .metadata
                .get("pdf:num_pages")
                .and_then(|v| v.as_str()),
            Some("12")
        );
        assert_eq!(
            result
                .metadata
                .get("xmp:CreatorTool")
                .and_then(|v| v.as_str()),
            Some("Adobe Acrobat")
        );
        // Canonical `content_type` (params stripped) is injected alongside the
        // raw `Content-Type`, so the UI and preview route get a clean value.
        assert_eq!(
            result.metadata.get("content_type").and_then(|v| v.as_str()),
            Some("application/pdf")
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
                        "_source": { "display_path": "a", "extracted_from": "", "content": "", "meta": {} }
                    },
                    {
                        "_id": "b", "_index": "idx", "_score": 0.5,
                        "_source": { "display_path": "b", "extracted_from": "", "content": "", "meta": {} }
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
