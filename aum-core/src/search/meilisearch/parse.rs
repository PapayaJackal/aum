//! Parsing Meilisearch search hits and facet distributions into domain types.

use std::collections::HashMap;

use serde_json::Value;

use crate::search::types::{FacetMap, SearchResult};
use crate::search::utils::string_field;

// ---------------------------------------------------------------------------
// Hit parsing
// ---------------------------------------------------------------------------

/// Convert a raw Meilisearch search hit (JSON object) into a [`SearchResult`].
///
/// `score_override` takes precedence over any `_rankingScore` field embedded in
/// the JSON — the SDK surfaces the score on the typed hit wrapper rather than
/// inside the document body, so callers that have access to it should pass it
/// here.
///
/// `formatted` is the `_formatted` map from the SDK's `SearchResult::formatted_result`
/// field — the SDK extracts it from the hit before exposing `result`, so it must be
/// passed separately.
pub(super) fn parse_hit(
    hit: &Value,
    index_name: &str,
    score_override: Option<f64>,
    formatted: Option<&serde_json::Map<String, Value>>,
) -> Option<SearchResult> {
    let obj = hit.as_object()?;
    let doc_id = obj.get("id")?.as_str()?.to_owned();
    let source_path = string_field(obj, "source_path");
    let display_path = string_field(obj, "display_path");
    let extracted_from = string_field(obj, "extracted_from");
    let score = score_override.unwrap_or_else(|| {
        obj.get("_rankingScore")
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
    });

    let (snippet, display_path_highlighted) = extract_formatted(obj, &display_path, formatted);
    let metadata = extract_metadata(obj);

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

/// Extract highlighted snippet and display path from the `_formatted` map.
///
/// `formatted` comes from the SDK's `SearchResult::formatted_result` — the SDK
/// deserialises it into its own field rather than leaving it inside the document.
fn extract_formatted(
    obj: &serde_json::Map<String, Value>,
    display_path: &str,
    formatted: Option<&serde_json::Map<String, Value>>,
) -> (String, String) {
    let snippet = formatted
        .and_then(|f| f.get("content"))
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| obj.get("content").and_then(|v| v.as_str()).unwrap_or(""))
        .to_owned();
    let display_path_highlighted = formatted
        .and_then(|f| f.get("display_path"))
        .and_then(|v| v.as_str())
        .unwrap_or(display_path)
        .to_owned();
    (snippet, display_path_highlighted)
}

/// Collect `meta_*` fields from a hit into the metadata map.
fn extract_metadata(obj: &serde_json::Map<String, Value>) -> HashMap<String, Value> {
    obj.iter()
        .filter(|(k, _)| k.starts_with("meta_"))
        .map(|(k, v)| {
            let stripped = k.strip_prefix("meta_").unwrap_or(k);
            (stripped.to_owned(), v.clone())
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Facet distribution parsing
// ---------------------------------------------------------------------------

/// Convert a Meilisearch `facetDistribution` object to a typed [`FacetMap`].
///
/// Meilisearch returns `{ "field_name": { "value": count } }`.
/// The outer keys are raw field names (e.g. `"meta_content_type"`); we map
/// them to display labels using the inverse of [`FACET_FIELDS`].
#[cfg(test)]
pub(super) fn parse_facet_distribution(raw: &Value) -> FacetMap {
    use crate::search::constants::FACET_FIELDS;

    // Build field-name → display-label reverse map.
    let field_to_label: HashMap<&str, &str> = FACET_FIELDS
        .entries()
        .map(|(label, field)| (*field, *label))
        .collect();

    let Some(outer) = raw.as_object() else {
        return FacetMap::new();
    };

    outer
        .iter()
        .filter_map(|(field_name, counts_val)| {
            let label = *field_to_label.get(field_name.as_str())?;
            let counts = counts_val.as_object()?;
            let dist: HashMap<String, u64> = counts
                .iter()
                .filter_map(|(val, cnt)| Some((val.clone(), cnt.as_u64()?)))
                .collect();
            Some((label.to_owned(), dist))
        })
        .collect()
}

/// Merge two facet maps by summing counts for matching keys.
pub(super) fn merge_facets(mut base: FacetMap, other: FacetMap) -> FacetMap {
    for (label, counts) in other {
        let entry = base.entry(label).or_default();
        for (value, count) in counts {
            *entry.entry(value).or_insert(0) += count;
        }
    }
    base
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
            "id": "doc1",
            "display_path": "docs/foo.pdf",
            "extracted_from": "",
            "content": "hello world",
            "has_embeddings": false,
            "_rankingScore": 0.9,
        });
        let result = parse_hit(&hit, "aum", None, None).context("should parse hit")?;
        assert_eq!(result.doc_id, "doc1");
        assert_eq!(result.display_path, "docs/foo.pdf");
        assert!((result.score - 0.9).abs() < f64::EPSILON);
        assert_eq!(result.index, "aum");
        Ok(())
    }

    #[test]
    fn parse_hit_uses_formatted_snippet() -> anyhow::Result<()> {
        let hit = json!({
            "id": "doc2",
            "display_path": "a.txt",
            "extracted_from": "",
            "content": "full content",
            "_rankingScore": 0.5,
        });
        // The SDK extracts `_formatted` into a separate field before exposing the document.
        let formatted: serde_json::Map<String, Value> = serde_json::from_value(json!({
            "content": "<mark>highlighted</mark> content",
            "display_path": "<mark>a</mark>.txt",
        }))?;
        let result = parse_hit(&hit, "idx", None, Some(&formatted)).context("should parse hit")?;
        assert_eq!(result.snippet, "<mark>highlighted</mark> content");
        assert_eq!(result.display_path_highlighted, "<mark>a</mark>.txt");
        Ok(())
    }

    #[test]
    fn parse_hit_extracts_meta_fields() -> anyhow::Result<()> {
        let hit = json!({
            "id": "doc3",
            "display_path": "x.pdf",
            "extracted_from": "",
            "content": "",
            "meta_content_type": "application/pdf",
            "meta_creator": "Alice",
            "_rankingScore": 1.0,
        });
        let result = parse_hit(&hit, "aum", None, None).context("should parse hit")?;
        assert_eq!(
            result.metadata.get("content_type").and_then(|v| v.as_str()),
            Some("application/pdf")
        );
        assert_eq!(
            result.metadata.get("creator").and_then(|v| v.as_str()),
            Some("Alice")
        );
        Ok(())
    }

    #[test]
    fn parse_facet_distribution_maps_labels() -> anyhow::Result<()> {
        let raw = json!({
            "meta_content_type": { "application/pdf": 5, "text/plain": 3 },
            "meta_created_year": { "2023": 10 },
        });
        let facets = parse_facet_distribution(&raw);
        let file_type = facets.get("File Type").context("File Type facet missing")?;
        assert_eq!(file_type.get("application/pdf"), Some(&5u64));
        let created = facets.get("Created").context("Created facet missing")?;
        assert_eq!(created.get("2023"), Some(&10u64));
        Ok(())
    }

    #[test]
    fn merge_facets_sums_counts() -> anyhow::Result<()> {
        let mut a = FacetMap::new();
        a.insert(
            "File Type".into(),
            [("PDF".to_owned(), 3u64)].into_iter().collect(),
        );
        let mut b = FacetMap::new();
        b.insert(
            "File Type".into(),
            [("PDF".to_owned(), 2u64), ("Text".to_owned(), 1u64)]
                .into_iter()
                .collect(),
        );
        let merged = merge_facets(a, b);
        let ft = merged.get("File Type").context("File Type missing")?;
        assert_eq!(ft.get("PDF"), Some(&5u64));
        assert_eq!(ft.get("Text"), Some(&1u64));
        Ok(())
    }
}
