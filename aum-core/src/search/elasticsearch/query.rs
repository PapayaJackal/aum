//! Elasticsearch query body builders: filters, sorts, text/vector search bodies,
//! highlight config, and facet aggregation parsing.

use std::collections::HashMap;

use serde_json::{Value, json};

use crate::search::constants::{DATE_FACETS, REVERSE_MIMETYPE_ALIASES};
use crate::search::types::{FacetMap, FilterMap, SortSpec};

// ---------------------------------------------------------------------------
// ES-specific field name mappings
// ---------------------------------------------------------------------------

/// Maps facet display labels to their Elasticsearch field paths.
///
/// These use nested dot notation (`meta.created`) unlike the Meilisearch backend
/// which uses flat `meta_*` field names.
pub(super) static ES_FACET_FIELDS: &[(&str, &str)] = &[
    ("Created", "meta.created"),
    ("File Type", "meta.content_type"),
    ("Creator", "meta.creator"),
    ("Email Addresses", "meta.email_addresses"),
];

/// Object-key suffix of each ES facet field (the part after `"meta."`), paired
/// with its display label. Used by the parse layer to avoid repeated `strip_prefix`
/// calls in the search hot path.
pub(super) static ES_FACET_META_KEYS: &[(&str, &str)] = &[
    ("Created", "created"),
    ("File Type", "content_type"),
    ("Creator", "creator"),
    ("Email Addresses", "email_addresses"),
];

/// Maps sort key names (from `SortSpec.field`) to Elasticsearch field paths.
static ES_SORT_FIELD_MAP: &[(&str, &str)] = &[
    ("meta_created_year", "meta.created"),
    ("meta_file_size", "meta.file_size"),
];

// ---------------------------------------------------------------------------
// Filter builder
// ---------------------------------------------------------------------------

/// Convert a [`FilterMap`] into a list of Elasticsearch filter clause objects.
///
/// Date facets become range filters; File Type values are reverse-mapped from
/// display aliases to raw MIME types; all others become `terms` filters.
pub(super) fn build_filter_clauses(filters: &FilterMap) -> Vec<Value> {
    let mut clauses = Vec::new();

    for (label, values) in filters {
        if values.is_empty() {
            continue;
        }
        let Some(&es_field) = ES_FACET_FIELDS
            .iter()
            .find(|(l, _)| *l == label.as_str())
            .map(|(_, f)| f)
        else {
            continue;
        };

        if DATE_FACETS.contains(label.as_str()) {
            // values = ["2020", "from:2020", "to:2023"] etc.
            let mut range: serde_json::Map<String, Value> =
                serde_json::Map::from_iter([("format".to_owned(), Value::String("yyyy".into()))]);
            for v in values {
                if let Some(year) = v
                    .strip_prefix("from:")
                    .and_then(|s| s.trim().parse::<u32>().ok())
                {
                    range.insert("gte".into(), Value::String(year.to_string()));
                } else if let Some(year) = v
                    .strip_prefix("to:")
                    .and_then(|s| s.trim().parse::<u32>().ok())
                {
                    range.insert("lte".into(), Value::String(year.to_string()));
                } else if let Ok(year) = v.trim().parse::<u32>() {
                    range.insert("gte".into(), Value::String(year.to_string()));
                    range.insert("lte".into(), Value::String(year.to_string()));
                }
            }
            if range.len() > 1 {
                // More than just "format" means we have actual bounds.
                clauses.push(json!({ "range": { es_field: Value::Object(range) } }));
            }
        } else if label == "File Type" {
            let raw_types: Vec<Value> = values
                .iter()
                .map(|alias| {
                    let raw = REVERSE_MIMETYPE_ALIASES
                        .get(alias.as_str())
                        .copied()
                        .unwrap_or(alias.as_str());
                    Value::String(raw.to_owned())
                })
                .collect();
            if !raw_types.is_empty() {
                clauses.push(json!({ "terms": { es_field: raw_types } }));
            }
        } else {
            let terms: Vec<Value> = values.iter().map(|v| Value::String(v.clone())).collect();
            clauses.push(json!({ "terms": { es_field: terms } }));
        }
    }

    clauses
}

// ---------------------------------------------------------------------------
// Sort builder
// ---------------------------------------------------------------------------

/// Convert a [`SortSpec`] into an Elasticsearch sort clause array.
///
/// Returns `None` if the field name is not recognised as an ES sort field.
pub(super) fn build_sort_clause(sort: &SortSpec) -> Option<Value> {
    let &es_field = ES_SORT_FIELD_MAP
        .iter()
        .find(|(k, _)| *k == sort.field.as_str())
        .map(|(_, f)| f)?;
    let order = if sort.descending { "desc" } else { "asc" };
    Some(json!([{ es_field: { "order": order, "missing": "_last" } }]))
}

// ---------------------------------------------------------------------------
// Query builders
// ---------------------------------------------------------------------------

/// Build an Elasticsearch bool query for full-text search.
///
/// Matches on `content` (boosted) and `display_path`, with optional filter
/// clauses applied.
pub(super) fn build_text_query(query: &str, filter_clauses: &[Value]) -> Value {
    let should = json!([
        { "match": { "content":      { "query": query, "operator": "and", "boost": 2 } } },
        { "match": { "display_path": { "query": query, "operator": "and" } } },
    ]);
    if filter_clauses.is_empty() {
        json!({
            "bool": {
                "should": should,
                "minimum_should_match": 1
            }
        })
    } else {
        json!({
            "bool": {
                "should": should,
                "minimum_should_match": 1,
                "filter": filter_clauses
            }
        })
    }
}

/// Build the Elasticsearch `knn` body for vector search.
pub(super) fn build_knn_body(vector: &[f32], limit: usize, filter_clauses: &[Value]) -> Value {
    let mut knn = json!({
        "field": "chunks.embedding",
        "query_vector": vector,
        "k": limit,
        "num_candidates": limit * 5,
    });
    if !filter_clauses.is_empty() {
        knn["filter"] = json!({ "bool": { "filter": filter_clauses } });
    }
    knn
}

/// Build the Elasticsearch highlight configuration.
///
/// Uses `<mark>` tags to match the Meilisearch backend output.
pub(super) fn build_highlight(max_analyzed_offset: u64) -> Value {
    json!({
        "pre_tags":  ["<mark>"],
        "post_tags": ["</mark>"],
        "max_analyzed_offset": max_analyzed_offset,
        "fields": {
            "content":      { "fragment_size": 200, "number_of_fragments": 1 },
            "display_path": { "number_of_fragments": 0 }
        }
    })
}

/// Build the Elasticsearch aggregations body for facet counts.
pub(super) fn build_facet_aggs() -> Value {
    let mut aggs = serde_json::Map::new();
    for (label, es_field) in ES_FACET_FIELDS {
        let agg = if DATE_FACETS.contains(label) {
            json!({
                "date_histogram": {
                    "field": es_field,
                    "calendar_interval": "year",
                    "format": "yyyy",
                    "min_doc_count": 1
                }
            })
        } else {
            json!({ "terms": { "field": es_field, "size": 100 } })
        };
        aggs.insert((*label).to_owned(), agg);
    }
    Value::Object(aggs)
}

// ---------------------------------------------------------------------------
// Facet response parser
// ---------------------------------------------------------------------------

/// Parse Elasticsearch aggregation buckets from a search response into a [`FacetMap`].
pub(super) fn parse_facets(resp: &Value) -> FacetMap {
    let mut result = FacetMap::new();

    for (label, _es_field) in ES_FACET_FIELDS {
        let buckets = resp
            .get("aggregations")
            .and_then(|a| a.get(*label))
            .and_then(|agg| agg.get("buckets"))
            .and_then(|b| b.as_array());

        let Some(buckets) = buckets else {
            continue;
        };

        let mut dist: HashMap<String, u64> = HashMap::new();

        for bucket in buckets {
            let count = bucket.get("doc_count").and_then(Value::as_u64).unwrap_or(0);
            if count == 0 {
                continue;
            }
            // Date histogram buckets have "key_as_string"; term buckets have "key".
            let key = if DATE_FACETS.contains(label) {
                bucket
                    .get("key_as_string")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            } else {
                bucket
                    .get("key")
                    .and_then(|v| v.as_str())
                    .map(str::to_owned)
            };
            if let Some(k) = key {
                *dist.entry(k).or_insert(0) += count;
            }
        }

        if !dist.is_empty() {
            result.insert((*label).to_owned(), dist);
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use anyhow::Context as _;
    use serde_json::json;

    use super::*;
    use crate::search::types::SortSpec;

    #[test]
    fn filter_clauses_empty_for_empty_map() {
        let clauses = build_filter_clauses(&FilterMap::new());
        assert!(clauses.is_empty());
    }

    #[test]
    fn filter_clauses_file_type_maps_alias_to_mime() -> anyhow::Result<()> {
        let mut f = FilterMap::new();
        f.insert("File Type".into(), vec!["PDF".into()]);
        let clauses = build_filter_clauses(&f);
        assert_eq!(clauses.len(), 1);
        let terms = clauses[0].get("terms").context("missing terms")?;
        let types = terms
            .get("meta.content_type")
            .context("missing meta.content_type")?
            .as_array()
            .context("not an array")?;
        assert!(types.iter().any(|v| v.as_str() == Some("application/pdf")));
        Ok(())
    }

    #[test]
    fn filter_clauses_date_range() -> anyhow::Result<()> {
        let mut f = FilterMap::new();
        f.insert("Created".into(), vec!["from:2020".into(), "to:2023".into()]);
        let clauses = build_filter_clauses(&f);
        assert_eq!(clauses.len(), 1);
        let range = clauses[0].get("range").context("missing range")?;
        let created = range.get("meta.created").context("missing meta.created")?;
        assert_eq!(created.get("gte").and_then(|v| v.as_str()), Some("2020"));
        assert_eq!(created.get("lte").and_then(|v| v.as_str()), Some("2023"));
        Ok(())
    }

    #[test]
    fn filter_clauses_unknown_label_ignored() {
        let mut f = FilterMap::new();
        f.insert("Unknown".into(), vec!["foo".into()]);
        let clauses = build_filter_clauses(&f);
        assert!(clauses.is_empty());
    }

    #[test]
    fn sort_clause_date_desc() -> anyhow::Result<()> {
        let sort = SortSpec {
            field: "meta_created_year".into(),
            descending: true,
        };
        let clause = build_sort_clause(&sort).context("no sort clause")?;
        let arr = clause.as_array().context("not an array")?;
        let field = arr[0].get("meta.created").context("missing meta.created")?;
        assert_eq!(field.get("order").and_then(|v| v.as_str()), Some("desc"));
        Ok(())
    }

    #[test]
    fn sort_clause_unknown_field_returns_none() {
        let sort = SortSpec {
            field: "unknown_field".into(),
            descending: false,
        };
        assert!(build_sort_clause(&sort).is_none());
    }

    #[test]
    fn parse_facets_date_histogram() -> anyhow::Result<()> {
        let resp = json!({
            "aggregations": {
                "Created": {
                    "buckets": [
                        { "key_as_string": "2023", "doc_count": 10 },
                        { "key_as_string": "2022", "doc_count": 5 },
                    ]
                }
            }
        });
        let facets = parse_facets(&resp);
        let created = facets.get("Created").context("missing Created facet")?;
        assert_eq!(created.get("2023"), Some(&10u64));
        assert_eq!(created.get("2022"), Some(&5u64));
        Ok(())
    }

    #[test]
    fn parse_facets_term_aggregation() -> anyhow::Result<()> {
        let resp = json!({
            "aggregations": {
                "File Type": {
                    "buckets": [
                        { "key": "application/pdf", "doc_count": 3 },
                        { "key": "text/plain",      "doc_count": 0 },
                    ]
                }
            }
        });
        let facets = parse_facets(&resp);
        let file_type = facets.get("File Type").context("missing File Type facet")?;
        assert_eq!(file_type.get("application/pdf"), Some(&3u64));
        // Zero-count buckets are excluded.
        assert!(!file_type.contains_key("text/plain"));
        Ok(())
    }
}
