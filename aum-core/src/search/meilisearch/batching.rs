//! Payload-size-aware document batching for Meilisearch indexing.

use serde_json::Value;

use crate::search::types::TruncationRecord;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Maximum JSON payload size for a single Meilisearch indexing request.
pub(super) const MAX_PAYLOAD_BYTES: usize = 95 * 1024 * 1024;

// ---------------------------------------------------------------------------
// Size estimation
// ---------------------------------------------------------------------------

/// Cheaply estimate the JSON-encoded byte size of a Meilisearch document body.
///
/// The estimate is deliberately pessimistic (slightly over-counts) to ensure
/// the actual serialised payload stays below the limit.
pub(super) fn estimate_doc_size(body: &Value) -> usize {
    let Some(obj) = body.as_object() else {
        return 4; // "null"
    };
    // Opening/closing braces + commas between fields.
    let overhead = 2 + obj.len().saturating_sub(1);
    let fields: usize = obj
        .iter()
        .map(|(k, v)| estimate_key_size(k) + estimate_value_size(v))
        .sum();
    overhead + fields
}

fn estimate_key_size(k: &str) -> usize {
    // Quoted key + colon + space.
    k.len() + 4
}

fn estimate_value_size(v: &Value) -> usize {
    match v {
        Value::String(s) => {
            // UTF-8 bytes + 10% for JSON escape overhead + surrounding quotes.
            let bytes = s.len();
            bytes + bytes / 10 + 2
        }
        Value::Array(arr) => {
            let inner: usize = arr.iter().map(estimate_value_size).sum();
            let commas = arr.len().saturating_sub(1);
            inner + commas + 2
        }
        Value::Object(_) => estimate_doc_size(v),
        Value::Bool(_) => 5,    // "false" worst case
        Value::Number(_) => 20, // generous upper bound
        Value::Null => 4,
    }
}

// ---------------------------------------------------------------------------
// Truncation
// ---------------------------------------------------------------------------

/// Truncate the `content` field of a document body if it exceeds `max_bytes`.
///
/// Returns the (possibly modified) body and an optional `TruncationRecord`.
pub(super) fn truncate_oversized(
    mut body: Value,
    max_bytes: usize,
) -> (Value, Option<TruncationRecord>) {
    let size = estimate_doc_size(&body);
    if size <= max_bytes {
        return (body, None);
    }

    // Compute new length before taking mutable borrow.
    let Some((original_chars, new_chars, doc_id)) = measure_truncation(&body, size, max_bytes)
    else {
        return (body, None);
    };

    let truncated: String = body
        .as_object()
        .and_then(|o| o.get("content"))
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .chars()
        .take(new_chars)
        .collect();

    if let Some(obj) = body.as_object_mut() {
        obj.insert("content".into(), Value::String(truncated));
    }

    let record = TruncationRecord {
        doc_id,
        original_chars,
        truncated_chars: new_chars,
    };
    (body, Some(record))
}

/// Extract truncation parameters from a document body.
///
/// Returns `(original_chars, new_chars, doc_id)` or `None` if the body has no
/// string `content` field.
fn measure_truncation(
    body: &Value,
    size: usize,
    max_bytes: usize,
) -> Option<(usize, usize, String)> {
    let obj = body.as_object()?;
    let content = obj.get("content")?.as_str()?;
    let original_chars = content.chars().count();
    // new_chars = original_chars * (max_bytes / size) * 0.9, using u128 to avoid overflow.
    let new_chars = (original_chars as u128 * max_bytes as u128 * 9 / (size as u128 * 10)) as usize;
    let doc_id = obj
        .get("id")
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned();
    Some((original_chars, new_chars, doc_id))
}

// ---------------------------------------------------------------------------
// Sub-batch splitting
// ---------------------------------------------------------------------------

/// Split a list of document bodies into sub-batches that each fit within
/// `max_bytes`.  Documents that are individually oversized are first truncated.
///
/// Returns `(sub_batches, truncations)`.
pub(super) fn split_by_payload_size(
    docs: Vec<Value>,
    max_bytes: usize,
) -> (Vec<Vec<Value>>, Vec<TruncationRecord>) {
    let mut batches: Vec<Vec<Value>> = Vec::new();
    let mut current_batch: Vec<Value> = Vec::new();
    let mut current_size: usize = 2; // opening + closing array brackets
    let mut truncations: Vec<TruncationRecord> = Vec::new();

    for doc in docs {
        let (doc, trunc) = truncate_oversized(doc, max_bytes);
        if let Some(t) = trunc {
            truncations.push(t);
        }

        let doc_size = estimate_doc_size(&doc) + 1; // +1 for comma separator

        if current_size + doc_size > max_bytes && !current_batch.is_empty() {
            batches.push(std::mem::take(&mut current_batch));
            current_size = 2;
        }

        current_size += doc_size;
        current_batch.push(doc);
    }

    if !current_batch.is_empty() {
        batches.push(current_batch);
    }

    (batches, truncations)
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use anyhow::Context as _;
    use serde_json::json;

    use super::*;

    fn make_doc(id: &str, content_len: usize) -> Value {
        json!({
            "id": id,
            "display_path": format!("/docs/{id}.txt"),
            "content": "x".repeat(content_len),
            "has_embeddings": false,
        })
    }

    #[test]
    fn estimate_small_doc_is_nonzero() {
        let doc = make_doc("doc1", 100);
        assert!(estimate_doc_size(&doc) > 100);
    }

    #[test]
    fn truncate_small_doc_unchanged() {
        let doc = make_doc("doc1", 50);
        let (out, record) = truncate_oversized(doc.clone(), MAX_PAYLOAD_BYTES);
        assert!(record.is_none());
        assert_eq!(out["content"], doc["content"]);
    }

    #[test]
    fn truncate_oversized_doc_shrinks_content() -> anyhow::Result<()> {
        // Manufacture a doc whose content alone exceeds the limit.
        let doc = make_doc("big", MAX_PAYLOAD_BYTES + 100);
        let original_chars = doc["content"].as_str().map_or(0, |s| s.chars().count());
        let (out, record) = truncate_oversized(doc, MAX_PAYLOAD_BYTES);
        let rec = record.context("expected truncation record")?;
        assert_eq!(rec.original_chars, original_chars);
        assert!(rec.truncated_chars < original_chars);
        let new_len = out["content"].as_str().map_or(0, |s| s.chars().count());
        assert_eq!(new_len, rec.truncated_chars);
        Ok(())
    }

    #[test]
    fn split_single_batch_when_small() {
        let docs: Vec<Value> = (0..5).map(|i| make_doc(&format!("doc{i}"), 100)).collect();
        let (batches, truncations) = split_by_payload_size(docs, MAX_PAYLOAD_BYTES);
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].len(), 5);
        assert!(truncations.is_empty());
    }

    #[test]
    fn split_multiple_batches_on_limit() {
        // Each doc is ~1 MB; limit is 2 MB → should produce multiple batches.
        let doc_size = 1024 * 1024;
        let docs: Vec<Value> = (0..5)
            .map(|i| make_doc(&format!("d{i}"), doc_size))
            .collect();
        let limit = 2 * 1024 * 1024;
        let (batches, _) = split_by_payload_size(docs, limit);
        assert!(batches.len() > 1, "expected multiple batches");
        let total: usize = batches.iter().map(Vec::len).sum();
        assert_eq!(total, 5);
    }
}
