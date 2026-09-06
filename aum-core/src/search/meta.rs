//! Shared metadata extraction for search backends.
//!
//! Both the Meilisearch and `OpenSearch` backends extract the same curated
//! set of fields from raw Tika metadata. This module holds the shared logic
//! so each backend only needs to provide its own document body builder.

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use crate::models::MetadataValue;
use crate::search::utils::{extract_email, normalize_message_id};

// ---------------------------------------------------------------------------
// Source key mappings
// ---------------------------------------------------------------------------

/// Maps canonical field names to lists of candidate Tika metadata keys,
/// tried in priority order (first match wins).
pub(super) static META_SOURCE_KEYS: &[(&str, &[&str])] = &[
    ("content_type", &["Content-Type"]),
    (
        "creator",
        &[
            "dc:creator",
            "xmp:dc:creator",
            "Author",
            "meta:author",
            "creator",
        ],
    ),
    (
        "created",
        &[
            "dcterms:created",
            "Creation-Date",
            "meta:creation-date",
            "created",
            "date",
        ],
    ),
    (
        "modified",
        &[
            "dcterms:modified",
            "Last-Modified",
            "meta:save-date",
            "modified",
        ],
    ),
    ("file_size", &["Content-Length"]),
    ("message_id", &["message:raw-header:Message-ID"]),
    ("in_reply_to", &["message:raw-header:In-Reply-To"]),
    ("references", &["message:raw-header:References"]),
];

/// Email header keys whose values are collected into the deduplicated `email_addresses` facet.
pub(super) static EMAIL_HEADER_KEYS: &[&str] =
    &["message:from", "message:to", "message:cc", "message:bcc"];

static EMAIL_FROM_KEYS: &[&str] = &["message:from"];
static EMAIL_TO_KEYS: &[&str] = &["message:to"];
static EMAIL_CC_KEYS: &[&str] = &["message:cc"];
static EMAIL_BCC_KEYS: &[&str] = &["message:bcc"];

/// Candidate Tika keys for the email subject line.
pub(super) static EMAIL_SUBJECT_KEYS: &[&str] = &["dc:subject", "subject"];

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Retrieve the first matching value for a list of candidate keys.
pub(super) fn first_match<'a, S: std::hash::BuildHasher>(
    metadata: &'a HashMap<String, MetadataValue, S>,
    keys: &[&str],
) -> Option<&'a MetadataValue> {
    keys.iter().find_map(|k| metadata.get(*k))
}

/// Convert a `MetadataValue` to a single string (takes first element of lists).
pub(super) fn as_single_string(val: &MetadataValue) -> Option<String> {
    match val {
        MetadataValue::Single(s) => Some(s.clone()),
        MetadataValue::List(v) => v.first().cloned(),
    }
}

/// Return the document type label based on whether the document was extracted
/// from another file (attachment) or is a top-level parent document.
pub(super) fn document_type_label(extracted_from: &str) -> &'static str {
    use crate::search::constants::{DOC_TYPE_ATTACHMENT, DOC_TYPE_PARENT};
    if extracted_from.is_empty() {
        DOC_TYPE_PARENT
    } else {
        DOC_TYPE_ATTACHMENT
    }
}

/// Convert a `MetadataValue` to a list of strings.
///
/// Borrows the underlying `Vec` for the `List` arm to avoid cloning.
pub(super) fn as_string_list(val: &MetadataValue) -> Cow<'_, [String]> {
    match val {
        MetadataValue::Single(s) => Cow::Owned(vec![s.clone()]),
        MetadataValue::List(v) => Cow::Borrowed(v.as_slice()),
    }
}

// ---------------------------------------------------------------------------
// IndexedMeta
// ---------------------------------------------------------------------------

/// Curated metadata extracted from raw Tika metadata, shared by all backends.
#[derive(Debug, Default)]
pub struct IndexedMeta {
    /// Base MIME type (parameters stripped).
    pub content_type: Option<String>,
    /// Document author or creator.
    pub creator: Option<String>,
    /// Creation date as an ISO 8601 string.
    pub created: Option<String>,
    /// Four-digit year parsed from `created`, used for faceting.
    pub created_year: Option<i64>,
    /// Last-modified date as an ISO 8601 string.
    pub modified: Option<String>,
    /// Raw file size string from Tika (`Content-Length`).
    pub file_size: Option<String>,
    /// Normalised RFC 2822 `Message-ID` header value.
    pub message_id: Option<String>,
    /// Normalised RFC 2822 `In-Reply-To` header value.
    pub in_reply_to: Option<String>,
    /// Normalised RFC 2822 `References` header values.
    pub references: Vec<String>,
    /// Deduplicated email addresses from From/To/CC/BCC headers (for faceting).
    pub email_addresses: Vec<String>,
    /// Raw display values from the email `From` header.
    pub email_from: Vec<String>,
    /// Raw display values from the email `To` header.
    pub email_to: Vec<String>,
    /// Raw display values from the email `Cc` header.
    pub email_cc: Vec<String>,
    /// Raw display values from the email `Bcc` header.
    pub email_bcc: Vec<String>,
    /// Email subject line.
    pub email_subject: Option<String>,
}

/// Extract curated metadata fields from raw Tika metadata.
#[must_use]
pub fn extract_indexed_meta<S: std::hash::BuildHasher>(
    metadata: &HashMap<String, MetadataValue, S>,
) -> IndexedMeta {
    let mut out = IndexedMeta::default();

    for (field, keys) in META_SOURCE_KEYS {
        let Some(val) = first_match(metadata, keys) else {
            continue;
        };
        match *field {
            "content_type" => {
                // Strip MIME type parameters (e.g. "; charset=UTF-8").
                out.content_type = as_single_string(val)
                    .map(|s| s.split(';').next().unwrap_or(&s).trim().to_owned());
            }
            "creator" => out.creator = as_single_string(val),
            "created" => {
                let s = as_single_string(val);
                out.created_year = parse_year(s.as_deref());
                out.created = s;
            }
            "modified" => out.modified = as_single_string(val),
            "file_size" => out.file_size = as_single_string(val),
            "message_id" => {
                out.message_id = as_single_string(val).map(|s| normalize_message_id(&s));
            }
            "in_reply_to" => {
                out.in_reply_to = as_single_string(val).map(|s| normalize_message_id(&s));
            }
            "references" => {
                out.references = as_string_list(val)
                    .iter()
                    .flat_map(|s| s.split_whitespace().map(normalize_message_id))
                    .filter(|s| !s.is_empty())
                    .collect();
            }
            _ => {}
        }
    }

    // Deduplicated email addresses (for faceting).
    let mut seen = HashSet::new();
    for key in EMAIL_HEADER_KEYS {
        let Some(val) = metadata.get(*key) else {
            continue;
        };
        for raw in as_string_list(val).iter() {
            if let Some(addr) = extract_email(raw) {
                let lower = addr.to_lowercase();
                if seen.insert(lower.clone()) {
                    out.email_addresses.push(lower);
                }
            }
        }
    }

    // Per-header display values for rendering.
    out.email_from = first_match(metadata, EMAIL_FROM_KEYS)
        .map(|value| as_string_list(value).into_owned())
        .unwrap_or_default();
    out.email_to = first_match(metadata, EMAIL_TO_KEYS)
        .map(|value| as_string_list(value).into_owned())
        .unwrap_or_default();
    out.email_cc = first_match(metadata, EMAIL_CC_KEYS)
        .map(|value| as_string_list(value).into_owned())
        .unwrap_or_default();
    out.email_bcc = first_match(metadata, EMAIL_BCC_KEYS)
        .map(|value| as_string_list(value).into_owned())
        .unwrap_or_default();

    // Subject line.
    for key in EMAIL_SUBJECT_KEYS {
        if let Some(val) = metadata.get(*key) {
            out.email_subject = as_single_string(val);
            break;
        }
    }

    out
}

/// Parse the four-digit year from a date string (ISO 8601 or similar).
fn parse_year(s: Option<&str>) -> Option<i64> {
    let s = s?;
    s.get(..4)?.parse::<i64>().ok()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::MetadataValue;

    fn meta(pairs: &[(&str, &str)]) -> HashMap<String, MetadataValue> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), MetadataValue::Single(v.to_string())))
            .collect()
    }

    #[test]
    fn extract_creator_from_author_key() {
        let m = meta(&[("Author", "Alice")]);
        let out = extract_indexed_meta(&m);
        assert_eq!(out.creator.as_deref(), Some("Alice"));
    }

    #[test]
    fn extract_created_year() {
        let m = meta(&[("dcterms:created", "2023-06-15T10:00:00Z")]);
        let out = extract_indexed_meta(&m);
        assert_eq!(out.created_year, Some(2023));
    }

    #[test]
    fn content_type_strips_params() {
        let m = meta(&[("Content-Type", "text/html; charset=UTF-8")]);
        let out = extract_indexed_meta(&m);
        assert_eq!(out.content_type.as_deref(), Some("text/html"));
    }

    #[test]
    fn extract_message_id_normalised() {
        let m = meta(&[("message:raw-header:Message-ID", " <abc@example.com> ")]);
        let out = extract_indexed_meta(&m);
        assert_eq!(out.message_id.as_deref(), Some("abc@example.com"));
    }

    #[test]
    fn extract_tika_4_email_fields() {
        let m = meta(&[
            ("message:from", "Alice <alice@example.com>"),
            ("message:to", "Bob <bob@example.com>"),
            ("message:raw-header:Message-ID", "<msg-2@example.com>"),
            ("message:raw-header:In-Reply-To", "<msg-1@example.com>"),
            (
                "message:raw-header:References",
                "<msg-0@example.com> <msg-1@example.com>",
            ),
            ("dc:subject", "Subject"),
        ]);
        let out = extract_indexed_meta(&m);

        assert_eq!(out.message_id.as_deref(), Some("msg-2@example.com"));
        assert_eq!(out.in_reply_to.as_deref(), Some("msg-1@example.com"));
        assert_eq!(out.references, ["msg-0@example.com", "msg-1@example.com"]);
        assert_eq!(out.email_from, ["Alice <alice@example.com>"]);
        assert_eq!(out.email_to, ["Bob <bob@example.com>"]);
        assert_eq!(out.email_subject.as_deref(), Some("Subject"));
        assert_eq!(out.email_addresses.len(), 2);
    }

    #[test]
    fn extract_email_addresses_deduped() {
        let mut md: HashMap<String, MetadataValue> = HashMap::new();
        md.insert(
            "message:from".into(),
            MetadataValue::Single("Alice <alice@example.com>".into()),
        );
        md.insert(
            "message:to".into(),
            MetadataValue::List(vec![
                "bob@example.com".into(),
                "Alice <alice@example.com>".into(),
            ]),
        );
        let out = extract_indexed_meta(&md);
        assert_eq!(out.email_addresses.len(), 2);
        assert!(
            out.email_addresses
                .contains(&"alice@example.com".to_owned())
        );
        assert!(out.email_addresses.contains(&"bob@example.com".to_owned()));
    }

    #[test]
    fn extract_email_addresses_case_insensitive_dedup() {
        let mut md: HashMap<String, MetadataValue> = HashMap::new();
        md.insert(
            "message:from".into(),
            MetadataValue::Single("ALICE@EXAMPLE.COM".into()),
        );
        md.insert(
            "message:to".into(),
            MetadataValue::Single("alice@example.com".into()),
        );
        let out = extract_indexed_meta(&md);
        assert_eq!(out.email_addresses.len(), 1);
        assert_eq!(out.email_addresses[0], "alice@example.com");
    }
}
