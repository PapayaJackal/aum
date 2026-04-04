//! Shared utility functions for search metadata handling and metrics.

use crate::search::constants::MIMETYPE_ALIASES;

// ---------------------------------------------------------------------------
// Message-ID normalisation
// ---------------------------------------------------------------------------

/// Strip leading/trailing whitespace and angle brackets from an RFC 2822
/// Message-ID value.
///
/// `" <abc@example.com> "` → `"abc@example.com"`
#[must_use]
pub fn normalize_message_id(raw: &str) -> String {
    raw.trim().trim_matches(|c| c == '<' || c == '>').to_owned()
}

// ---------------------------------------------------------------------------
// Email address extraction
// ---------------------------------------------------------------------------

/// Extract the bare email address from an RFC 2822 address string.
///
/// Handles both `"Name <user@example.com>"` and plain `"user@example.com"` forms.
/// Returns `None` if the extracted value contains no `@`.
#[must_use]
pub fn extract_email(raw: &str) -> Option<String> {
    let addr = if let (Some(start), Some(end)) = (raw.find('<'), raw.find('>'))
        && start < end
    {
        raw[start + 1..end].trim()
    } else {
        raw.trim()
    };
    addr.contains('@').then(|| addr.to_owned())
}

// ---------------------------------------------------------------------------
// MIME type alias
// ---------------------------------------------------------------------------

/// Return the human-readable alias for a MIME type, or the raw type unchanged.
#[must_use]
pub fn alias_mimetype(raw: &str) -> &str {
    MIMETYPE_ALIASES.get(raw).copied().unwrap_or(raw)
}

// ---------------------------------------------------------------------------
// JSON helpers
// ---------------------------------------------------------------------------

/// Read a string field from a JSON object, defaulting to an empty string.
pub(super) fn string_field(obj: &serde_json::Map<String, serde_json::Value>, key: &str) -> String {
    obj.get(key)
        .and_then(|v| v.as_str())
        .unwrap_or("")
        .to_owned()
}

// ---------------------------------------------------------------------------
// Search metrics
// ---------------------------------------------------------------------------

/// Record standard search request metrics (counter + latency histogram).
///
/// Both backends emit identical counters/histograms; this shared function
/// ensures the metric names stay in sync.
pub(super) fn record_search_metrics(elapsed: std::time::Duration, success: bool) {
    let status = if success { "ok" } else { "error" };
    metrics::counter!("aum_search_requests_total", "status" => status).increment(1);
    metrics::histogram!("aum_search_latency_seconds").record(elapsed.as_secs_f64());
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_strips_brackets_and_whitespace() {
        assert_eq!(
            normalize_message_id(" <abc@example.com> "),
            "abc@example.com"
        );
        assert_eq!(normalize_message_id("abc@example.com"), "abc@example.com");
        assert_eq!(normalize_message_id("<id>"), "id");
    }

    #[test]
    fn normalize_empty_stays_empty() {
        assert_eq!(normalize_message_id(""), "");
        assert_eq!(normalize_message_id("   "), "");
    }

    #[test]
    fn extract_email_angle_bracket_form() {
        assert_eq!(
            extract_email("Alice <alice@example.com>"),
            Some("alice@example.com".to_owned())
        );
    }

    #[test]
    fn extract_email_bare_form() {
        assert_eq!(
            extract_email("bob@example.com"),
            Some("bob@example.com".to_owned())
        );
    }

    #[test]
    fn extract_email_empty_returns_none() {
        assert_eq!(extract_email(""), None);
        assert_eq!(extract_email("   "), None);
    }

    #[test]
    fn extract_email_empty_brackets_returns_none() {
        assert_eq!(extract_email("<>"), None);
        assert_eq!(extract_email("Name <>"), None);
    }

    #[test]
    fn extract_email_undisclosed_recipients_returns_none() {
        assert_eq!(extract_email("undisclosed-recipients:;"), None);
        assert_eq!(extract_email("undisclosed-recipients: ;"), None);
        assert_eq!(extract_email("Recipients <undisclosed>"), None);
    }

    #[test]
    fn alias_mimetype_known() {
        assert_eq!(alias_mimetype("application/pdf"), "PDF");
        assert_eq!(alias_mimetype("message/rfc822"), "Email");
    }

    #[test]
    fn alias_mimetype_unknown_passthrough() {
        assert_eq!(
            alias_mimetype("application/x-custom"),
            "application/x-custom"
        );
    }
}
