//! Extract HTML body from email (.eml) files for preview rendering.
//!
//! Parses MIME structure, resolves `cid:` references to inline `data:` URIs,
//! and falls back to plain-text wrapped in `<pre>` when no HTML part exists.

use std::path::Path;
use std::sync::LazyLock;

use base64ct::{Base64, Encoding};
use regex::Regex;

use crate::error::ApiError;

/// Compiled regex for matching `cid:` references in HTML email bodies.
static CID_RE: LazyLock<Regex> = LazyLock::new(|| {
    #[allow(clippy::expect_used)]
    Regex::new(r#"cid:([^\s"'><]+)"#).expect("cid regex is valid")
});

/// Return an HTML preview of an `.eml` file as UTF-8 bytes.
///
/// Resolves `cid:` image references to inline `data:` URIs. Falls back to
/// `<pre>`-wrapped plain text when no HTML part exists.
///
/// # Errors
///
/// Returns an [`ApiError::UnprocessableEntity`] if the email cannot be parsed or
/// contains no viewable content.
pub fn extract_email_html(file_path: &Path) -> Result<Vec<u8>, ApiError> {
    let raw = std::fs::read(file_path).map_err(|e| {
        tracing::warn!(path = %file_path.display(), error = %e, "failed to read email file");
        ApiError::NotFound("Source file not found on disk".into())
    })?;

    let parsed = mailparse::parse_mail(&raw).map_err(|e| {
        tracing::warn!(path = %file_path.display(), error = %e, "email parse failed");
        ApiError::UnprocessableEntity("Could not parse email file".into())
    })?;

    // Collect Content-ID → data URI map for inline attachments.
    let mut cid_map = std::collections::HashMap::new();
    let mut html_part: Option<String> = None;
    let mut text_part: Option<String> = None;

    collect_parts(&parsed, &mut cid_map, &mut html_part, &mut text_part);

    if html_part.is_none() && text_part.is_none() {
        return Err(ApiError::UnprocessableEntity(
            "Could not extract viewable content from email".into(),
        ));
    }

    let result = if let Some(html) = html_part {
        // Rewrite cid: references to data: URIs.
        let replaced = CID_RE.replace_all(&html, |caps: &regex::Captures<'_>| {
            let cid_key = &caps[1];
            cid_map.get(cid_key).cloned().unwrap_or_default()
        });

        // Sanitize HTML to prevent XSS.
        ammonia::clean(&replaced)
    } else {
        // Wrap plain text in minimal HTML.
        let escaped = text_part
            .as_deref()
            .unwrap_or("")
            .replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;");

        format!(
            "<!DOCTYPE html><html><head>\
             <meta charset=\"utf-8\">\
             <style>body{{font-family:monospace;white-space:pre-wrap;padding:1em;}}</style>\
             </head><body><pre>{escaped}</pre></body></html>"
        )
    };

    Ok(result.into_bytes())
}

/// Recursively collect HTML/text parts and Content-ID mappings from a parsed email.
fn collect_parts(
    mail: &mailparse::ParsedMail<'_>,
    cid_map: &mut std::collections::HashMap<String, String>,
    html_part: &mut Option<String>,
    text_part: &mut Option<String>,
) {
    let content_type = mail.ctype.mimetype.to_lowercase();

    // Build CID map entry if this part has a Content-ID header.
    if let Some(cid_header) = mail
        .headers
        .iter()
        .find(|h| h.get_key_ref().eq_ignore_ascii_case("Content-ID"))
    {
        let cid_val = cid_header.get_value();
        let cid_key = cid_val
            .trim()
            .trim_matches(|c| c == '<' || c == '>')
            .to_owned();
        if !cid_key.is_empty()
            && let Ok(body) = mail.get_body_raw()
        {
            let b64 = Base64::encode_string(&body);
            let mime = &mail.ctype.mimetype;
            let data_uri = format!("data:{mime};base64,{b64}");
            cid_map.insert(cid_key, data_uri);
        }
    }

    // Capture first text/html and text/plain parts.
    if content_type == "text/html" && html_part.is_none() {
        if let Ok(body) = mail.get_body() {
            *html_part = Some(body);
        }
    } else if content_type == "text/plain"
        && text_part.is_none()
        && let Ok(body) = mail.get_body()
    {
        *text_part = Some(body);
    }

    // Recurse into sub-parts.
    for sub in &mail.subparts {
        collect_parts(sub, cid_map, html_part, text_part);
    }
}
