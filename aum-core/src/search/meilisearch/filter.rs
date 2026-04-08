//! Meilisearch filter expression builder.

use crate::search::constants::{
    DATE_FACETS, FACET_FIELDS, FACET_FILE_TYPE, REVERSE_MIMETYPE_ALIASES,
};
use crate::search::types::FilterMap;

// ---------------------------------------------------------------------------
// Filter value escaping
// ---------------------------------------------------------------------------

/// Escape a string for use inside a Meilisearch quoted filter value.
///
/// Meilisearch filter strings use `"value"` quoting; embedded double-quotes
/// and backslashes must be backslash-escaped.
pub(super) fn escape_filter_value(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

// ---------------------------------------------------------------------------
// Filter string builder
// ---------------------------------------------------------------------------

/// Convert a [`FilterMap`] (display label → values) into a Meilisearch filter
/// expression string.
///
/// Multiple facets are joined with `AND`; multiple values within a facet are
/// joined with `OR`.  Date facets are emitted as year equality filters.
/// File-type values are reverse-mapped from display aliases back to raw MIME
/// types before comparison.
///
/// Returns `None` if `filters` is empty or contains no non-empty entries.
pub(super) fn build_filter_string(filters: &FilterMap) -> Option<String> {
    let parts: Vec<String> = filters
        .iter()
        .filter_map(|(label, values)| build_facet_filter(label, values))
        .collect();

    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" AND "))
    }
}

/// Build the filter clause for a single facet label.
fn build_facet_filter(label: &str, values: &[String]) -> Option<String> {
    if values.is_empty() {
        return None;
    }
    let field = FACET_FIELDS.get(label).copied()?;

    if DATE_FACETS.contains(label) {
        return build_date_filter(field, values);
    }

    let clauses: Vec<String> = values
        .iter()
        .flat_map(|v| resolve_filter_values(label, v))
        .map(|resolved| format!("{field} = \"{}\"", escape_filter_value(&resolved)))
        .collect();

    wrap_or(clauses)
}

/// Resolve a display value to one or more raw index values.
///
/// For "File Type" facets, reverse-maps the alias (e.g. "Word") to the raw
/// MIME type(s). For all other facets returns the value unchanged.
fn resolve_filter_values(label: &str, value: &str) -> Vec<String> {
    if label == FACET_FILE_TYPE
        && let Some(&raw) = REVERSE_MIMETYPE_ALIASES.get(value)
    {
        return vec![raw.to_owned()];
    }
    vec![value.to_owned()]
}

/// Build a date filter from a list of year strings.
///
/// Each year value is compared with `=`. A special `"from:YYYY"` / `"to:YYYY"`
/// prefix can be used for range bounds.
fn build_date_filter(field: &str, values: &[String]) -> Option<String> {
    let clauses: Vec<String> = values
        .iter()
        .filter_map(|v| parse_date_clause(field, v))
        .collect();

    wrap_or(clauses)
}

/// Parse a single date filter clause from a value string.
///
/// Supported formats:
/// - `"2023"` → `field = 2023`
/// - `"from:2020"` → `field >= 2020`
/// - `"to:2023"` → `field <= 2023`
fn parse_date_clause(field: &str, value: &str) -> Option<String> {
    if let Some(year_str) = value.strip_prefix("from:") {
        let year: i64 = year_str.trim().parse().ok()?;
        Some(format!("{field} >= {year}"))
    } else if let Some(year_str) = value.strip_prefix("to:") {
        let year: i64 = year_str.trim().parse().ok()?;
        Some(format!("{field} <= {year}"))
    } else {
        let year: i64 = value.trim().parse().ok()?;
        Some(format!("{field} = {year}"))
    }
}

/// Wrap a list of clauses in parentheses joined with ` OR `.
///
/// Returns `None` if the list is empty, the single clause string if there is
/// only one, or `(a OR b OR ...)` for multiple clauses.
fn wrap_or(mut clauses: Vec<String>) -> Option<String> {
    match clauses.len() {
        0 => None,
        1 => clauses.pop(),
        _ => Some(format!("({})", clauses.join(" OR "))),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use anyhow::Context as _;

    use super::*;
    use crate::search::constants::{
        DOC_TYPE_PARENT, FACET_CREATED, FACET_DOCUMENT_TYPE, FACET_FILE_TYPE,
    };

    #[test]
    fn escape_double_quotes() {
        assert_eq!(escape_filter_value(r#"say "hello""#), r#"say \"hello\""#);
    }

    #[test]
    fn escape_backslash() {
        assert_eq!(escape_filter_value(r"C:\path"), r"C:\\path");
    }

    #[test]
    fn empty_filters_returns_none() {
        assert!(build_filter_string(&FilterMap::new()).is_none());
    }

    #[test]
    fn single_file_type_filter() -> anyhow::Result<()> {
        let mut f = FilterMap::new();
        f.insert(FACET_FILE_TYPE.into(), vec!["PDF".into()]);
        let result = build_filter_string(&f).context("should produce filter")?;
        assert!(result.contains("meta_content_type"));
        assert!(result.contains("application/pdf"));
        Ok(())
    }

    #[test]
    fn date_filter_exact_year() -> anyhow::Result<()> {
        let mut f = FilterMap::new();
        f.insert(FACET_CREATED.into(), vec!["2023".into()]);
        let result = build_filter_string(&f).context("should produce filter")?;
        assert_eq!(result, "meta_created_year = 2023");
        Ok(())
    }

    #[test]
    fn date_filter_from_to() -> anyhow::Result<()> {
        let mut f = FilterMap::new();
        f.insert(
            FACET_CREATED.into(),
            vec!["from:2020".into(), "to:2023".into()],
        );
        let result = build_filter_string(&f).context("should produce filter")?;
        assert!(result.contains("meta_created_year >= 2020"));
        assert!(result.contains("meta_created_year <= 2023"));
        Ok(())
    }

    #[test]
    fn document_type_filter() -> anyhow::Result<()> {
        let mut f = FilterMap::new();
        f.insert(FACET_DOCUMENT_TYPE.into(), vec![DOC_TYPE_PARENT.into()]);
        let result = build_filter_string(&f).context("should produce filter")?;
        assert!(result.contains("meta_document_type"));
        assert!(result.contains(DOC_TYPE_PARENT));
        Ok(())
    }

    #[test]
    fn unknown_facet_label_ignored() {
        let mut f = FilterMap::new();
        f.insert("Unknown Facet".into(), vec!["foo".into()]);
        assert!(build_filter_string(&f).is_none());
    }
}
