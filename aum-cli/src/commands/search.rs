//! `aum search <INDEX> <QUERY>` — search documents in an index.

use clap::Args;
use futures::TryStreamExt as _;
use owo_colors::OwoColorize as _;

use aum_core::search::SearchBackend;
use aum_core::search::constants::{
    FACET_CREATED, FACET_CREATOR, FACET_EMAIL_ADDRESSES, FACET_FILE_TYPE,
};
use aum_core::search::types::{FilterMap, SearchRequest, SearchResult, SortSpec};

use crate::output::{format_content_type, format_file_size, truncate_snippet};

#[derive(Args)]
pub struct SearchArgs {
    /// Index name(s) to search (comma-separated for multi-index).
    pub index: String,
    /// Search query.
    pub query: String,
    /// Maximum number of results to return.
    #[arg(long, default_value_t = 20)]
    pub limit: usize,
    /// Number of results to skip (for pagination).
    #[arg(long, default_value_t = 0)]
    pub offset: usize,
    /// Filter by file type (can be repeated, e.g. --file-type PDF).
    #[arg(long = "file-type")]
    pub file_types: Vec<String>,
    /// Filter by creator/author (can be repeated).
    #[arg(long)]
    pub creator: Vec<String>,
    /// Filter by email address (can be repeated).
    #[arg(long)]
    pub email: Vec<String>,
    /// Filter results created from this year (inclusive, e.g. 2020).
    #[arg(long)]
    pub created_from: Option<String>,
    /// Filter results created up to this year (inclusive, e.g. 2023).
    #[arg(long)]
    pub created_to: Option<String>,
    /// Sort by field: date:asc, date:desc, size:asc, size:desc.
    #[arg(long)]
    pub sort: Option<String>,
    /// Display available facet values.
    #[arg(long)]
    pub show_facets: bool,
}

/// # Errors
///
/// Returns an error if the backend query fails.
pub async fn run(args: &SearchArgs, backend: &dyn SearchBackend) -> anyhow::Result<()> {
    let indices: Vec<String> = args
        .index
        .split(',')
        .map(|s| s.trim().to_owned())
        .filter(|s| !s.is_empty())
        .collect();

    let mut filters = FilterMap::new();
    if !args.file_types.is_empty() {
        filters.insert(FACET_FILE_TYPE.to_owned(), args.file_types.clone());
    }
    if !args.creator.is_empty() {
        filters.insert(FACET_CREATOR.to_owned(), args.creator.clone());
    }
    if !args.email.is_empty() {
        filters.insert(FACET_EMAIL_ADDRESSES.to_owned(), args.email.clone());
    }
    let year_filter = build_year_filter(args.created_from.as_deref(), args.created_to.as_deref());
    if !year_filter.is_empty() {
        filters.insert(FACET_CREATED.to_owned(), year_filter);
    }

    let sort = parse_sort(args.sort.as_deref());

    if args.show_facets {
        let (count, facets) = backend
            .count(&indices, Some(&args.query), &filters)
            .await
            .map_err(|e| anyhow::anyhow!("count query failed: {e}"))?;
        println!("{} {count}", "Total:".bold());
        if !facets.is_empty() {
            println!();
            println!("{}", "Facets:".bold());
            let mut sorted_facets: Vec<_> = facets.iter().collect();
            sorted_facets.sort_by_key(|(k, _)| k.as_str());
            for (facet, values) in sorted_facets {
                let mut sorted_values: Vec<_> = values.iter().collect();
                sorted_values.sort_by(|a, b| b.1.cmp(a.1));
                let parts: Vec<String> = sorted_values
                    .iter()
                    .take(10)
                    .map(|(v, c)| format!("{v} ({})", c.to_string().cyan()))
                    .collect();
                println!("  {}: {}", facet.bold(), parts.join(", "));
            }
            println!();
        }
    }

    let request = SearchRequest {
        indices: &indices,
        query: &args.query,
        limit: args.limit,
        offset: args.offset,
        filters: &filters,
        sort,
        include_facets: false,
    };

    let results: Vec<_> = backend
        .search_text(request)
        .try_collect()
        .await
        .map_err(|e| anyhow::anyhow!("search failed: {e}"))?;

    if results.is_empty() {
        println!("No results found.");
        return Ok(());
    }

    for (i, r) in results.iter().enumerate() {
        print_result(args.offset + i + 1, r);
    }

    Ok(())
}

fn print_result(n: usize, r: &SearchResult) {
    let snippet = truncate_snippet(&r.snippet, 200);

    println!(
        "{}  {}  {}",
        format!("{n}.").bold(),
        format!("[{:.3}]", r.score).cyan(),
        r.display_path.bold(),
    );

    let type_label = r
        .metadata
        .get("content_type")
        .and_then(|v| v.as_str())
        .map(format_content_type);
    let size_label = r
        .metadata
        .get("file_size")
        .and_then(|v| v.as_str())
        .and_then(format_file_size);
    let date_label = r
        .metadata
        .get("created")
        .and_then(|v| v.as_str())
        .map(|s| s.get(..10).unwrap_or(s));
    let meta_parts: Vec<String> = [
        type_label.map(str::to_owned),
        size_label,
        date_label.map(str::to_owned),
    ]
    .into_iter()
    .flatten()
    .collect();
    if !meta_parts.is_empty() {
        println!("   {}", meta_parts.join("  ").dimmed());
    }

    let subject = r
        .metadata
        .get("email_subject")
        .and_then(|v| v.as_str())
        .unwrap_or("");
    if !subject.is_empty() {
        println!("   {} {}", "Subject:".dimmed(), subject);
    }
    for (label, key) in [
        ("From:   ", "email_from"),
        ("To:     ", "email_to"),
        ("CC:     ", "email_cc"),
        ("BCC:    ", "email_bcc"),
    ] {
        let vals = json_str_array(r.metadata.get(key));
        if !vals.is_empty() {
            println!("   {} {}", label.dimmed(), vals.join(", "));
        }
    }

    if !snippet.is_empty() {
        println!("   {}", snippet.dimmed());
    }
    println!();
}

/// Extract a list of strings from a JSON value (array or single string).
fn json_str_array(val: Option<&serde_json::Value>) -> Vec<&str> {
    match val {
        Some(serde_json::Value::Array(arr)) => arr.iter().filter_map(|v| v.as_str()).collect(),
        Some(serde_json::Value::String(s)) => vec![s.as_str()],
        _ => vec![],
    }
}

/// Parse a sort string like "date:asc" into a `SortSpec`.
fn parse_sort(s: Option<&str>) -> Option<SortSpec> {
    let s = s?;
    let (field_alias, dir) = s.split_once(':')?;
    let descending = dir == "desc";
    let field = match field_alias {
        "date" => "meta_created_year",
        "size" => "meta_file_size",
        other => other,
    };
    Some(SortSpec {
        field: field.to_owned(),
        descending,
    })
}

/// Build a year range filter list from optional from/to year strings.
fn build_year_filter(from: Option<&str>, to: Option<&str>) -> Vec<String> {
    match (
        from.and_then(|s| s.parse::<i32>().ok()),
        to.and_then(|s| s.parse::<i32>().ok()),
    ) {
        (Some(f), Some(t)) => (f..=t).map(|y| y.to_string()).collect(),
        (Some(f), None) => vec![f.to_string()],
        (None, Some(t)) => vec![t.to_string()],
        (None, None) => vec![],
    }
}
