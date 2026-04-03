//! Shared display formatting helpers for CLI output.

use aum_core::models::{IngestJob, JobStatus, JobType};

// ---------------------------------------------------------------------------
// Job summary table
// ---------------------------------------------------------------------------

/// Print a summary line after an ingest/retry/resume job completes.
pub fn print_job_summary(job: &IngestJob) {
    let elapsed = job.finished_at.map_or_else(
        || "-".to_owned(),
        |end| format!("{}s", (end - job.created_at).num_seconds()),
    );

    println!();
    println!("Job:       {}", job.job_id);
    println!("Status:    {}", format_status(job.status));
    println!("Index:     {}", job.index_name);
    println!("Files:     {}", job.total_files);
    println!("Indexed:   {}", job.processed);
    println!("Skipped:   {}", job.skipped);
    println!("Empty:     {}", job.empty);
    println!("Failed:    {}", job.failed);
    println!("Elapsed:   {elapsed}");
}

// ---------------------------------------------------------------------------
// Jobs table (list)
// ---------------------------------------------------------------------------

/// Print a table of jobs from `list_jobs`.
pub fn print_jobs_table(jobs: &[IngestJob]) {
    if jobs.is_empty() {
        println!("No jobs found.");
        return;
    }

    println!(
        "{:<30}  {:<7}  {:<20}  {:<12}  {:>7}  {:>7}  {:>7}  {:>7}  CREATED",
        "JOB ID", "TYPE", "INDEX", "STATUS", "FILES", "OK", "EMPTY", "FAILED"
    );
    println!("{}", "-".repeat(130));

    for job in jobs {
        println!(
            "{:<30}  {:<7}  {:<20}  {:<12}  {:>7}  {:>7}  {:>7}  {:>7}  {}",
            job.job_id,
            format_type(job.job_type),
            truncate(&job.index_name, 20),
            format_status(job.status),
            job.total_files,
            job.processed,
            job.empty,
            job.failed,
            job.created_at.format("%Y-%m-%d %H:%M"),
        );
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

pub fn format_status(status: JobStatus) -> &'static str {
    match status {
        JobStatus::Pending => "pending",
        JobStatus::Running => "running",
        JobStatus::Completed => "completed",
        JobStatus::Failed => "failed",
        JobStatus::Interrupted => "interrupted",
    }
}

pub fn format_type(t: JobType) -> &'static str {
    match t {
        JobType::Ingest => "ingest",
        JobType::Embed => "embed",
    }
}

/// Truncate a string to at most `max` characters, appending `…` if truncated.
pub fn truncate(s: &str, max: usize) -> String {
    let mut iter = s.char_indices();
    let trunc_start = iter.nth(max.saturating_sub(1)).map(|(i, _)| i);
    match (trunc_start, iter.next()) {
        (Some(i), Some(_)) => format!("{}…", &s[..i]),
        _ => s.to_owned(),
    }
}

/// Strip `<mark>` / `</mark>` HTML tags from a string (Meilisearch highlights).
pub fn strip_highlights(s: &str) -> String {
    let mut result = String::with_capacity(s.len());
    let mut rem = s;
    while let Some(pos) = rem.find('<') {
        result.push_str(&rem[..pos]);
        let tail = &rem[pos..];
        if let Some(stripped) = tail.strip_prefix("<mark>") {
            rem = stripped;
        } else if let Some(stripped) = tail.strip_prefix("</mark>") {
            rem = stripped;
        } else {
            result.push('<');
            rem = &tail[1..];
        }
    }
    result.push_str(rem);
    result
}

/// Format a byte count (from Tika's `Content-Length` string) as a human-readable size.
///
/// Returns `None` if the string cannot be parsed as a number.
#[must_use]
#[allow(clippy::cast_precision_loss)]
pub fn format_file_size(raw: &str) -> Option<String> {
    let bytes: u64 = raw.trim().parse().ok()?;
    let s = if bytes < 1_024 {
        format!("{bytes} B")
    } else if bytes < 1_024 * 1_024 {
        format!("{:.1} KB", bytes as f64 / 1_024.0)
    } else if bytes < 1_024 * 1_024 * 1_024 {
        format!("{:.1} MB", bytes as f64 / (1_024.0 * 1_024.0))
    } else {
        format!("{:.1} GB", bytes as f64 / (1_024.0 * 1_024.0 * 1_024.0))
    };
    Some(s)
}

/// Return a short human-readable label for a MIME content type.
///
/// Falls back to the raw MIME string when there is no known mapping.
#[must_use]
pub fn format_content_type(mime: &str) -> &str {
    match mime {
        "application/pdf" => "PDF",
        "application/msword"
        | "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => "Word",
        "application/vnd.ms-excel"
        | "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => "Excel",
        "application/vnd.ms-powerpoint"
        | "application/vnd.openxmlformats-officedocument.presentationml.presentation" => {
            "PowerPoint"
        }
        "text/plain" => "Text",
        "text/html" => "HTML",
        "text/csv" => "CSV",
        "text/markdown" => "Markdown",
        "application/rtf" => "RTF",
        "application/zip" => "ZIP",
        "application/json" => "JSON",
        "application/xml" | "text/xml" => "XML",
        "image/jpeg" => "JPEG",
        "image/png" => "PNG",
        "image/gif" => "GIF",
        "image/tiff" => "TIFF",
        "audio/mpeg" | "audio/mp3" => "MP3",
        "video/mp4" => "MP4",
        "message/rfc822" => "Email",
        "application/mbox" => "Mbox",
        other => other,
    }
}

/// Truncate a snippet to at most `max` chars, appending `…` if truncated.
pub fn truncate_snippet(s: &str, max: usize) -> String {
    let stripped = strip_highlights(s);
    if stripped.len() <= max {
        stripped
    } else {
        // Find the last char boundary at or before `max - 1`.
        let mut end = max - 1;
        while !stripped.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &stripped[..end])
    }
}
