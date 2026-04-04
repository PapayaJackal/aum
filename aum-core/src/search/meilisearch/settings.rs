//! Meilisearch index settings constants and initialization helpers.

use std::collections::HashMap;
use std::time::Duration;

use meilisearch_sdk::client::Client;
use meilisearch_sdk::settings::{Embedder, EmbedderSource, PaginationSetting, Settings};
use meilisearch_sdk::task_info::TaskInfo;
use meilisearch_sdk::tasks::Task;

use crate::search::types::SearchError;

// ---------------------------------------------------------------------------
// Index attribute constants
// ---------------------------------------------------------------------------

/// Fields that can be used in filter expressions.
pub(super) static FILTERABLE_ATTRS: &[&str] = &[
    "id",
    "meta_content_type",
    "meta_creator",
    "meta_created_year",
    "meta_file_size",
    "meta_email_addresses",
    "has_embeddings",
    "extracted_from",
    "display_path",
    "meta_message_id",
    "meta_in_reply_to",
    "meta_references",
];

/// Fields that can be used in sort expressions.
pub(super) static SORTABLE_ATTRS: &[&str] = &["id", "meta_created_year", "meta_file_size"];

/// Ranking rules with "sort" first so that explicit sort parameters are the
/// primary ordering criterion.  When no sort is specified in a query,
/// Meilisearch ignores the sort rule and falls through to "words", preserving
/// normal relevance-based ranking for "best match" searches.
pub(super) static RANKING_RULES: &[&str] = &[
    "sort",
    "words",
    "typo",
    "proximity",
    "attribute",
    "exactness",
];

/// Fields included in full-text search (order determines ranking priority).
///
/// `display_path` ranks before `content` so that filename matches are scored
/// higher than body matches.
pub(super) static SEARCHABLE_ATTRS: &[&str] = &["display_path", "content"];

/// Name of the Meilisearch embedder used for hybrid/vector search.
pub(super) const EMBEDDER_NAME: &str = "default";

/// Timeout for general index settings update tasks.
pub(super) const TASK_TIMEOUT: Duration = Duration::from_secs(60);

/// Timeout for embedding update tasks (longer to allow large batch writes).
pub(super) const EMBED_TASK_TIMEOUT: Duration = Duration::from_secs(300);

// ---------------------------------------------------------------------------
// Settings builders
// ---------------------------------------------------------------------------

/// Build the base [`Settings`] object for a Meilisearch index.
pub(super) fn base_settings() -> Settings {
    Settings::new()
        .with_filterable_attributes(FILTERABLE_ATTRS)
        .with_sortable_attributes(SORTABLE_ATTRS)
        .with_searchable_attributes(SEARCHABLE_ATTRS)
        .with_ranking_rules(RANKING_RULES)
        .with_pagination(PaginationSetting {
            max_total_hits: 1_000_000,
        })
}

/// Build the embedder settings map for user-provided vectors.
pub(super) fn embedder_settings(dimension: u32) -> HashMap<String, Embedder> {
    let embedder = Embedder {
        source: EmbedderSource::UserProvided,
        dimensions: Some(dimension as usize),
        ..Default::default()
    };
    HashMap::from([(EMBEDDER_NAME.to_owned(), embedder)])
}

// ---------------------------------------------------------------------------
// Task waiting
// ---------------------------------------------------------------------------

/// Wait for a Meilisearch task to reach a terminal state within `timeout`.
///
/// Returns `Ok(())` on success, `Err(SearchError::TaskFailed)` if the task
/// ends in a failed state, and `Err(SearchError::TaskTimeout)` if it doesn't
/// finish within `timeout`.
pub(super) async fn wait_for_task(
    task_info: TaskInfo,
    client: &Client,
    timeout: Duration,
) -> Result<(), SearchError> {
    let completed = task_info
        .wait_for_completion(client, Some(Duration::from_millis(100)), Some(timeout))
        .await
        .map_err(|_| SearchError::TaskTimeout)?;

    match completed {
        Task::Succeeded { .. } => Ok(()),
        Task::Failed { content } => {
            let error = content.error.error_message.clone();
            Err(SearchError::TaskFailed { error })
        }
        _ => Err(SearchError::TaskTimeout),
    }
}
