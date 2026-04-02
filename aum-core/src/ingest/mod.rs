//! Document ingest pipeline: concurrent extraction, batching, and progress
//! tracking.
//!
//! The pipeline orchestrates directory walking, parallel document extraction
//! via an [`Extractor`](crate::extraction::Extractor) pool, and batched output
//! through a [`BatchSink`].
//!
//! # Entry points
//!
//! Use [`IngestPipeline::run`] for a full ingest, [`IngestPipeline::run_resume`]
//! to continue an interrupted job, or [`IngestPipeline::run_retry`] to reprocess
//! specific failed files.

pub mod display_path;
pub mod doc_id;
pub mod error;
pub mod pipeline;
pub mod sink;
pub mod walker;
pub mod worker;

pub use doc_id::file_doc_id;
pub use error::IngestPipelineError;
pub use pipeline::IngestPipeline;
pub use sink::{BatchSink, ExistenceChecker, NoOpChecker, NullSink};

use std::path::Path;
use std::sync::Arc;

use tracing::warn;

use crate::db::JobTracker;
use crate::extraction::RecordErrorFn;

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

/// Build a [`RecordErrorFn`] that records errors to the tracker asynchronously.
///
/// The returned callback clones its captures and spawns a fire-and-forget task
/// on each invocation.  Errors from the tracker call are logged as warnings.
pub(super) fn make_record_error_fn(tracker: JobTracker, job_id: String) -> RecordErrorFn {
    Arc::new(move |file_path: &Path, error_type: &str, message: &str| {
        let tracker = tracker.clone();
        let job_id = job_id.clone();
        let file_path = file_path.to_owned();
        let error_type = error_type.to_owned();
        let message = message.to_owned();

        // Fire-and-forget: the caller cannot await the DB write.
        tokio::spawn(async move {
            if let Err(e) = tracker
                .record_error(&job_id, &file_path, &error_type, &message)
                .await
            {
                warn!(error = %e, path = %file_path.display(), "failed to record error");
            }
        });
    })
}

// ---------------------------------------------------------------------------
// Shared test helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) mod test_helpers {
    use std::collections::HashSet;
    use std::sync::Arc;

    use crate::db::{JobTracker, test_pool};
    use crate::extraction::Extractor;
    use crate::ingest::sink::ExistenceChecker;
    use crate::pool::{InstanceDesc, InstancePool, InstancePoolConfig};

    pub async fn make_tracker() -> anyhow::Result<JobTracker> {
        use anyhow::Context as _;
        Ok(JobTracker::new(test_pool().await.context("test pool")?))
    }

    pub fn make_pool<E: Extractor + Send + Sync + 'static>(
        extractor: E,
    ) -> anyhow::Result<Arc<InstancePool<E>>> {
        use anyhow::Context as _;
        Ok(Arc::new(
            InstancePool::new(
                vec![InstanceDesc {
                    url: "mock://test".to_owned(),
                    client: extractor,
                    concurrency: 4,
                }],
                InstancePoolConfig::new("test"),
            )
            .context("pool creation")?,
        ))
    }

    /// An [`ExistenceChecker`] that reports a fixed set of IDs as existing.
    pub struct TestChecker(pub HashSet<String>);

    #[async_trait::async_trait]
    impl ExistenceChecker for TestChecker {
        async fn get_existing(&self, doc_ids: &[String]) -> HashSet<String> {
            doc_ids
                .iter()
                .filter(|id| self.0.contains(id.as_str()))
                .cloned()
                .collect()
        }
    }

    pub fn make_temp_tree(files: &[&str]) -> anyhow::Result<tempfile::TempDir> {
        use anyhow::Context as _;
        let dir = tempfile::TempDir::new().context("tempdir")?;
        for name in files {
            let path = dir.path().join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent).context("mkdir")?;
            }
            std::fs::write(&path, "content").context("write")?;
        }
        Ok(dir)
    }
}
