//! `aum indices` — list all search indices and their document counts.

use futures::future::try_join_all;

use aum_core::search::SearchBackend;

/// # Errors
///
/// Returns an error if the backend cannot be reached.
pub async fn run(backend: &dyn SearchBackend) -> anyhow::Result<()> {
    let names = backend
        .list_indices()
        .await
        .map_err(|e| anyhow::anyhow!("failed to list indices: {e}"))?;

    if names.is_empty() {
        println!("No indices found.");
        return Ok(());
    }

    println!("{:<30}  {:>8}", "INDEX", "DOCS");
    println!("{}", "-".repeat(42));

    let counts = try_join_all(names.iter().map(|name| backend.doc_count(name)))
        .await
        .map_err(|e| anyhow::anyhow!("failed to get doc counts: {e}"))?;
    for (name, count) in names.iter().zip(counts) {
        println!("{name:<30}  {count:>8}");
    }

    Ok(())
}
