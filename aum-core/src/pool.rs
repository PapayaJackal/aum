//! Async generic instance pool with health-aware round-robin selection.
//!
//! [`InstancePool`] manages multiple service instances (Tika extractors,
//! embedders, etc.) with per-instance concurrency limiting via
//! [`tokio::sync::Semaphore`], automatic health tracking, and cooldown-based
//! retry for unhealthy instances.

use std::future::Future;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use futures::StreamExt;
use futures::stream::BoxStream;

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Default number of consecutive failures before marking an instance unhealthy.
const DEFAULT_FAILURE_THRESHOLD: u32 = 5;

/// Default cooldown before retrying an unhealthy instance.
const DEFAULT_HEALTH_RETRY_INTERVAL: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Error types
// ---------------------------------------------------------------------------

/// Errors originating from the instance pool itself.
#[derive(Debug, thiserror::Error)]
pub enum PoolError {
    /// The pool was constructed with zero instances.
    #[error("InstancePool requires at least one instance")]
    Empty,
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// Descriptor for a single instance to add to the pool.
pub struct InstanceDesc<T> {
    /// URL or identifier for this instance (used in metrics and logs).
    pub url: String,
    /// The client object.
    pub client: T,
    /// Maximum concurrent operations on this instance.
    pub concurrency: u32,
}

/// Tuning parameters for an [`InstancePool`].
pub struct InstancePoolConfig {
    /// Service name used in metric labels and log messages (e.g. `"tika"`).
    pub service: String,
    /// Consecutive failures before marking an instance unhealthy.
    pub failure_threshold: u32,
    /// Duration before an unhealthy instance becomes retryable.
    pub health_retry_interval: Duration,
}

impl InstancePoolConfig {
    /// Create a config with the given service name and default thresholds.
    #[must_use]
    pub fn new(service: impl Into<String>) -> Self {
        Self {
            service: service.into(),
            failure_threshold: DEFAULT_FAILURE_THRESHOLD,
            health_retry_interval: DEFAULT_HEALTH_RETRY_INTERVAL,
        }
    }

    /// Override the failure threshold.
    #[must_use]
    pub fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Override the health retry interval.
    #[must_use]
    pub fn with_health_retry_interval(mut self, interval: Duration) -> Self {
        self.health_retry_interval = interval;
        self
    }
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

/// Per-instance health state, protected by a [`std::sync::Mutex`].
///
/// We use `std::sync::Mutex` rather than `tokio::sync::Mutex` because the
/// critical section never contains an `.await` point.
struct HealthState {
    healthy: bool,
    consecutive_failures: u32,
    last_failure_time: Option<Instant>,
}

/// A single managed instance inside the pool.
struct InstanceState<T> {
    url: String,
    client: T,
    concurrency: u32,
    semaphore: Arc<tokio::sync::Semaphore>,
    health: Mutex<HealthState>,
}

// ---------------------------------------------------------------------------
// InstancePool
// ---------------------------------------------------------------------------

/// A generic, thread-safe pool of service instances with round-robin
/// selection, per-instance concurrency limiting, and health tracking.
///
/// # Type parameters
///
/// `T` is the client type stored in each instance. It must be `Send + Sync`
/// so the pool can be shared across tasks.
pub struct InstancePool<T> {
    instances: Vec<InstanceState<T>>,
    config: InstancePoolConfig,
    index: AtomicUsize,
}

// ---------------------------------------------------------------------------
// Construction
// ---------------------------------------------------------------------------

impl<T: Send + Sync> InstancePool<T> {
    /// Create a new pool from instance descriptors.
    ///
    /// # Errors
    ///
    /// Returns [`PoolError::Empty`] if `instances` is empty.
    pub fn new(
        instances: Vec<InstanceDesc<T>>,
        config: InstancePoolConfig,
    ) -> Result<Self, PoolError> {
        if instances.is_empty() {
            return Err(PoolError::Empty);
        }

        let states: Vec<InstanceState<T>> = instances
            .into_iter()
            .map(|desc| {
                metrics::gauge!(
                    "aum_pool_instance_healthy",
                    "service" => config.service.clone(),
                    "instance" => desc.url.clone(),
                )
                .set(1.0);

                metrics::gauge!(
                    "aum_pool_in_flight",
                    "service" => config.service.clone(),
                    "instance" => desc.url.clone(),
                )
                .set(0.0);

                InstanceState {
                    url: desc.url,
                    concurrency: desc.concurrency,
                    semaphore: Arc::new(tokio::sync::Semaphore::new(desc.concurrency as usize)),
                    client: desc.client,
                    health: Mutex::new(HealthState {
                        healthy: true,
                        consecutive_failures: 0,
                        last_failure_time: None,
                    }),
                }
            })
            .collect();

        Ok(Self {
            instances: states,
            config,
            index: AtomicUsize::new(0),
        })
    }

    /// Sum of concurrency limits across all instances.
    #[must_use]
    pub fn total_concurrency(&self) -> u32 {
        self.instances.iter().map(|i| i.concurrency).sum()
    }

    /// Number of instances in the pool.
    #[must_use]
    pub fn len(&self) -> usize {
        self.instances.len()
    }

    /// Whether the pool is empty (always `false` for a valid pool).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.instances.is_empty()
    }
}

// ---------------------------------------------------------------------------
// Instance selection
// ---------------------------------------------------------------------------

impl<T: Send + Sync> InstancePool<T> {
    /// Select the next instance using health-aware round-robin.
    ///
    /// Prefers healthy instances and unhealthy instances whose cooldown has
    /// elapsed. Falls back to all instances when none qualify.
    ///
    /// Eligibility is snapshotted in a single pass so each health mutex is
    /// locked at most once per call, halving lock acquisitions compared to
    /// the previous two-pass approach.
    fn select_instance(&self) -> &InstanceState<T> {
        let now = Instant::now();
        let retry_interval = self.config.health_retry_interval;

        // Single pass: lock each health mutex once, record eligibility.
        let mut eligible_indices: Vec<usize> = Vec::with_capacity(self.instances.len());
        for (i, inst) in self.instances.iter().enumerate() {
            let health = inst
                .health
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if health.healthy
                || health
                    .last_failure_time
                    .is_some_and(|t| now.duration_since(t) >= retry_interval)
            {
                eligible_indices.push(i);
            }
        }

        let idx = self.index.fetch_add(1, Ordering::Relaxed);

        if eligible_indices.is_empty() {
            // All instances unhealthy — fall back to round-robin over all.
            &self.instances[idx % self.instances.len()]
        } else {
            &self.instances[eligible_indices[idx % eligible_indices.len()]]
        }
    }
}

// ---------------------------------------------------------------------------
// Health tracking
// ---------------------------------------------------------------------------

impl<T: Send + Sync> InstancePool<T> {
    /// Record a successful operation on an instance.
    fn record_success(&self, instance: &InstanceState<T>) {
        let mut health = instance
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);

        health.consecutive_failures = 0;

        if !health.healthy {
            health.healthy = true;
            drop(health);

            metrics::gauge!(
                "aum_pool_instance_healthy",
                "service" => self.config.service.clone(),
                "instance" => instance.url.clone(),
            )
            .set(1.0);

            tracing::info!(
                service = %self.config.service,
                instance = %instance.url,
                "instance recovered",
            );
        }
    }

    /// Record a failed operation on an instance.
    fn record_failure(&self, instance: &InstanceState<T>, error_display: &str) {
        let error_type = truncate_error_label(error_display);

        metrics::counter!(
            "aum_pool_errors_total",
            "service" => self.config.service.clone(),
            "instance" => instance.url.clone(),
            "error_type" => error_type.to_owned(),
        )
        .increment(1);

        let mut health = instance
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        health.consecutive_failures += 1;
        health.last_failure_time = Some(Instant::now());

        if health.consecutive_failures >= self.config.failure_threshold && health.healthy {
            health.healthy = false;
            let failures = health.consecutive_failures;
            drop(health);

            metrics::gauge!(
                "aum_pool_instance_healthy",
                "service" => self.config.service.clone(),
                "instance" => instance.url.clone(),
            )
            .set(0.0);

            tracing::warn!(
                service = %self.config.service,
                instance = %instance.url,
                consecutive_failures = failures,
                "instance marked unhealthy",
            );
        }
    }

    /// Emit the per-request metrics that bracket every pool operation.
    fn emit_acquire_metrics(&self, instance: &InstanceState<T>) {
        metrics::counter!(
            "aum_pool_requests_total",
            "service" => self.config.service.clone(),
            "instance" => instance.url.clone(),
        )
        .increment(1);

        metrics::gauge!(
            "aum_pool_in_flight",
            "service" => self.config.service.clone(),
            "instance" => instance.url.clone(),
        )
        .increment(1.0);
    }

    /// Emit the completion metrics after a pool operation.
    fn emit_release_metrics(&self, instance: &InstanceState<T>, elapsed: Duration) {
        metrics::histogram!(
            "aum_pool_duration_seconds",
            "service" => self.config.service.clone(),
            "instance" => instance.url.clone(),
        )
        .record(elapsed.as_secs_f64());

        metrics::gauge!(
            "aum_pool_in_flight",
            "service" => self.config.service.clone(),
            "instance" => instance.url.clone(),
        )
        .decrement(1.0);
    }
}

// ---------------------------------------------------------------------------
// Pool operations
// ---------------------------------------------------------------------------

impl<T: Send + Sync> InstancePool<T> {
    /// Execute an async operation on a pool-selected instance.
    ///
    /// Acquires a concurrency permit, selects an instance via health-aware
    /// round-robin, runs the closure, and automatically handles health
    /// tracking and metrics based on the [`Result`].
    ///
    /// The closure receives a reference to the instance's client. The pool
    /// records success or failure, updates health state, and emits metrics
    /// before returning the closure's result unchanged.
    ///
    /// # Errors
    ///
    /// Returns `Err` if the closure returns `Err`. The pool does not produce
    /// errors of its own from this method.
    pub async fn run<F, Fut, R, E>(&self, f: F) -> Result<R, E>
    where
        F: FnOnce(&T) -> Fut,
        Fut: Future<Output = Result<R, E>>,
        E: std::fmt::Display,
    {
        let instance = self.select_instance();

        // Safety: the semaphore is never closed (we own it for the pool's
        // lifetime), so `acquire_owned` always succeeds.
        let _permit = instance.semaphore.clone().acquire_owned().await.ok();

        self.emit_acquire_metrics(instance);
        let start = Instant::now();

        let result = f(&instance.client).await;

        self.emit_release_metrics(instance, start.elapsed());

        match &result {
            Ok(_) => self.record_success(instance),
            Err(e) => self.record_failure(instance, &e.to_string()),
        }

        result
    }

    /// Execute a streaming operation on a pool-selected instance.
    ///
    /// Like [`run`](Self::run), but for operations that return a
    /// [`BoxStream`]. The concurrency permit is held for the lifetime of the
    /// returned stream, ensuring backpressure is propagated to the pool.
    ///
    /// Health is recorded when the stream completes: if any `Err` item was
    /// yielded the operation counts as a failure, otherwise as a success.
    pub fn run_stream<'a, F, S, R, E>(&'a self, f: F) -> BoxStream<'a, Result<R, E>>
    where
        F: FnOnce(&'a T) -> S + Send + 'a,
        S: futures::Stream<Item = Result<R, E>> + Send + 'a,
        R: Send + 'a,
        E: std::fmt::Display + Send + 'a,
    {
        let stream = async_stream::stream! {
            let instance = self.select_instance();

            // Safety: the semaphore is never closed, so acquire always succeeds.
            let permit = instance.semaphore.clone().acquire_owned().await.ok();

            self.emit_acquire_metrics(instance);
            let start = Instant::now();

            let mut had_error = false;
            let mut last_error_display: Option<String> = None;

            let inner = f(&instance.client);
            futures::pin_mut!(inner);

            while let Some(item) = inner.next().await {
                if let Err(ref e) = item {
                    had_error = true;
                    last_error_display = Some(e.to_string());
                }
                yield item;
            }

            self.emit_release_metrics(instance, start.elapsed());

            if had_error {
                if let Some(ref msg) = last_error_display {
                    self.record_failure(instance, msg);
                }
            } else {
                self.record_success(instance);
            }

            drop(permit);
        };

        Box::pin(stream)
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Truncate an error Display string to a short label for metric tags.
fn truncate_error_label(display: &str) -> &str {
    let s = display.trim();
    // Take up to the first colon, newline, or 64 chars — whichever is shortest.
    let end = s
        .find(':')
        .unwrap_or(s.len())
        .min(s.find('\n').unwrap_or(s.len()))
        .min(64);
    &s[..end]
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::atomic::AtomicU32;

    use anyhow::Context as _;
    use futures::StreamExt;

    // -----------------------------------------------------------------------
    // Mock client
    // -----------------------------------------------------------------------

    /// Trivial client type for testing. Holds a label to identify which
    /// instance was selected.
    #[derive(Debug, Clone)]
    struct MockClient {
        label: String,
    }

    impl MockClient {
        fn new(label: &str) -> Self {
            Self {
                label: label.to_owned(),
            }
        }
    }

    fn desc(label: &str, concurrency: u32) -> InstanceDesc<MockClient> {
        InstanceDesc {
            url: format!("http://{label}:9998"),
            client: MockClient::new(label),
            concurrency,
        }
    }

    fn test_config(service: &str) -> InstancePoolConfig {
        InstancePoolConfig::new(service)
            .with_failure_threshold(3)
            .with_health_retry_interval(Duration::from_millis(50))
    }

    // -----------------------------------------------------------------------
    // Construction tests
    // -----------------------------------------------------------------------

    #[test]
    fn empty_instances_returns_error() {
        let result = InstancePool::<MockClient>::new(vec![], test_config("test"));
        assert!(result.is_err());
    }

    #[test]
    fn single_instance() -> anyhow::Result<()> {
        let pool = InstancePool::new(vec![desc("a", 3)], test_config("test"))?;
        assert_eq!(pool.len(), 1);
        assert_eq!(pool.total_concurrency(), 3);
        assert!(!pool.is_empty());
        Ok(())
    }

    #[test]
    fn multiple_instances_sum_concurrency() -> anyhow::Result<()> {
        let pool = InstancePool::new(
            vec![desc("a", 2), desc("b", 4), desc("c", 1)],
            test_config("test"),
        )?;
        assert_eq!(pool.len(), 3);
        assert_eq!(pool.total_concurrency(), 7);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Round-robin tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn round_robin_distributes_across_instances() -> anyhow::Result<()> {
        let pool = InstancePool::new(
            vec![desc("a", 4), desc("b", 4), desc("c", 4)],
            test_config("test"),
        )?;

        let mut counts = [0u32; 3];
        for _ in 0..9 {
            let idx = pool
                .run(|client| {
                    let label = client.label.clone();
                    async move {
                        let idx = match label.as_str() {
                            "a" => 0,
                            "b" => 1,
                            "c" => 2,
                            _ => unreachable!(),
                        };
                        Ok::<usize, String>(idx)
                    }
                })
                .await
                .map_err(anyhow::Error::msg)?;
            counts[idx] += 1;
        }

        // Each instance should get exactly 3 calls.
        assert_eq!(counts, [3, 3, 3]);
        Ok(())
    }

    #[tokio::test]
    async fn single_instance_always_selected() -> anyhow::Result<()> {
        let pool = InstancePool::new(vec![desc("only", 2)], test_config("test"))?;

        for _ in 0..5 {
            let label = pool
                .run(|client| {
                    let l = client.label.clone();
                    async move { Ok::<_, String>(l) }
                })
                .await
                .map_err(anyhow::Error::msg)?;
            assert_eq!(label, "only");
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Health tracking tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn success_resets_failure_count() -> anyhow::Result<()> {
        let pool = InstancePool::new(vec![desc("a", 4)], test_config("test"))?;

        // Fail twice (below threshold of 3).
        for _ in 0..2 {
            let _ = pool
                .run(|_| async { Err::<(), String>("boom".into()) })
                .await;
        }

        // Succeed once.
        pool.run(|_| async { Ok::<_, String>(()) })
            .await
            .map_err(anyhow::Error::msg)?;

        let health = pool.instances[0]
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(health.healthy);
        assert_eq!(health.consecutive_failures, 0);
        Ok(())
    }

    #[tokio::test]
    async fn failure_threshold_marks_unhealthy() -> anyhow::Result<()> {
        let pool = InstancePool::new(vec![desc("a", 4)], test_config("test"))?;

        for _ in 0..3 {
            let _ = pool
                .run(|_| async { Err::<(), String>("boom".into()) })
                .await;
        }

        let health = pool.instances[0]
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!health.healthy);
        assert_eq!(health.consecutive_failures, 3);
        Ok(())
    }

    #[tokio::test]
    async fn success_after_unhealthy_recovers() -> anyhow::Result<()> {
        let pool = InstancePool::new(vec![desc("a", 4)], test_config("test"))?;

        // Drive to unhealthy.
        for _ in 0..3 {
            let _ = pool
                .run(|_| async { Err::<(), String>("boom".into()) })
                .await;
        }

        // Succeed — should recover.
        pool.run(|_| async { Ok::<_, String>(()) })
            .await
            .map_err(anyhow::Error::msg)?;

        let health = pool.instances[0]
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(health.healthy);
        assert_eq!(health.consecutive_failures, 0);
        Ok(())
    }

    #[tokio::test]
    async fn unhealthy_instance_skipped() -> anyhow::Result<()> {
        let pool = InstancePool::new(
            vec![desc("a", 4), desc("b", 4)],
            // Use a long retry interval so "a" stays unhealthy during the test.
            InstancePoolConfig::new("test")
                .with_failure_threshold(3)
                .with_health_retry_interval(Duration::from_secs(3600)),
        )?;

        // Mark "a" unhealthy by sending 3 failures to it specifically.
        // Round-robin alternates: idx 0→"a", 1→"b", 2→"a", 3→"b", 4→"a".
        // We need "a" to fail 3 times. Send failures only when we hit "a",
        // and succeed on "b" to keep it healthy.
        pool.index.store(0, Ordering::Relaxed);
        for _ in 0..6 {
            let _ = pool
                .run(|client| {
                    let is_a = client.label == "a";
                    async move {
                        if is_a {
                            Err::<(), String>("boom".into())
                        } else {
                            Ok(())
                        }
                    }
                })
                .await;
        }

        // Verify "a" is unhealthy.
        {
            let health_a = pool.instances[0]
                .health
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(!health_a.healthy, "instance 'a' should be unhealthy");
        }

        // Now all subsequent calls should go to "b" only.
        pool.index.store(0, Ordering::Relaxed);
        for _ in 0..4 {
            let label = pool
                .run(|client| {
                    let l = client.label.clone();
                    async move { Ok::<_, String>(l) }
                })
                .await
                .map_err(anyhow::Error::msg)?;
            assert_eq!(label, "b");
        }
        Ok(())
    }

    #[tokio::test]
    async fn unhealthy_instance_retried_after_cooldown() -> anyhow::Result<()> {
        let pool = InstancePool::new(
            vec![desc("a", 4), desc("b", 4)],
            test_config("test"), // threshold=3, 50ms cooldown
        )?;

        // Mark "a" unhealthy by targeting failures at it.
        pool.index.store(0, Ordering::Relaxed);
        for _ in 0..6 {
            let _ = pool
                .run(|client| {
                    let is_a = client.label == "a";
                    async move {
                        if is_a {
                            Err::<(), String>("boom".into())
                        } else {
                            Ok(())
                        }
                    }
                })
                .await;
        }

        // Wait for cooldown to elapse.
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Now "a" should be eligible again.
        pool.index.store(0, Ordering::Relaxed);
        let mut saw_a = false;
        for _ in 0..4 {
            let label = pool
                .run(|client| {
                    let l = client.label.clone();
                    async move { Ok::<_, String>(l) }
                })
                .await
                .map_err(anyhow::Error::msg)?;
            if label == "a" {
                saw_a = true;
            }
        }
        assert!(saw_a, "expected instance 'a' to be retried after cooldown");
        Ok(())
    }

    #[tokio::test]
    async fn all_unhealthy_falls_back_to_all() -> anyhow::Result<()> {
        let pool = InstancePool::new(
            vec![desc("a", 4), desc("b", 4)],
            InstancePoolConfig::new("test")
                .with_failure_threshold(3)
                .with_health_retry_interval(Duration::from_secs(3600)),
        )?;

        // Mark both unhealthy.
        for _ in 0..6 {
            let _ = pool
                .run(|_| async { Err::<(), String>("boom".into()) })
                .await;
        }

        // Should still be able to select instances (fallback to all).
        let label = pool
            .run(|client| {
                let l = client.label.clone();
                async move { Ok::<_, String>(l) }
            })
            .await
            .map_err(anyhow::Error::msg)?;
        assert!(label == "a" || label == "b");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Concurrency tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn concurrency_limit_enforced() -> anyhow::Result<()> {
        let pool = Arc::new(InstancePool::new(vec![desc("a", 1)], test_config("test"))?);

        let started = Arc::new(AtomicU32::new(0));
        let max_concurrent = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..3 {
            let pool = Arc::clone(&pool);
            let started = Arc::clone(&started);
            let max_concurrent = Arc::clone(&max_concurrent);

            handles.push(tokio::spawn(async move {
                pool.run(|_| {
                    let started = Arc::clone(&started);
                    let max_concurrent = Arc::clone(&max_concurrent);
                    async move {
                        let current = started.fetch_add(1, Ordering::SeqCst) + 1;
                        max_concurrent.fetch_max(current, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(50)).await;
                        started.fetch_sub(1, Ordering::SeqCst);
                        Ok::<_, String>(())
                    }
                })
                .await
            }));
        }

        for h in handles {
            h.await
                .context("task panicked")?
                .map_err(anyhow::Error::msg)?;
        }

        assert_eq!(max_concurrent.load(Ordering::SeqCst), 1);
        Ok(())
    }

    #[tokio::test]
    async fn concurrency_permits_independent_per_instance() -> anyhow::Result<()> {
        let pool = Arc::new(InstancePool::new(
            vec![desc("a", 1), desc("b", 1)],
            test_config("test"),
        )?);

        let started = Arc::new(AtomicU32::new(0));
        let max_concurrent = Arc::new(AtomicU32::new(0));

        let mut handles = Vec::new();
        for _ in 0..2 {
            let pool = Arc::clone(&pool);
            let started = Arc::clone(&started);
            let max_concurrent = Arc::clone(&max_concurrent);

            handles.push(tokio::spawn(async move {
                pool.run(|_| {
                    let started = Arc::clone(&started);
                    let max_concurrent = Arc::clone(&max_concurrent);
                    async move {
                        let current = started.fetch_add(1, Ordering::SeqCst) + 1;
                        max_concurrent.fetch_max(current, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        started.fetch_sub(1, Ordering::SeqCst);
                        Ok::<_, String>(())
                    }
                })
                .await
            }));
        }

        for h in handles {
            h.await
                .context("task panicked")?
                .map_err(anyhow::Error::msg)?;
        }

        // Both should have run in parallel since they're on different instances.
        assert_eq!(max_concurrent.load(Ordering::SeqCst), 2);
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Stream tests
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn run_stream_yields_all_items() -> anyhow::Result<()> {
        let pool = InstancePool::new(vec![desc("a", 4)], test_config("test"))?;

        let items: Vec<Result<i32, String>> = pool
            .run_stream(|_client| futures::stream::iter(vec![Ok(1), Ok(2), Ok(3)]))
            .collect()
            .await;

        assert_eq!(items.len(), 3);
        assert!(items.iter().all(Result::is_ok));
        Ok(())
    }

    #[tokio::test]
    async fn run_stream_records_success_when_no_errors() -> anyhow::Result<()> {
        let pool = InstancePool::new(vec![desc("a", 4)], test_config("test"))?;

        // First fail a couple times to increment the counter.
        for _ in 0..2 {
            let _ = pool
                .run(|_| async { Err::<(), String>("boom".into()) })
                .await;
        }

        // Consume a successful stream.
        let _: Vec<_> = pool
            .run_stream(|_| futures::stream::iter(vec![Ok::<i32, String>(1)]))
            .collect()
            .await;

        let health = pool.instances[0]
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(health.healthy);
        assert_eq!(health.consecutive_failures, 0);
        Ok(())
    }

    #[tokio::test]
    async fn run_stream_records_failure_on_error_item() -> anyhow::Result<()> {
        let pool = InstancePool::new(
            vec![desc("a", 4)],
            test_config("test"), // threshold=3
        )?;

        // Run 3 streams that each yield an error.
        for _ in 0..3 {
            let _: Vec<_> = pool
                .run_stream(|_| {
                    futures::stream::iter(vec![Ok::<i32, String>(1), Err("stream error".into())])
                })
                .collect()
                .await;
        }

        let health = pool.instances[0]
            .health
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(!health.healthy);
        Ok(())
    }

    #[tokio::test]
    async fn run_stream_holds_permit_for_stream_lifetime() -> anyhow::Result<()> {
        let pool = InstancePool::new(vec![desc("a", 1)], test_config("test"))?;

        let mut stream =
            pool.run_stream(|_| futures::stream::iter(vec![Ok::<i32, String>(1), Ok(2), Ok(3)]));

        // Consume first item — permit should now be held.
        let first = stream.next().await;
        assert!(first.is_some());

        let available = pool.instances[0].semaphore.available_permits();
        assert_eq!(available, 0, "permit should be held while stream is alive");

        // Drain remaining items.
        while stream.next().await.is_some() {}

        // Drop the stream explicitly so the permit is released.
        drop(stream);

        // Give a moment for async cleanup.
        tokio::time::sleep(Duration::from_millis(10)).await;
        let available = pool.instances[0].semaphore.available_permits();
        assert_eq!(available, 1, "permit should be released after stream ends");
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Concurrent stress test
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn concurrent_health_updates_are_safe() -> anyhow::Result<()> {
        let pool = Arc::new(InstancePool::new(
            vec![desc("a", 8), desc("b", 8)],
            test_config("test"),
        )?);

        let mut handles = Vec::new();
        for i in 0..32 {
            let pool = Arc::clone(&pool);
            handles.push(tokio::spawn(async move {
                if i % 3 == 0 {
                    let _ = pool
                        .run(|_| async { Err::<(), String>("fail".into()) })
                        .await;
                } else {
                    // Succeeding tasks: ignore the result (it always succeeds
                    // for a healthy pool, but we don't want to panic here).
                    let _ = pool.run(|_| async { Ok::<_, String>(()) }).await;
                }
            }));
        }

        for h in handles {
            h.await.context("task panicked")?;
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Truncate error label
    // -----------------------------------------------------------------------

    #[test]
    fn truncate_error_label_at_colon() {
        assert_eq!(
            truncate_error_label("connection failed: timeout"),
            "connection failed"
        );
    }

    #[test]
    fn truncate_error_label_at_newline() {
        assert_eq!(truncate_error_label("oops\ndetails"), "oops");
    }

    #[test]
    fn truncate_error_label_at_max_length() {
        let long = "a".repeat(100);
        assert_eq!(truncate_error_label(&long).len(), 64);
    }

    #[test]
    fn truncate_error_label_short_string() {
        assert_eq!(truncate_error_label("boom"), "boom");
    }
}
