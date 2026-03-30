"""Thread-safe instance pool for distributing work across multiple service endpoints."""

from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generic, Iterator, TypeVar

import structlog

from aum.metrics import (
    POOL_ERRORS,
    POOL_IN_FLIGHT,
    POOL_INSTANCE_HEALTHY,
    POOL_LATENCY,
    POOL_REQUESTS,
)

log = structlog.get_logger()

T = TypeVar("T")

# After this many consecutive failures an instance is marked unhealthy.
_FAILURE_THRESHOLD = 5
# Seconds before an unhealthy instance is retried.
_HEALTH_RETRY_INTERVAL = 60.0


@dataclass
class Instance(Generic[T]):
    """A single service instance with its own concurrency semaphore."""

    url: str
    client: T
    concurrency: int
    semaphore: threading.Semaphore = field(init=False)
    healthy: bool = True
    consecutive_failures: int = 0
    _last_failure_time: float = 0.0

    def __post_init__(self) -> None:
        self.semaphore = threading.Semaphore(self.concurrency)


class InstancePool(Generic[T]):
    """Manages multiple service instances with per-instance concurrency limits.

    Uses round-robin selection among healthy instances.  Each instance has a
    semaphore that enforces its concurrency limit — ``acquire()`` blocks when
    the selected instance is at capacity.

    Tracks consecutive failures and marks instances unhealthy after a
    threshold.  Unhealthy instances are retried after a cooldown period.
    """

    def __init__(
        self,
        instances: list[Instance[T]],
        service_name: str,
        failure_threshold: int = _FAILURE_THRESHOLD,
        health_retry_interval: float = _HEALTH_RETRY_INTERVAL,
    ) -> None:
        if not instances:
            raise ValueError("InstancePool requires at least one instance")
        self._instances = instances
        self._service = service_name
        self._failure_threshold = failure_threshold
        self._health_retry_interval = health_retry_interval
        self._lock = threading.Lock()
        self._index = 0

        # Initialise health gauges.
        for inst in self._instances:
            POOL_INSTANCE_HEALTHY.labels(self._service, inst.url).set(1)
            POOL_IN_FLIGHT.labels(self._service, inst.url).set(0)

    @property
    def total_concurrency(self) -> int:
        """Sum of concurrency limits across all instances."""
        return sum(i.concurrency for i in self._instances)

    @property
    def instances(self) -> list[Instance[T]]:
        return list(self._instances)

    def close(self) -> None:
        """Close all instance clients that have a ``close()`` method."""
        for inst in self._instances:
            if hasattr(inst.client, "close"):
                inst.client.close()

    @contextmanager
    def acquire(self) -> Iterator[T]:
        """Acquire a slot on an instance and yield its client.

        Selects a healthy instance via round-robin, acquires its semaphore,
        and yields the client object.  On success the failure counter is
        reset; on exception it is incremented and the instance may be
        marked unhealthy.
        """
        instance = self._select_instance()
        instance.semaphore.acquire()
        POOL_IN_FLIGHT.labels(self._service, instance.url).inc()
        POOL_REQUESTS.labels(self._service, instance.url).inc()
        start = time.monotonic()
        try:
            yield instance.client
        except Exception as exc:
            POOL_ERRORS.labels(self._service, instance.url, type(exc).__name__).inc()
            with self._lock:
                instance.consecutive_failures += 1
                instance._last_failure_time = time.monotonic()
                if instance.consecutive_failures >= self._failure_threshold and instance.healthy:
                    instance.healthy = False
                    POOL_INSTANCE_HEALTHY.labels(self._service, instance.url).set(0)
                    log.warning(
                        "instance marked unhealthy",
                        service=self._service,
                        url=instance.url,
                        consecutive_failures=instance.consecutive_failures,
                    )
            raise
        else:
            with self._lock:
                if instance.consecutive_failures > 0:
                    instance.consecutive_failures = 0
                if not instance.healthy:
                    instance.healthy = True
                    POOL_INSTANCE_HEALTHY.labels(self._service, instance.url).set(1)
                    log.info("instance recovered", service=self._service, url=instance.url)
        finally:
            POOL_LATENCY.labels(self._service, instance.url).observe(time.monotonic() - start)
            POOL_IN_FLIGHT.labels(self._service, instance.url).dec()
            instance.semaphore.release()

    def _select_instance(self) -> Instance[T]:
        """Round-robin among healthy instances.

        Unhealthy instances that have passed the retry interval are
        eligible for selection.  If no healthy or retryable instances
        exist, falls back to all instances.
        """
        with self._lock:
            now = time.monotonic()
            candidates = [
                i for i in self._instances if i.healthy or (now - i._last_failure_time >= self._health_retry_interval)
            ]
            if not candidates:
                candidates = list(self._instances)
            selected = candidates[self._index % len(candidates)]
            self._index += 1
            return selected
