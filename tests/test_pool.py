from __future__ import annotations

import threading
import time
from collections import Counter
from unittest.mock import MagicMock

import pytest

from aum.pool import Instance, InstancePool


def _make_instance(url: str = "http://localhost:9998", concurrency: int = 2) -> Instance[str]:
    return Instance(url=url, client=url, concurrency=concurrency)


class TestInstancePool:
    def test_requires_at_least_one_instance(self):
        with pytest.raises(ValueError, match="at least one"):
            InstancePool([], service_name="test")

    def test_total_concurrency(self):
        pool = InstancePool(
            [_make_instance("http://a", 4), _make_instance("http://b", 8)],
            service_name="test",
        )
        assert pool.total_concurrency == 12

    def test_acquire_yields_client(self):
        pool = InstancePool([_make_instance("http://a", 2)], service_name="test")
        with pool.acquire() as client:
            assert client == "http://a"

    def test_round_robin_selection(self):
        pool = InstancePool(
            [_make_instance("http://a", 10), _make_instance("http://b", 10)],
            service_name="test",
        )
        clients = []
        for _ in range(6):
            with pool.acquire() as c:
                clients.append(c)
        # Should alternate between the two instances
        assert clients == ["http://a", "http://b", "http://a", "http://b", "http://a", "http://b"]

    def test_semaphore_limits_concurrency(self):
        pool = InstancePool([_make_instance("http://a", 2)], service_name="test")

        barrier = threading.Barrier(3, timeout=5)
        results: list[bool] = []

        def worker():
            with pool.acquire():
                barrier.wait()
                results.append(True)

        # Two workers should proceed; the third should block.
        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()

        # Third worker should be blocked since concurrency=2
        blocked = threading.Event()
        acquired = threading.Event()

        def blocked_worker():
            blocked.set()
            with pool.acquire():
                acquired.set()

        t3 = threading.Thread(target=blocked_worker)
        t3.start()
        blocked.wait(timeout=2)
        # Give a moment for t3 to attempt acquire
        time.sleep(0.1)
        assert not acquired.is_set(), "Third worker should be blocked"

        # Now let the barrier complete so t1, t2 release
        try:
            barrier.wait(timeout=2)
        except threading.BrokenBarrierError:
            pass
        t1.join(timeout=2)
        t2.join(timeout=2)

        # Now t3 should proceed
        acquired.wait(timeout=2)
        assert acquired.is_set()
        t3.join(timeout=2)

    def test_health_tracking_marks_unhealthy(self):
        pool = InstancePool(
            [_make_instance("http://a", 10)],
            service_name="test",
            failure_threshold=3,
        )

        # Cause 3 consecutive failures on the single instance
        for _ in range(3):
            try:
                with pool.acquire():
                    raise ConnectionError("down")
            except ConnectionError:
                pass

        inst_a = pool.instances[0]
        assert not inst_a.healthy
        assert inst_a.consecutive_failures == 3

    def test_unhealthy_instance_skipped(self):
        pool = InstancePool(
            [_make_instance("http://a", 10), _make_instance("http://b", 10)],
            service_name="test",
            failure_threshold=2,
            health_retry_interval=9999,  # Don't retry unhealthy during this test
        )

        # Mark instance a as unhealthy
        inst_a = pool.instances[0]
        inst_a.healthy = False
        inst_a._last_failure_time = time.monotonic()

        # All acquisitions should go to instance b
        clients = []
        for _ in range(4):
            with pool.acquire() as c:
                clients.append(c)
        assert all(c == "http://b" for c in clients)

    def test_unhealthy_instance_retried_after_cooldown(self):
        pool = InstancePool(
            [_make_instance("http://a", 10), _make_instance("http://b", 10)],
            service_name="test",
            failure_threshold=2,
            health_retry_interval=0.0,  # Immediate retry
        )

        # Mark instance a as unhealthy with old failure time
        inst_a = pool.instances[0]
        inst_a.healthy = False
        inst_a._last_failure_time = time.monotonic() - 1.0

        # Instance a should be eligible again
        clients = set()
        for _ in range(4):
            with pool.acquire() as c:
                clients.add(c)
        assert "http://a" in clients

    def test_success_resets_failure_count(self):
        pool = InstancePool(
            [_make_instance("http://a", 10)],
            service_name="test",
            failure_threshold=5,
        )

        # Cause some failures
        for _ in range(3):
            try:
                with pool.acquire():
                    raise ConnectionError("down")
            except ConnectionError:
                pass

        assert pool.instances[0].consecutive_failures == 3

        # Successful call resets
        with pool.acquire():
            pass
        assert pool.instances[0].consecutive_failures == 0

    def test_success_recovers_unhealthy_instance(self):
        pool = InstancePool(
            [_make_instance("http://a", 10)],
            service_name="test",
            failure_threshold=2,
        )

        inst_a = pool.instances[0]
        inst_a.healthy = False

        with pool.acquire():
            pass

        assert inst_a.healthy

    def test_fallback_to_all_when_all_unhealthy(self):
        pool = InstancePool(
            [_make_instance("http://a", 10), _make_instance("http://b", 10)],
            service_name="test",
            failure_threshold=2,
            health_retry_interval=9999,
        )

        # Mark both unhealthy
        for inst in pool.instances:
            inst.healthy = False
            inst._last_failure_time = time.monotonic()

        # Should still be able to acquire (falls back to all)
        with pool.acquire() as c:
            assert c in ("http://a", "http://b")

    def test_concurrent_distribution(self):
        pool = InstancePool(
            [_make_instance("http://a", 5), _make_instance("http://b", 5)],
            service_name="test",
        )

        usage: Counter[str] = Counter()
        lock = threading.Lock()

        def worker():
            for _ in range(10):
                with pool.acquire() as c:
                    with lock:
                        usage[c] += 1

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=10)

        # Both instances should have been used
        assert "http://a" in usage
        assert "http://b" in usage
        assert usage["http://a"] + usage["http://b"] == 40
