from __future__ import annotations

from asyncio import gather, sleep
from time import perf_counter

from pytest import fixture, mark
from src.nodes.lock_manager import (
    DeadlockDetector,
    LockInfo,
    LockState,
    LockType,
)


class TestDeadlockDetectorPerformance:
    @fixture
    def detector(self) -> DeadlockDetector:
        return DeadlockDetector()

    def test_add_wait_performance(self, detector: DeadlockDetector) -> None:
        iterations = 1000
        start = perf_counter()
        for i in range(iterations):
            detector.add_wait(f"waiter-{i}", {f"holder-{i}"})
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 10000

    def test_detect_deadlock_no_cycle_performance(
        self,
        detector: DeadlockDetector,
    ) -> None:
        for i in range(100):
            detector.add_wait(f"node-{i}", {f"node-{i + 1}"})
        iterations = 100
        start = perf_counter()
        for i in range(iterations):
            detector.detect_deadlock(f"node-{i % 100}")
        elapsed = perf_counter() - start
        avg_latency_ms = (elapsed / iterations) * 1000
        assert avg_latency_ms < 10

    def test_detect_deadlock_with_cycle_performance(
        self,
        detector: DeadlockDetector,
    ) -> None:
        for i in range(50):
            detector.add_wait(f"node-{i}", {f"node-{(i + 1) % 50}"})
        iterations = 100
        start = perf_counter()
        for _ in range(iterations):
            detector.detect_deadlock("node-0")
        elapsed = perf_counter() - start
        avg_latency_ms = (elapsed / iterations) * 1000
        assert avg_latency_ms < 10


class TestLockStateTransitionPerformance:
    def test_lock_info_creation_performance(self) -> None:
        iterations = 10000
        start = perf_counter()
        for i in range(iterations):
            LockInfo(resource_id=f"resource-{i}")
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 50000

    def test_lock_state_enum_access_performance(self) -> None:
        iterations = 100000
        start = perf_counter()
        for _ in range(iterations):
            _ = LockState.FREE
            _ = LockState.SHARED
            _ = LockState.EXCLUSIVE
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 500000


class TestLockTypePerformance:
    def test_lock_type_comparison_performance(self) -> None:
        iterations = 100000
        shared = LockType.SHARED
        exclusive = LockType.EXCLUSIVE
        start = perf_counter()
        for _ in range(iterations):
            _ = shared == LockType.SHARED
            _ = exclusive == LockType.EXCLUSIVE
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 500000


class TestLockContention:
    @mark.asyncio
    async def test_concurrent_lock_info_updates(self) -> None:
        lock_info = LockInfo(resource_id="test-resource")
        iterations = 1000

        async def simulate_shared_acquire(owner_id: str) -> None:
            lock_info.shared_owners.add(owner_id)
            await sleep(0)
            lock_info.shared_owners.discard(owner_id)

        start = perf_counter()
        tasks = [simulate_shared_acquire(f"owner-{i}") for i in range(iterations)]
        await gather(*tasks)
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 1000


class TestLockMetrics:
    def test_fencing_token_increment_performance(self) -> None:
        lock_info = LockInfo(resource_id="test")
        iterations = 100000
        start = perf_counter()
        for _ in range(iterations):
            lock_info.fencing_token += 1
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 1000000
        assert lock_info.fencing_token == iterations
