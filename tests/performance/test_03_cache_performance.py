from __future__ import annotations

from time import monotonic, perf_counter
from typing import TYPE_CHECKING

from pytest import fixture
from src.nodes.cache_node import CacheLine, CacheStats, LRUCache, MESIState

if TYPE_CHECKING:
    pass


class TestLRUCachePerformance:
    @fixture
    def cache(self) -> LRUCache:
        return LRUCache(capacity=1000)

    def test_put_performance(self, cache: LRUCache) -> None:
        iterations = 10000
        start = perf_counter()
        for i in range(iterations):
            line = CacheLine(
                key=f"key-{i}",
                value=b"value",
                state=MESIState.EXCLUSIVE,
                version=1,
                last_access=monotonic(),
            )
            cache.put(f"key-{i}", line)
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 50000

    def test_get_performance(self) -> None:
        cache = LRUCache(capacity=1000)
        for i in range(1000):
            line = CacheLine(
                key=f"key-{i}",
                value=b"value",
                state=MESIState.EXCLUSIVE,
                version=1,
                last_access=monotonic(),
            )
            cache.put(f"key-{i}", line)
        iterations = 100000
        start = perf_counter()
        for i in range(iterations):
            cache.get(f"key-{i % 1000}")
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 200000

    def test_eviction_performance(self) -> None:
        cache = LRUCache(capacity=100)
        iterations = 10000
        start = perf_counter()
        for i in range(iterations):
            line = CacheLine(
                key=f"key-{i}",
                value=b"value",
                state=MESIState.EXCLUSIVE,
                version=1,
                last_access=monotonic(),
            )
            cache.put(f"key-{i}", line)
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 30000
        assert len(cache) == 100


class TestMESIStateTransitions:
    def test_state_transition_performance(self) -> None:
        line = CacheLine(
            key="test",
            value=b"value",
            state=MESIState.INVALID,
            version=0,
            last_access=0.0,
        )
        iterations = 100000
        states = [
            MESIState.INVALID,
            MESIState.SHARED,
            MESIState.EXCLUSIVE,
            MESIState.MODIFIED,
        ]
        start = perf_counter()
        for i in range(iterations):
            line.state = states[i % 4]
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 500000

    def test_version_increment_performance(self) -> None:
        line = CacheLine(
            key="test",
            value=b"value",
            state=MESIState.MODIFIED,
            version=0,
            last_access=0.0,
        )
        iterations = 100000
        start = perf_counter()
        for _ in range(iterations):
            line.version += 1
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 1000000


class TestCacheLinePerformance:
    def test_cache_line_creation_performance(self) -> None:
        iterations = 10000
        start = perf_counter()
        for i in range(iterations):
            CacheLine(
                key=f"key-{i}",
                value=b"test value",
                state=MESIState.EXCLUSIVE,
                version=1,
                last_access=monotonic(),
            )
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 50000

    def test_value_update_performance(self) -> None:
        line = CacheLine(
            key="test",
            value=b"initial",
            state=MESIState.MODIFIED,
            version=0,
            last_access=0.0,
        )
        iterations = 100000
        new_value = b"updated value"
        start = perf_counter()
        for _ in range(iterations):
            line.value = new_value
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 1000000


class TestCacheStatsPerformance:
    def test_stats_update_performance(self) -> None:
        stats = CacheStats()
        iterations = 100000
        start = perf_counter()
        for _ in range(iterations):
            stats.hits += 1
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 1000000

    def test_hit_rate_calculation_performance(self) -> None:
        stats = CacheStats(hits=8500, misses=1500)
        iterations = 100000
        start = perf_counter()
        for _ in range(iterations):
            total = stats.hits + stats.misses
            _ = stats.hits / total if total > 0 else 0.0
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 500000


class TestCacheKeyPatterns:
    def test_hotspot_access_pattern(self) -> None:
        cache = LRUCache(capacity=100)
        for i in range(100):
            line = CacheLine(
                key=f"key-{i}",
                value=b"value",
                state=MESIState.SHARED,
                version=1,
                last_access=monotonic(),
            )
            cache.put(f"key-{i}", line)
        iterations = 100000
        hotspot_keys = [f"key-{i}" for i in range(10)]
        start = perf_counter()
        for i in range(iterations):
            cache.get(hotspot_keys[i % 10])
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 300000

    def test_scan_access_pattern(self) -> None:
        cache = LRUCache(capacity=100)
        for i in range(100):
            line = CacheLine(
                key=f"key-{i}",
                value=b"value",
                state=MESIState.SHARED,
                version=1,
                last_access=monotonic(),
            )
            cache.put(f"key-{i}", line)
        iterations = 10000
        start = perf_counter()
        for _ in range(iterations):
            for key in cache.keys():
                cache.get(key)
        elapsed = perf_counter() - start
        total_ops = iterations * 100
        ops_per_sec = total_ops / elapsed
        assert ops_per_sec > 500000
