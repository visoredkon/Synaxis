from __future__ import annotations

from src.nodes.cache_node import (
    CacheLine,
    LRUCache,
    MESIState,
)


class TestLRUCache:
    def test_empty_cache(self) -> None:
        cache = LRUCache(capacity=3)
        assert len(cache) == 0
        assert cache.get("nonexistent") is None

    def test_put_and_get(self) -> None:
        cache = LRUCache(capacity=3)
        line = CacheLine(
            key="key1",
            value=b"value1",
            state=MESIState.EXCLUSIVE,
        )
        cache.put("key1", line)
        result = cache.get("key1")
        assert result is not None
        assert result.value == b"value1"

    def test_eviction(self) -> None:
        cache = LRUCache(capacity=2)
        cache.put("k1", CacheLine("k1", b"v1", MESIState.EXCLUSIVE))
        cache.put("k2", CacheLine("k2", b"v2", MESIState.EXCLUSIVE))
        cache.put("k3", CacheLine("k3", b"v3", MESIState.EXCLUSIVE))
        assert cache.get("k1") is None
        assert cache.get("k2") is not None
        assert cache.get("k3") is not None

    def test_lru_order_update(self) -> None:
        cache = LRUCache(capacity=2)
        cache.put("k1", CacheLine("k1", b"v1", MESIState.EXCLUSIVE))
        cache.put("k2", CacheLine("k2", b"v2", MESIState.EXCLUSIVE))
        cache.get("k1")
        cache.put("k3", CacheLine("k3", b"v3", MESIState.EXCLUSIVE))
        assert cache.get("k1") is not None
        assert cache.get("k2") is None
        assert cache.get("k3") is not None

    def test_remove(self) -> None:
        cache = LRUCache(capacity=3)
        cache.put("k1", CacheLine("k1", b"v1", MESIState.EXCLUSIVE))
        removed = cache.remove("k1")
        assert removed is not None
        assert cache.get("k1") is None

    def test_contains(self) -> None:
        cache = LRUCache(capacity=3)
        cache.put("k1", CacheLine("k1", b"v1", MESIState.EXCLUSIVE))
        assert cache.contains("k1")
        assert not cache.contains("k2")


class TestMESIProtocol:
    def test_initial_state_invalid(self) -> None:
        line = CacheLine(
            key="test",
            value=b"data",
            state=MESIState.INVALID,
        )
        assert line.state == MESIState.INVALID

    def test_exclusive_to_shared(self) -> None:
        line = CacheLine(
            key="test",
            value=b"data",
            state=MESIState.EXCLUSIVE,
        )
        line.state = MESIState.SHARED
        assert line.state == MESIState.SHARED

    def test_shared_to_invalid(self) -> None:
        line = CacheLine(
            key="test",
            value=b"data",
            state=MESIState.SHARED,
        )
        line.state = MESIState.INVALID
        assert line.state == MESIState.INVALID

    def test_exclusive_to_modified(self) -> None:
        line = CacheLine(
            key="test",
            value=b"data",
            state=MESIState.EXCLUSIVE,
        )
        line.state = MESIState.MODIFIED
        line.value = b"new_data"
        assert line.state == MESIState.MODIFIED

    def test_state_values(self) -> None:
        assert MESIState.MODIFIED.value == 1
        assert MESIState.EXCLUSIVE.value == 2
        assert MESIState.SHARED.value == 3
        assert MESIState.INVALID.value == 4


class TestCacheStats:
    def test_hit_rate_calculation(self) -> None:
        stats = {"hits": 80, "misses": 20}
        total = stats["hits"] + stats["misses"]
        hit_rate = stats["hits"] / total
        assert hit_rate == 0.8

    def test_empty_stats(self) -> None:
        stats = {"hits": 0, "misses": 0}
        total = stats["hits"] + stats["misses"]
        hit_rate = stats["hits"] / total if total > 0 else 0.0
        assert hit_rate == 0.0
