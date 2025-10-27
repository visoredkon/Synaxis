"""Unit tests for cache node."""

from src.nodes.cache_node import CacheEntry, LRUCache, MESIState


def test_lru_cache_initialization() -> None:
    """Test LRU cache initialization."""
    cache = LRUCache(capacity=3)
    assert cache.capacity == 3
    assert cache.size() == 0


def test_lru_cache_put_get() -> None:
    """Test LRU cache put and get operations."""
    cache = LRUCache(capacity=3)

    entry1 = CacheEntry(
        key="key1",
        value="value1",
        state=MESIState.EXCLUSIVE,
        timestamp=0.0,
        last_access=0.0,
    )

    cache.put("key1", entry1)
    assert cache.size() == 1

    retrieved = cache.get("key1")
    assert retrieved is not None
    assert retrieved.key == "key1"
    assert retrieved.value == "value1"


def test_lru_cache_eviction() -> None:
    """Test LRU cache eviction policy."""
    cache = LRUCache(capacity=2)

    entry1 = CacheEntry("key1", "value1", MESIState.EXCLUSIVE, 0.0, 0.0)
    entry2 = CacheEntry("key2", "value2", MESIState.EXCLUSIVE, 0.0, 0.0)
    entry3 = CacheEntry("key3", "value3", MESIState.EXCLUSIVE, 0.0, 0.0)

    cache.put("key1", entry1)
    cache.put("key2", entry2)

    evicted = cache.put("key3", entry3)

    assert evicted is not None
    assert evicted.key == "key1"
    assert cache.size() == 2
    assert cache.get("key1") is None
    assert cache.get("key2") is not None
    assert cache.get("key3") is not None


def test_cache_entry_validity() -> None:
    """Test cache entry validity check."""
    valid_entry = CacheEntry("key1", "value1", MESIState.SHARED, 0.0, 0.0)
    invalid_entry = CacheEntry("key2", "value2", MESIState.INVALID, 0.0, 0.0)

    assert valid_entry.is_valid() is True
    assert invalid_entry.is_valid() is False


def test_mesi_states() -> None:
    """Test MESI state enumeration."""
    assert MESIState.MODIFIED.value == "modified"
    assert MESIState.EXCLUSIVE.value == "exclusive"
    assert MESIState.SHARED.value == "shared"
    assert MESIState.INVALID.value == "invalid"
