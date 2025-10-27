"""Performance tests for distributed components."""

import asyncio
import time

import pytest
from src.nodes.cache_node import CacheEntry, LRUCache, MESIState
from src.nodes.queue_node import ConsistentHashRing


@pytest.mark.asyncio
async def test_lru_cache_performance() -> None:
    """Test LRU cache performance."""
    cache = LRUCache(capacity=10000)

    start_time = time.time()
    for i in range(10000):
        entry = CacheEntry(
            key=f"key{i}",
            value=f"value{i}",
            state=MESIState.EXCLUSIVE,
            timestamp=time.time(),
            last_access=time.time(),
        )
        cache.put(f"key{i}", entry)
    put_duration = time.time() - start_time

    start_time = time.time()
    for i in range(10000):
        cache.get(f"key{i}")
    get_duration = time.time() - start_time

    assert put_duration < 1.0
    assert get_duration < 1.0
    assert cache.size() == 10000


@pytest.mark.asyncio
async def test_consistent_hash_performance() -> None:
    """Test consistent hash ring performance."""
    ring = ConsistentHashRing()

    for i in range(10):
        ring.add_node(f"node{i}")

    start_time = time.time()
    for i in range(100000):
        ring.get_node(f"key{i}")
    duration = time.time() - start_time

    assert duration < 2.0


@pytest.mark.asyncio
async def test_lock_acquisition_latency() -> None:
    """Test lock acquisition latency."""
    from src.consensuses.raft import RaftConfig
    from src.nodes.lock_manager import LockManager, LockType

    raft_config = RaftConfig(
        election_timeout_min=50,
        election_timeout_max=100,
        heartbeat_interval=10,
    )

    manager = LockManager(
        node_id="lock1",
        host="localhost",
        port=19001,
        cluster_nodes=["lock1"],
        node_addresses={"lock1": ("localhost", 19001)},
        raft_config=raft_config,
    )

    await manager.start()
    await asyncio.sleep(0.2)

    if manager.raft.is_leader():
        latencies = []
        for i in range(10):
            start = time.time()
            await manager.acquire_lock(f"lock{i}", LockType.EXCLUSIVE)
            latency = time.time() - start
            latencies.append(latency)

        avg_latency = sum(latencies) / len(latencies)
        assert avg_latency < 0.5

    await manager.stop()
