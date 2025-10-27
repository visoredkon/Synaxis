"""Integration tests for lock manager."""

import asyncio

import pytest
from src.consensuses.raft import RaftConfig
from src.nodes.lock_manager import LockManager, LockType


@pytest.fixture
def node_addresses() -> dict[str, tuple[str, int]]:
    """Create node address mapping."""
    return {
        "lock1": ("localhost", 9001),
        "lock2": ("localhost", 9002),
        "lock3": ("localhost", 9003),
    }


@pytest.fixture
def raft_config() -> RaftConfig:
    """Create Raft configuration."""
    return RaftConfig(
        election_timeout_min=100,
        election_timeout_max=200,
        heartbeat_interval=25,
    )


@pytest.mark.asyncio
async def test_lock_manager_startup(
    node_addresses: dict[str, tuple[str, int]],
    raft_config: RaftConfig,
) -> None:
    """Test lock manager startup and shutdown."""
    manager = LockManager(
        node_id="lock1",
        host="localhost",
        port=9001,
        cluster_nodes=["lock1", "lock2", "lock3"],
        node_addresses=node_addresses,
        raft_config=raft_config,
    )

    await manager.start()
    assert manager.is_running() is True

    await asyncio.sleep(0.5)

    await manager.stop()
    assert manager.is_running() is False


@pytest.mark.asyncio
async def test_lock_acquisition_basic(
    node_addresses: dict[str, tuple[str, int]],
    raft_config: RaftConfig,
) -> None:
    """Test basic lock acquisition."""
    manager = LockManager(
        node_id="lock1",
        host="localhost",
        port=9001,
        cluster_nodes=["lock1"],
        node_addresses={"lock1": ("localhost", 9001)},
        raft_config=raft_config,
    )

    await manager.start()
    await asyncio.sleep(0.3)

    if manager.raft.is_leader():
        await manager.acquire_lock("test_lock", LockType.EXCLUSIVE)
        assert manager.is_locked("test_lock") is True

    await manager.stop()
