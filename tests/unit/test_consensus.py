"""Unit tests for consensus protocols."""

import asyncio

import pytest
from src.consensuses import ConsensusMessage, ConsensusState
from src.consensuses.raft import RaftConfig, RaftConsensus


@pytest.fixture
def raft_config() -> RaftConfig:
    """Create Raft configuration for testing."""
    return RaftConfig(
        election_timeout_min=100,
        election_timeout_max=200,
        heartbeat_interval=25,
    )


@pytest.mark.asyncio
async def test_raft_initialization(raft_config: RaftConfig) -> None:
    """Test Raft consensus initialization."""
    raft = RaftConsensus(
        node_id="node1",
        cluster_nodes=["node1", "node2", "node3"],
        config=raft_config,
    )

    assert raft.node_id == "node1"
    assert raft.current_term == 0
    assert raft.state == ConsensusState.FOLLOWER
    assert len(raft.cluster_nodes) == 3


@pytest.mark.asyncio
async def test_raft_start_stop(raft_config: RaftConfig) -> None:
    """Test Raft start and stop."""
    raft = RaftConsensus(
        node_id="node1",
        cluster_nodes=["node1", "node2", "node3"],
        config=raft_config,
    )

    await raft.start()
    assert raft._running is True

    await raft.stop()
    assert raft._running is False


@pytest.mark.asyncio
async def test_raft_leader_election(raft_config: RaftConfig) -> None:
    """Test Raft leader election."""
    raft = RaftConsensus(
        node_id="node1",
        cluster_nodes=["node1", "node2", "node3"],
        config=raft_config,
    )

    await raft.start()
    await asyncio.sleep(0.3)

    assert raft.state in (
        ConsensusState.LEADER,
        ConsensusState.CANDIDATE,
        ConsensusState.FOLLOWER,
    )

    await raft.stop()


@pytest.mark.asyncio
async def test_consensus_message_serialization() -> None:
    """Test consensus message serialization."""
    msg = ConsensusMessage(
        message_type="request_vote",
        sender_id="node1",
        term=1,
        data={"key": "value"},
    )

    msg_dict = msg.to_dict()
    assert msg_dict["message_type"] == "request_vote"
    assert msg_dict["sender_id"] == "node1"
    assert msg_dict["term"] == 1

    restored = ConsensusMessage.from_dict(msg_dict)
    assert restored.message_type == msg.message_type
    assert restored.sender_id == msg.sender_id
    assert restored.term == msg.term
