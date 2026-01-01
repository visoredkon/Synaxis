from __future__ import annotations

from asyncio import sleep

from pytest import fixture, mark
from src.consensus.raft import LogEntry, RaftConsensus, RaftRole, RaftState

from conftest import MockMessageBus


class TestRaftState:
    def test_initial_state(self) -> None:
        state = RaftState()
        assert state.current_term == 0
        assert state.voted_for is None
        assert state.log == []
        assert state.commit_index == 0
        assert state.last_applied == 0

    def test_log_entry_serialization(self) -> None:
        entry = LogEntry(term=1, index=1, command=b"test")
        data = entry.to_dict()
        restored = LogEntry.from_dict(data)
        assert restored.term == entry.term
        assert restored.index == entry.index
        assert restored.command == entry.command


class TestRaftConsensus:
    @fixture
    def raft_node(self, mock_bus: MockMessageBus) -> RaftConsensus:
        peers = [
            ("node-2", "localhost", 8001),
            ("node-3", "localhost", 8002),
        ]
        return RaftConsensus(
            node_id="node-1",
            message_bus=mock_bus,
            peers=peers,
            election_timeout_min=150,
            election_timeout_max=300,
        )

    def test_initial_role_is_follower(self, raft_node: RaftConsensus) -> None:
        assert raft_node.role == RaftRole.FOLLOWER

    def test_initial_term_is_zero(self, raft_node: RaftConsensus) -> None:
        assert raft_node.term == 0

    def test_not_leader_initially(self, raft_node: RaftConsensus) -> None:
        assert not raft_node.is_leader

    @mark.asyncio
    async def test_propose_fails_when_not_leader(
        self,
        raft_node: RaftConsensus,
    ) -> None:
        await raft_node.start()
        result = await raft_node.propose(b"test command")
        assert result is False
        await raft_node.stop()

    def test_state_summary(self, raft_node: RaftConsensus) -> None:
        summary = raft_node.get_state_summary()
        assert summary["node_id"] == "node-1"
        assert summary["role"] == "FOLLOWER"
        assert summary["term"] == 0
        assert summary["leader_id"] is None


class TestLeaderElection:
    @mark.asyncio
    async def test_election_timeout_triggers_election(
        self,
        mock_bus: MockMessageBus,
    ) -> None:
        peers = [("node-2", "localhost", 8001)]
        raft = RaftConsensus(
            node_id="node-1",
            message_bus=mock_bus,
            peers=peers,
            election_timeout_min=50,
            election_timeout_max=100,
        )
        await raft.start()
        await sleep(0.2)
        assert raft.role == RaftRole.CANDIDATE
        assert raft.term >= 1
        await raft.stop()


class TestLogReplication:
    def test_log_entry_creation(self) -> None:
        entry = LogEntry(term=1, index=1, command=b"SET x 1")
        assert entry.term == 1
        assert entry.index == 1
        assert entry.command == b"SET x 1"

    def test_log_entry_to_dict(self) -> None:
        entry = LogEntry(term=2, index=5, command=b"data")
        data = entry.to_dict()
        assert data["term"] == 2
        assert data["index"] == 5
        assert data["command"] == b"data"
