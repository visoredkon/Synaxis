from asyncio import sleep
from unittest.mock import AsyncMock, MagicMock

from pytest import fixture, mark
from src.communication.message_passing import Message, MessageBus, MessageType
from src.consensus.pbft import PBFTConsensus, PBFTPhase


@fixture
def mock_message_bus() -> MagicMock:
    bus = MagicMock(spec=MessageBus)
    bus.send = AsyncMock(return_value=None)
    bus.register_handler = MagicMock()
    return bus


@mark.asyncio
async def test_pbft_pre_prepare_phase(mock_message_bus: MagicMock) -> None:
    peers = [
        ("node2", "localhost", 8001),
        ("node3", "localhost", 8002),
        ("node4", "localhost", 8003),
    ]
    pbft = PBFTConsensus("node1", mock_message_bus, peers, is_primary=True)
    await pbft.start()

    request = b"test-request"
    await pbft.propose(request)

    assert mock_message_bus.send.call_count >= len(peers)

    digest = pbft._compute_digest(request)
    assert digest in pbft._requests
    state = pbft._requests[digest]
    assert state.phase == PBFTPhase.PREPARE
    assert state.pre_prepare_received

    await pbft.stop()


@mark.asyncio
async def test_pbft_commit_workflow(mock_message_bus: MagicMock) -> None:
    peers = [
        ("node2", "localhost", 8001),
        ("node3", "localhost", 8002),
        ("node4", "localhost", 8003),
    ]
    pbft = PBFTConsensus("node1", mock_message_bus, peers, is_primary=False)
    await pbft.start()

    await pbft.start()

    request = b"test-request"
    digest = pbft._compute_digest(request)

    pre_prepare_payload = {
        "view": 0,
        "sequence": 1,
        "digest": digest,
        "node_id": "primary",
        "phase": PBFTPhase.PRE_PREPARE.value,
        "request": request,
    }

    msg = Message(
        msg_type=MessageType.PRE_PREPARE,
        sender_id="primary",
        payload=pre_prepare_payload,
    )

    await pbft._handle_pre_prepare(msg)

    state = pbft._requests[digest]
    assert state.phase == PBFTPhase.PREPARE
    assert "node1" in state.prepared_nodes

    assert state.phase == PBFTPhase.PREPARE
    assert "node1" in state.prepared_nodes

    prepare_payload_2 = {
        "view": 0,
        "sequence": 1,
        "digest": digest,
        "node_id": "node2",
        "phase": PBFTPhase.PREPARE.value,
    }
    msg2 = Message(MessageType.PREPARE, "node2", prepare_payload_2)
    await pbft._handle_prepare(msg2)

    prepare_payload_3 = {
        "view": 0,
        "sequence": 1,
        "digest": digest,
        "node_id": "node3",
        "phase": PBFTPhase.PREPARE.value,
    }
    msg3 = Message(MessageType.PREPARE, "node3", prepare_payload_3)
    await pbft._handle_prepare(msg3)

    await sleep(0.1)

    assert state.phase == PBFTPhase.COMMIT
    assert state.commit_count == 1

    commit_payload_2 = {
        "view": 0,
        "sequence": 1,
        "digest": digest,
        "node_id": "node2",
        "phase": PBFTPhase.COMMIT.value,
    }
    await pbft._handle_commit(Message(MessageType.COMMIT, "node2", commit_payload_2))

    commit_payload_3 = {
        "view": 0,
        "sequence": 1,
        "digest": digest,
        "node_id": "node3",
        "phase": PBFTPhase.COMMIT.value,
    }
    await pbft._handle_commit(Message(MessageType.COMMIT, "node3", commit_payload_3))

    assert state.phase == PBFTPhase.REPLY

    await pbft.stop()
