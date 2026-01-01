from pathlib import Path
from unittest.mock import MagicMock

from pytest import fixture, mark
from src.nodes.queue_node import QueueNode
from src.utils.config import Config, NodeConfig


@fixture
def mock_config(tmp_path: Path) -> Config:
    config = MagicMock(spec=Config)
    config.node = MagicMock(spec=NodeConfig)
    config.node.node_id = "test-node"
    config.node.host = "localhost"
    config.node.port = 0
    config.node.data_dir = tmp_path
    config.node.heartbeat_interval = 100
    config.node.request_timeout = 1.0
    config.get_peer_nodes.return_value = []
    return config


@mark.asyncio
async def test_wal_persistence_and_recovery(mock_config: Config) -> None:
    node1 = QueueNode(mock_config)
    await node1.start()

    topic = "test-topic"
    payload = b"test-payload"
    msg_id = await node1.publish(topic, payload)

    assert len(node1._wal) == 1
    assert node1._wal[0].message_id == msg_id

    await node1.stop()

    node2 = QueueNode(mock_config)
    await node2.start()

    assert len(node2._wal) == 1
    recovered_msg = node2._wal[0]
    assert recovered_msg.message_id == msg_id
    assert recovered_msg.payload == payload
    assert recovered_msg.topic == topic

    partitions = node2.get_topic_info(topic)
    assert partitions["total_messages"] == 1

    await node2.stop()


@mark.asyncio
async def test_wal_ack_persistence(mock_config: Config) -> None:
    node1 = QueueNode(mock_config)
    await node1.start()

    topic = "test-topic"
    group = "test-group"
    consumer_id = "consumer-1"

    msg_id = await node1.publish(topic, b"data")
    await node1.subscribe(topic, group, consumer_id)

    messages = await node1.consume(topic, group, consumer_id)
    assert len(messages) == 1
    assert messages[0].message_id == msg_id

    success = await node1.acknowledge(msg_id)
    assert success

    await node1.stop()

    node2 = QueueNode(mock_config)
    await node2.start()

    recovered_msg = node2._wal[0]

    from src.nodes.queue_node import MessageState

    assert recovered_msg.state == MessageState.ACKNOWLEDGED

    group_info = node2.get_consumer_group_info(group)
    assert group_info is not None
    assert group_info["offsets"][recovered_msg.partition] == recovered_msg.offset + 1

    await node2.stop()
