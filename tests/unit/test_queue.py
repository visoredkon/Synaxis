"""Unit tests for queue node."""

from src.nodes.queue_node import ConsistentHashRing, MessageStatus, QueueMessage


def test_consistent_hash_ring_initialization() -> None:
    """Test consistent hash ring initialization."""
    ring = ConsistentHashRing()
    assert len(ring.nodes) == 0
    assert len(ring.ring) == 0


def test_consistent_hash_add_node() -> None:
    """Test adding nodes to hash ring."""
    ring = ConsistentHashRing()

    ring.add_node("node1")
    assert len(ring.nodes) == 1
    assert len(ring.ring) == ring.virtual_nodes

    ring.add_node("node2")
    assert len(ring.nodes) == 2
    assert len(ring.ring) == ring.virtual_nodes * 2


def test_consistent_hash_remove_node() -> None:
    """Test removing nodes from hash ring."""
    ring = ConsistentHashRing()

    ring.add_node("node1")
    ring.add_node("node2")

    ring.remove_node("node1")
    assert len(ring.nodes) == 1
    assert "node1" not in ring.nodes


def test_consistent_hash_get_node() -> None:
    """Test getting node for a key."""
    ring = ConsistentHashRing()

    ring.add_node("node1")
    ring.add_node("node2")
    ring.add_node("node3")

    node = ring.get_node("test_key")
    assert node in ["node1", "node2", "node3"]

    node2 = ring.get_node("test_key")
    assert node == node2


def test_queue_message_creation() -> None:
    """Test queue message creation."""
    msg = QueueMessage(
        message_id="msg1",
        data={"key": "value"},
        timestamp=1234567890.0,
    )

    assert msg.message_id == "msg1"
    assert msg.data == {"key": "value"}
    assert msg.status == MessageStatus.PENDING
    assert msg.retry_count == 0


def test_message_status_enum() -> None:
    """Test message status enumeration."""
    assert MessageStatus.PENDING.value == "pending"
    assert MessageStatus.IN_FLIGHT.value == "in_flight"
    assert MessageStatus.DELIVERED.value == "delivered"
    assert MessageStatus.FAILED.value == "failed"
