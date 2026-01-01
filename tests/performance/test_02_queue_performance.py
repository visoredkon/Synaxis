from __future__ import annotations

from time import perf_counter
from typing import TYPE_CHECKING

from pytest import fixture
from src.nodes.queue_node import (
    ConsumerGroup,
    DeliveryGuarantee,
    MessageState,
    QueueMessage,
    TopicPartition,
)
from src.utils.consistent_hash import ConsistentHash

if TYPE_CHECKING:
    pass


class TestConsistentHashPerformance:
    @fixture
    def hash_ring(self) -> ConsistentHash:
        ch = ConsistentHash(virtual_nodes=150)
        for i in range(10):
            ch.add_node(f"node-{i}")
        return ch

    def test_get_node_performance(self, hash_ring: ConsistentHash) -> None:
        iterations = 10000
        keys = [f"key-{i}" for i in range(iterations)]
        start = perf_counter()
        for key in keys:
            hash_ring.get_node(key)
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 50000

    def test_add_node_performance(self) -> None:
        ch = ConsistentHash(virtual_nodes=150)
        iterations = 100
        start = perf_counter()
        for i in range(iterations):
            ch.add_node(f"node-{i}")
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 100

    def test_key_distribution(self, hash_ring: ConsistentHash) -> None:
        keys = 10000
        distribution: dict[str, int] = {}
        for i in range(keys):
            node = hash_ring.get_node(f"key-{i}")
            if node:
                distribution[node] = distribution.get(node, 0) + 1
        values = list(distribution.values())
        avg = sum(values) / len(values)
        max_deviation = max(abs(v - avg) / avg for v in values)
        assert max_deviation < 0.5


class TestQueueMessagePerformance:
    def test_message_creation_performance(self) -> None:
        iterations = 10000
        start = perf_counter()
        for i in range(iterations):
            QueueMessage(
                message_id=f"msg-{i}",
                topic="test-topic",
                payload=b"test payload",
                partition=i % 3,
                offset=i,
                timestamp=0.0,
            )
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 50000

    def test_message_serialization_performance(self) -> None:
        msg = QueueMessage(
            message_id="test-msg",
            topic="test-topic",
            payload=b"test payload",
            partition=0,
            offset=0,
            timestamp=0.0,
        )
        iterations = 10000
        start = perf_counter()
        for _ in range(iterations):
            msg.to_dict()
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 50000

    def test_message_deserialization_performance(self) -> None:
        data = {
            "message_id": "test-msg",
            "topic": "test-topic",
            "payload": b"test payload",
            "partition": 0,
            "offset": 0,
            "timestamp": 0.0,
            "state": MessageState.PENDING.value,
            "delivery_count": 0,
        }
        iterations = 10000
        start = perf_counter()
        for _ in range(iterations):
            QueueMessage.from_dict(data)
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 30000


class TestPartitionPerformance:
    @fixture
    def partition(self) -> TopicPartition:
        return TopicPartition(topic="test", partition=0)

    def test_message_append_performance(self, partition: TopicPartition) -> None:
        iterations = 10000
        messages = [
            QueueMessage(
                message_id=f"msg-{i}",
                topic="test",
                payload=b"payload",
                partition=0,
                offset=i,
                timestamp=0.0,
            )
            for i in range(iterations)
        ]
        start = perf_counter()
        for msg in messages:
            partition.messages.append(msg)
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 100000

    def test_message_popleft_performance(self) -> None:
        partition = TopicPartition(topic="test", partition=0)
        for i in range(10000):
            partition.messages.append(
                QueueMessage(
                    message_id=f"msg-{i}",
                    topic="test",
                    payload=b"payload",
                    partition=0,
                    offset=i,
                    timestamp=0.0,
                )
            )
        iterations = 10000
        start = perf_counter()
        for _ in range(iterations):
            partition.messages.popleft()
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 100000


class TestConsumerGroupPerformance:
    def test_consumer_partition_assignment(self) -> None:
        group = ConsumerGroup(group_id="test-group", topic="test-topic")
        num_consumers = 10
        num_partitions = 30
        for i in range(num_consumers):
            group.consumers[f"consumer-{i}"] = set()
        consumer_ids = list(group.consumers.keys())
        start = perf_counter()
        iterations = 1000
        for _ in range(iterations):
            for cid in consumer_ids:
                group.consumers[cid] = set()
            for partition_id in range(num_partitions):
                consumer_idx = partition_id % len(consumer_ids)
                consumer_id = consumer_ids[consumer_idx]
                group.consumers[consumer_id].add(partition_id)
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 1000


class TestDeliveryGuaranteePerformance:
    def test_enum_access_performance(self) -> None:
        iterations = 100000
        start = perf_counter()
        for _ in range(iterations):
            _ = DeliveryGuarantee.AT_LEAST_ONCE
            _ = DeliveryGuarantee.AT_MOST_ONCE
            _ = DeliveryGuarantee.EXACTLY_ONCE
        elapsed = perf_counter() - start
        ops_per_sec = iterations / elapsed
        assert ops_per_sec > 500000
