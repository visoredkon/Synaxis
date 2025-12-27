from __future__ import annotations

from asyncio import CancelledError, Task, create_task, get_event_loop, sleep
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from loguru import logger

from src.communication.message_passing import Message, MessageType
from src.nodes.base_node import BaseNode, NodeRole
from src.utils.config import Config
from src.utils.consistent_hash import ConsistentHash
from src.utils.metrics import MetricsCollector, Timer

if TYPE_CHECKING:
    pass


class DeliveryGuarantee(Enum):
    AT_MOST_ONCE = auto()
    AT_LEAST_ONCE = auto()
    EXACTLY_ONCE = auto()


class MessageState(Enum):
    PENDING = auto()
    DELIVERED = auto()
    ACKNOWLEDGED = auto()
    DEAD_LETTER = auto()


@dataclass(slots=True)
class QueueMessage:
    message_id: str
    topic: str
    payload: bytes
    partition: int
    offset: int
    timestamp: float
    state: MessageState = MessageState.PENDING
    delivery_count: int = 0
    consumer_id: str | None = None
    ack_deadline: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "message_id": self.message_id,
            "topic": self.topic,
            "payload": self.payload,
            "partition": self.partition,
            "offset": self.offset,
            "timestamp": self.timestamp,
            "state": self.state.value,
            "delivery_count": self.delivery_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> QueueMessage:
        return cls(
            message_id=data["message_id"],
            topic=data["topic"],
            payload=data["payload"],
            partition=data["partition"],
            offset=data["offset"],
            timestamp=data["timestamp"],
            state=MessageState(data["state"]),
            delivery_count=data.get("delivery_count", 0),
        )


@dataclass(slots=True)
class ConsumerGroup:
    group_id: str
    topic: str
    consumers: dict[str, set[int]] = field(default_factory=dict)
    offsets: dict[int, int] = field(default_factory=dict)


@dataclass(slots=True)
class TopicPartition:
    topic: str
    partition: int
    messages: deque[QueueMessage] = field(default_factory=deque)
    next_offset: int = 0
    committed_offset: int = 0


class QueueNode(BaseNode):
    def __init__(
        self,
        config: Config,
        num_partitions: int = 3,
        max_delivery_attempts: int = 5,
        ack_timeout: float = 30.0,
        delivery_guarantee: DeliveryGuarantee = DeliveryGuarantee.AT_LEAST_ONCE,
    ) -> None:
        super().__init__(config, NodeRole.QUEUE_NODE)
        self._num_partitions = num_partitions
        self._max_delivery_attempts = max_delivery_attempts
        self._ack_timeout = ack_timeout
        self._delivery_guarantee = delivery_guarantee

        self._consistent_hash = ConsistentHash()
        self._partitions: dict[str, dict[int, TopicPartition]] = defaultdict(dict)
        self._consumer_groups: dict[str, ConsumerGroup] = {}
        self._pending_acks: dict[str, QueueMessage] = {}
        self._wal: list[QueueMessage] = []

        self._redelivery_task: Task[None] | None = None
        self._metrics = MetricsCollector()

    async def _on_start(self) -> None:
        self._consistent_hash.add_node(self.node_id)
        for peer in self._config.get_peer_nodes():
            self._consistent_hash.add_node(peer.node_id)

        await self._recover_from_wal()
        self._redelivery_task = create_task(self._redelivery_loop())
        logger.info(f"Queue node started with {self._num_partitions} partitions")

    async def _on_stop(self) -> None:
        if self._redelivery_task:
            self._redelivery_task.cancel()
            try:
                await self._redelivery_task
            except CancelledError:
                pass
        await self._persist_to_wal()

    def _register_handlers(self) -> None:
        super()._register_handlers()
        self._message_bus.register_handler(
            MessageType.QUEUE_PUBLISH,
            self._handle_publish,
        )
        self._message_bus.register_handler(
            MessageType.QUEUE_CONSUME,
            self._handle_consume,
        )
        self._message_bus.register_handler(
            MessageType.QUEUE_ACK,
            self._handle_ack,
        )

    def _get_partition_for_key(self, topic: str, key: str | None = None) -> int:
        if key:
            node = self._consistent_hash.get_node(key)
            if node:
                h = hash(node)
                return h % self._num_partitions
        return hash(topic + str(uuid4())) % self._num_partitions

    def _get_or_create_partition(self, topic: str, partition: int) -> TopicPartition:
        if partition not in self._partitions[topic]:
            self._partitions[topic][partition] = TopicPartition(
                topic=topic,
                partition=partition,
            )
        return self._partitions[topic][partition]

    async def publish(
        self,
        topic: str,
        payload: bytes,
        key: str | None = None,
    ) -> str:
        partition_id = self._get_partition_for_key(topic, key)
        partition = self._get_or_create_partition(topic, partition_id)

        message = QueueMessage(
            message_id=str(uuid4()),
            topic=topic,
            payload=payload,
            partition=partition_id,
            offset=partition.next_offset,
            timestamp=get_event_loop().time(),
        )

        partition.messages.append(message)
        partition.next_offset += 1

        self._wal.append(message)

        self._metrics.record_queue_size(
            self.node_id,
            topic,
            len(partition.messages),
        )

        logger.debug(
            f"Published message {message.message_id} to {topic}:{partition_id}"
        )
        return message.message_id

    async def subscribe(
        self,
        topic: str,
        consumer_group: str,
        consumer_id: str,
    ) -> None:
        if consumer_group not in self._consumer_groups:
            self._consumer_groups[consumer_group] = ConsumerGroup(
                group_id=consumer_group,
                topic=topic,
            )

        group = self._consumer_groups[consumer_group]
        if consumer_id not in group.consumers:
            group.consumers[consumer_id] = set()

        self._rebalance_partitions(group)
        logger.info(f"Consumer {consumer_id} subscribed to {topic}")

    def _rebalance_partitions(self, group: ConsumerGroup) -> None:
        if not group.consumers:
            return

        consumer_ids = list(group.consumers.keys())
        for consumer_id in consumer_ids:
            group.consumers[consumer_id] = set()

        for partition_id in range(self._num_partitions):
            consumer_idx = partition_id % len(consumer_ids)
            consumer_id = consumer_ids[consumer_idx]
            group.consumers[consumer_id].add(partition_id)

    async def consume(
        self,
        topic: str,
        consumer_group: str,
        consumer_id: str,
        max_messages: int = 10,
    ) -> list[QueueMessage]:
        if consumer_group not in self._consumer_groups:
            return []

        group = self._consumer_groups[consumer_group]
        if consumer_id not in group.consumers:
            return []

        assigned_partitions = group.consumers[consumer_id]
        messages: list[QueueMessage] = []
        now = get_event_loop().time()

        for partition_id in assigned_partitions:
            if len(messages) >= max_messages:
                break

            partition = self._partitions.get(topic, {}).get(partition_id)
            if not partition:
                continue

            offset = group.offsets.get(partition_id, 0)

            for msg in partition.messages:
                if len(messages) >= max_messages:
                    break
                if msg.offset < offset:
                    continue
                if msg.state == MessageState.ACKNOWLEDGED:
                    continue
                if msg.state == MessageState.DELIVERED and msg.ack_deadline > now:
                    continue
                if msg.delivery_count >= self._max_delivery_attempts:
                    msg.state = MessageState.DEAD_LETTER
                    continue

                msg.state = MessageState.DELIVERED
                msg.consumer_id = consumer_id
                msg.delivery_count += 1
                msg.ack_deadline = now + self._ack_timeout
                self._pending_acks[msg.message_id] = msg
                messages.append(msg)

        return messages

    async def acknowledge(self, message_id: str) -> bool:
        if message_id not in self._pending_acks:
            return False

        msg = self._pending_acks.pop(message_id)
        msg.state = MessageState.ACKNOWLEDGED

        group_id = None
        for gid, group in self._consumer_groups.items():
            if group.topic == msg.topic:
                group_id = gid
                break

        if group_id:
            group = self._consumer_groups[group_id]
            current = group.offsets.get(msg.partition, 0)
            if msg.offset >= current:
                group.offsets[msg.partition] = msg.offset + 1

        logger.debug(f"Acknowledged message {message_id}")
        return True

    async def _redelivery_loop(self) -> None:
        while True:
            await sleep(1.0)
            now = get_event_loop().time()

            expired = [
                msg_id
                for msg_id, msg in self._pending_acks.items()
                if msg.ack_deadline < now
            ]

            for msg_id in expired:
                msg = self._pending_acks.pop(msg_id)
                msg.state = MessageState.PENDING
                logger.warning(
                    f"Message {msg_id} redelivery scheduled (attempt {msg.delivery_count})"
                )

    async def _persist_to_wal(self) -> None:
        pass

    async def _recover_from_wal(self) -> None:
        pass

    async def _handle_publish(self, message: Message) -> Message:
        topic = message.payload["topic"]
        payload = message.payload["payload"]
        key = message.payload.get("key")

        with Timer(
            lambda t: self._metrics.record_request(
                self.node_id, "queue_publish", "success", t
            )
        ):
            message_id = await self.publish(topic, payload, key)

        return Message(
            msg_type=MessageType.QUEUE_RESPONSE,
            sender_id=self.node_id,
            payload={"success": True, "message_id": message_id},
        )

    async def _handle_consume(self, message: Message) -> Message:
        topic = message.payload["topic"]
        consumer_group = message.payload["consumer_group"]
        consumer_id = message.sender_id
        max_messages = message.payload.get("max_messages", 10)

        await self.subscribe(topic, consumer_group, consumer_id)
        messages = await self.consume(topic, consumer_group, consumer_id, max_messages)

        return Message(
            msg_type=MessageType.QUEUE_RESPONSE,
            sender_id=self.node_id,
            payload={
                "success": True,
                "messages": [m.to_dict() for m in messages],
            },
        )

    async def _handle_ack(self, message: Message) -> Message:
        message_id = message.payload["message_id"]
        success = await self.acknowledge(message_id)

        return Message(
            msg_type=MessageType.QUEUE_RESPONSE,
            sender_id=self.node_id,
            payload={"success": success},
        )

    def get_topic_info(self, topic: str) -> dict[str, Any]:
        partitions = self._partitions.get(topic, {})
        return {
            "topic": topic,
            "partitions": len(partitions),
            "total_messages": sum(len(p.messages) for p in partitions.values()),
            "partition_info": {
                pid: {
                    "size": len(p.messages),
                    "next_offset": p.next_offset,
                    "committed_offset": p.committed_offset,
                }
                for pid, p in partitions.items()
            },
        }

    def get_consumer_group_info(self, group_id: str) -> dict[str, Any] | None:
        group = self._consumer_groups.get(group_id)
        if not group:
            return None
        return {
            "group_id": group.group_id,
            "topic": group.topic,
            "consumers": {
                cid: list(partitions) for cid, partitions in group.consumers.items()
            },
            "offsets": dict(group.offsets),
        }
