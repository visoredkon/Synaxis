import asyncio
import bisect
import hashlib
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum

import redis.asyncio as aioredis
from loguru import logger

from .base_node import BaseNode


class MessageStatus(Enum):
    PENDING = "pending"
    IN_FLIGHT = "in_flight"
    DELIVERED = "delivered"
    FAILED = "failed"


@dataclass
class QueueMessage:
    message_id: str
    data: object
    timestamp: float
    status: MessageStatus = MessageStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3


@dataclass
class ConsistentHashRing:
    nodes: list[str] = field(default_factory=list)
    virtual_nodes: int = 150
    ring: dict[int, str] = field(default_factory=dict)
    _sorted_keys: list[int] = field(default_factory=list)

    def add_node(self, node: str) -> None:
        for i in range(self.virtual_nodes):
            key = f"{node}:{i}"
            hash_value = self._hash(key)
            self.ring[hash_value] = node
        self.nodes.append(node)
        self._sorted_keys = sorted(self.ring.keys())
        logger.debug(f"Added node {node} to hash ring")

    def remove_node(self, node: str) -> None:
        for i in range(self.virtual_nodes):
            key = f"{node}:{i}"
            hash_value = self._hash(key)
            if hash_value in self.ring:
                del self.ring[hash_value]
        if node in self.nodes:
            self.nodes.remove(node)
        self._sorted_keys = sorted(self.ring.keys())
        logger.debug(f"Removed node {node} from hash ring")

    def get_node(self, key: str) -> str | None:
        if not self._sorted_keys:
            return None
        hash_value = self._hash(key)
        idx = bisect.bisect_right(self._sorted_keys, hash_value)
        if idx == len(self._sorted_keys):
            idx = 0
        return self.ring[self._sorted_keys[idx]]

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)


class QueueNode(BaseNode):
    redis_url: str
    partitions: int
    redis: aioredis.Redis | None
    hash_ring: ConsistentHashRing
    local_queues: dict[str, deque[QueueMessage]]
    in_flight: dict[str, QueueMessage]
    _consumer_task: asyncio.Task[None] | None
    _persistence_task: asyncio.Task[None] | None

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        cluster_nodes: list[str],
        redis_url: str = "redis://localhost:6379",
        partitions: int = 16,
    ) -> None:
        super().__init__(node_id, host, port, cluster_nodes)
        self.redis_url = redis_url
        self.partitions = partitions
        self.redis = None
        self.hash_ring = ConsistentHashRing()
        self.local_queues = {}
        self.in_flight = {}
        self._consumer_task = None
        self._persistence_task = None

    async def start(self) -> None:
        await super().start()
        self.redis = await aioredis.from_url(self.redis_url, decode_responses=False)
        for node in self.cluster_nodes:
            self.hash_ring.add_node(node)
        for i in range(self.partitions):
            partition_name = f"partition_{i}"
            self.local_queues[partition_name] = deque()
        await self._recover_messages()
        self.message_passing.register_handler("enqueue", self._handle_enqueue)
        self.message_passing.register_handler("dequeue", self._handle_dequeue)
        self.message_passing.register_handler("ack", self._handle_ack)
        self._consumer_task = asyncio.create_task(self._consumer_loop())
        self._persistence_task = asyncio.create_task(self._persistence_loop())
        logger.info(
            f"Queue node {self.node_id} started with {self.partitions} partitions"
        )

    async def stop(self) -> None:
        if self._consumer_task:
            self._consumer_task.cancel()
            try:
                await self._consumer_task
            except asyncio.CancelledError:
                pass
        if self._persistence_task:
            self._persistence_task.cancel()
            try:
                await self._persistence_task
            except asyncio.CancelledError:
                pass
        if self.redis:
            await self.redis.close()
        await super().stop()
        logger.info(f"Queue node {self.node_id} stopped")

    async def enqueue(self, queue_name: str, data: object) -> bool:
        self.metrics.start_timer("enqueue")
        message_id = f"{self.node_id}:{time.time()}:{id(data)}"
        message = QueueMessage(message_id=message_id, data=data, timestamp=time.time())
        target_node = self.hash_ring.get_node(queue_name)
        if not target_node:
            logger.error("No node available in hash ring")
            return False
        if target_node == self.node_id:
            success = await self._enqueue_local(queue_name, message)
        else:
            success = await self._enqueue_remote(target_node, queue_name, message)
        self.metrics.stop_timer("enqueue")
        if success:
            self.metrics.increment_counter("messages_enqueued")
        else:
            self.metrics.increment_counter("enqueue_failed")
        return success

    async def dequeue(self, queue_name: str, timeout: float = 5.0) -> object | None:
        self.metrics.start_timer("dequeue")
        target_node = self.hash_ring.get_node(queue_name)
        if not target_node:
            return None
        if target_node == self.node_id:
            message = await self._dequeue_local(queue_name, timeout)
        else:
            message = await self._dequeue_remote(target_node, queue_name, timeout)
        self.metrics.stop_timer("dequeue")
        if message:
            self.metrics.increment_counter("messages_dequeued")
        else:
            self.metrics.increment_counter("dequeue_timeout")
        return message.data if message else None

    async def acknowledge(self, message_id: str) -> bool:
        if message_id in self.in_flight:
            message = self.in_flight.pop(message_id)
            message.status = MessageStatus.DELIVERED
            await self._persist_message(message)
            self.metrics.increment_counter("messages_acked")
            return True
        logger.warning(f"Attempted to ack unknown message {message_id}")
        return False

    async def _enqueue_local(self, queue_name: str, message: QueueMessage) -> bool:
        partition = self._get_partition(queue_name)
        self.local_queues[partition].append(message)
        await self._persist_message(message)
        logger.debug(f"Enqueued message {message.message_id} to {partition}")
        return True

    async def _dequeue_local(
        self, queue_name: str, timeout: float
    ) -> QueueMessage | None:
        partition = self._get_partition(queue_name)
        queue = self.local_queues[partition]
        start_time = time.time()
        while time.time() - start_time < timeout:
            if queue:
                message = queue.popleft()
                message.status = MessageStatus.IN_FLIGHT
                self.in_flight[message.message_id] = message
                await self._persist_message(message)
                logger.debug(f"Dequeued message {message.message_id} from {partition}")
                return message
            await asyncio.sleep(0.1)
        return None

    async def _enqueue_remote(
        self, target_node: str, queue_name: str, message: QueueMessage
    ) -> bool:
        logger.warning(f"Remote enqueue not fully implemented for node {target_node}")
        return False

    async def _dequeue_remote(
        self, target_node: str, queue_name: str, timeout: float
    ) -> QueueMessage | None:
        logger.warning(f"Remote dequeue not fully implemented for node {target_node}")
        return None

    async def _handle_enqueue(self, message: dict[str, object]) -> dict[str, object]:
        queue_name_obj = message.get("queue_name", "")
        queue_name = str(queue_name_obj) if isinstance(queue_name_obj, str) else ""
        data = message.get("data")
        success = False
        if data is not None:
            message_id_obj = message.get("message_id", "")
            message_id = str(message_id_obj) if isinstance(message_id_obj, str) else ""
            timestamp_obj = message.get("timestamp", time.time())
            timestamp = (
                float(timestamp_obj)
                if isinstance(timestamp_obj, (int, float))
                else time.time()
            )
            msg = QueueMessage(message_id=message_id, data=data, timestamp=timestamp)
            success = await self._enqueue_local(queue_name, msg)
        return {"type": "enqueue_response", "success": success}

    async def _handle_dequeue(self, message: dict[str, object]) -> dict[str, object]:
        queue_name_obj = message.get("queue_name", "")
        queue_name = str(queue_name_obj) if isinstance(queue_name_obj, str) else ""
        timeout_obj = message.get("timeout", 5.0)
        timeout = float(timeout_obj) if isinstance(timeout_obj, (int, float)) else 5.0
        msg = await self._dequeue_local(queue_name, timeout)
        return {
            "type": "dequeue_response",
            "message": msg.data if msg else None,
            "message_id": msg.message_id if msg else None,
        }

    async def _handle_ack(self, message: dict[str, object]) -> dict[str, object]:
        message_id_obj = message.get("message_id", "")
        message_id = str(message_id_obj) if isinstance(message_id_obj, str) else ""
        success = await self.acknowledge(message_id)
        return {"type": "ack_response", "success": success}

    def _get_partition(self, queue_name: str) -> str:
        hash_value = int(hashlib.md5(queue_name.encode()).hexdigest(), 16)
        partition_idx = hash_value % self.partitions
        return f"partition_{partition_idx}"

    async def _persist_message(self, message: QueueMessage) -> None:
        if not self.redis:
            return
        try:
            import orjson

            key = f"message:{message.message_id}"
            value = orjson.dumps(
                {
                    "message_id": message.message_id,
                    "data": message.data,
                    "timestamp": message.timestamp,
                    "status": message.status.value,
                    "retry_count": message.retry_count,
                }
            )
            await self.redis.set(key, value)
        except Exception as e:
            logger.error(f"Error persisting message: {e}")

    async def _recover_messages(self) -> None:
        if not self.redis:
            return
        try:
            import orjson

            pattern = "message:*"
            cursor = 0
            recovered_count = 0
            while True:
                cursor, keys = await self.redis.scan(cursor, match=pattern, count=100)
                for key in keys:
                    value = await self.redis.get(key)
                    if value:
                        data = orjson.loads(value)
                        message = QueueMessage(
                            message_id=data["message_id"],
                            data=data["data"],
                            timestamp=data["timestamp"],
                            status=MessageStatus(data["status"]),
                            retry_count=data["retry_count"],
                        )
                        if message.status == MessageStatus.PENDING:
                            queue_name = f"recovered_{recovered_count}"
                            partition = self._get_partition(queue_name)
                            self.local_queues[partition].append(message)
                            recovered_count += 1
                if cursor == 0:
                    break
            if recovered_count > 0:
                logger.info(f"Recovered {recovered_count} messages from persistence")
                self.metrics.increment_counter("messages_recovered", recovered_count)
        except Exception as e:
            logger.error(f"Error recovering messages: {e}")

    async def _consumer_loop(self) -> None:
        while self._running:
            for message_id, message in list(self.in_flight.items()):
                if time.time() - message.timestamp > 30.0:
                    if message.retry_count < message.max_retries:
                        message.retry_count += 1
                        message.status = MessageStatus.PENDING
                        self.in_flight.pop(message_id)
                        partition = self._get_partition(message_id)
                        self.local_queues[partition].append(message)
                        logger.debug(f"Retrying message {message_id}")
                        self.metrics.increment_counter("messages_retried")
                    else:
                        message.status = MessageStatus.FAILED
                        self.in_flight.pop(message_id)
                        await self._persist_message(message)
                        logger.warning(f"Message {message_id} failed after max retries")
                        self.metrics.increment_counter("messages_failed")
            await asyncio.sleep(1.0)

    async def _persistence_loop(self) -> None:
        while self._running:
            try:
                for partition_name, queue in self.local_queues.items():
                    self.metrics.set_gauge(f"queue_size_{partition_name}", len(queue))
                self.metrics.set_gauge("in_flight_messages", len(self.in_flight))
            except Exception as e:
                logger.error(f"Error in persistence loop: {e}")
            await asyncio.sleep(5.0)


__all__ = ["QueueNode", "QueueMessage", "MessageStatus", "ConsistentHashRing"]
