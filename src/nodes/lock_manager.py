import asyncio
import time
from dataclasses import dataclass
from enum import Enum

from loguru import logger

from ..communications import ConsensusMessagePassing
from ..consensuses import ConsensusMessage
from ..consensuses.raft import RaftConfig, RaftConsensus
from .base_node import BaseNode


class LockType(Enum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


@dataclass
class Lock:
    lock_id: str
    lock_type: LockType
    holders: set[str]
    timestamp: float
    ttl: float = 30.0

    def is_expired(self) -> bool:
        return time.time() - self.timestamp > self.ttl


@dataclass
class LockRequest:
    lock_id: str
    lock_type: LockType
    requester_id: str
    timeout: float = 30.0


class LockManager(BaseNode):
    node_addresses: dict[str, tuple[str, int]]
    consensus_messaging: ConsensusMessagePassing
    raft: RaftConsensus
    locks: dict[str, Lock]
    pending_requests: dict[str, LockRequest]
    wait_graph: dict[str, set[str]]
    _cleanup_task: asyncio.Task[None] | None

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        cluster_nodes: list[str],
        node_addresses: dict[str, tuple[str, int]],
        raft_config: RaftConfig | None = None,
    ) -> None:
        super().__init__(node_id, host, port, cluster_nodes)
        self.node_addresses = node_addresses
        self.consensus_messaging = ConsensusMessagePassing(
            self.message_passing, node_addresses
        )
        self.raft = RaftConsensus(
            node_id, cluster_nodes, raft_config, self._send_consensus_message
        )
        self.locks = {}
        self.pending_requests = {}
        self.wait_graph = {}
        self._cleanup_task = None

    async def start(self) -> None:
        await super().start()
        self.message_passing.register_handler(
            "consensus", self._handle_consensus_message
        )
        self.message_passing.register_handler("lock_request", self._handle_lock_request)
        self.message_passing.register_handler("lock_release", self._handle_lock_release)
        self.raft.register_commit_callback(self._on_consensus_commit)
        await self.raft.start()
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_locks())
        logger.info(f"Lock manager {self.node_id} started")

    async def stop(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        await self.raft.stop()
        await super().stop()
        logger.info(f"Lock manager {self.node_id} stopped")

    async def acquire_lock(
        self,
        lock_id: str,
        lock_type: LockType = LockType.EXCLUSIVE,
        timeout: float = 30.0,
    ) -> bool:
        self.metrics.start_timer(f"lock_acquire_{lock_id}")
        request = LockRequest(
            lock_id=lock_id,
            lock_type=lock_type,
            requester_id=self.node_id,
            timeout=timeout,
        )
        if self.raft.is_leader():
            success = await self._process_lock_request(request)
        else:
            leader_id = self.raft.get_leader_id()
            if not leader_id:
                logger.warning("No leader available for lock acquisition")
                return False
            success = await self._forward_to_leader(leader_id, request)
        duration = self.metrics.stop_timer(f"lock_acquire_{lock_id}")
        if duration:
            self.metrics.record_histogram("lock_acquire_duration", duration)
        if success:
            self.metrics.increment_counter("locks_acquired")
        else:
            self.metrics.increment_counter("locks_failed")
        return success

    async def release_lock(self, lock_id: str) -> bool:
        if lock_id not in self.locks:
            logger.warning(f"Attempt to release non-existent lock {lock_id}")
            return False
        lock = self.locks[lock_id]
        if self.node_id not in lock.holders:
            logger.warning(f"Node {self.node_id} does not hold lock {lock_id}")
            return False
        release_data = {"lock_id": lock_id, "holder": self.node_id}
        success = await self.raft.propose({"action": "release", "data": release_data})
        if success:
            self.metrics.increment_counter("locks_released")
        return success

    def is_locked(self, lock_id: str) -> bool:
        return lock_id in self.locks and (not self.locks[lock_id].is_expired())

    def get_lock_holders(self, lock_id: str) -> set[str]:
        if lock_id in self.locks:
            return self.locks[lock_id].holders.copy()
        return set()

    async def _send_consensus_message(
        self, target_node: str, message: ConsensusMessage
    ) -> ConsensusMessage | None:
        return await self.consensus_messaging.send_consensus_message(
            target_node, message
        )

    async def _handle_consensus_message(
        self, message: dict[str, object]
    ) -> dict[str, object] | None:
        payload_obj = message.get("payload", {})
        payload = payload_obj if isinstance(payload_obj, dict) else {}
        consensus_msg = ConsensusMessage.from_dict(payload)
        response = await self.raft.handle_message(consensus_msg)
        if response:
            return {"type": "consensus", "payload": response.to_dict()}
        return None

    async def _on_consensus_commit(self, data: dict[str, object]) -> None:
        action = data.get("action")
        lock_data_obj = data.get("data")
        lock_data = lock_data_obj if isinstance(lock_data_obj, dict) else {}
        if action == "acquire":
            await self._apply_lock_acquisition(lock_data)
        elif action == "release":
            await self._apply_lock_release(lock_data)

    async def _process_lock_request(self, request: LockRequest) -> bool:
        if self._can_grant_lock(request):
            lock_data = {
                "lock_id": request.lock_id,
                "lock_type": request.lock_type.value,
                "requester": request.requester_id,
                "timestamp": time.time(),
            }
            return await self.raft.propose({"action": "acquire", "data": lock_data})
        if self._check_deadlock(request):
            logger.warning(f"Deadlock detected for lock {request.lock_id}")
            self.metrics.increment_counter("deadlocks_detected")
            return False
        self.pending_requests[request.lock_id] = request
        self._update_wait_graph(request)
        return False

    def _can_grant_lock(self, request: LockRequest) -> bool:
        if request.lock_id not in self.locks:
            return True
        lock = self.locks[request.lock_id]
        if lock.is_expired():
            return True
        if request.lock_type == LockType.SHARED and lock.lock_type == LockType.SHARED:
            return True
        return False

    def _check_deadlock(self, request: LockRequest) -> bool:
        visited: set[str] = set()
        stack: set[str] = set()

        def dfs(node: str) -> bool:
            if node in stack:
                return True
            if node in visited:
                return False
            visited.add(node)
            stack.add(node)
            for neighbor in self.wait_graph.get(node, set()):
                if dfs(neighbor):
                    return True
            stack.remove(node)
            return False

        return dfs(request.requester_id)

    def _update_wait_graph(self, request: LockRequest) -> None:
        if request.lock_id in self.locks:
            holders = self.locks[request.lock_id].holders
            self.wait_graph[request.requester_id] = holders.copy()

    async def _apply_lock_acquisition(self, lock_data: dict[str, object]) -> None:
        lock_id_obj = lock_data.get("lock_id", "")
        lock_id = str(lock_id_obj) if isinstance(lock_id_obj, str) else ""
        lock_type_obj = lock_data.get("lock_type", "exclusive")
        lock_type_str = (
            str(lock_type_obj) if isinstance(lock_type_obj, str) else "exclusive"
        )
        lock_type = LockType(lock_type_str)
        requester_obj = lock_data.get("requester", "")
        requester = str(requester_obj) if isinstance(requester_obj, str) else ""
        timestamp_obj = lock_data.get("timestamp", time.time())
        timestamp = (
            float(timestamp_obj)
            if isinstance(timestamp_obj, (int, float))
            else time.time()
        )
        if lock_id in self.locks:
            lock = self.locks[lock_id]
            lock.holders.add(requester)
        else:
            lock = Lock(
                lock_id=lock_id,
                lock_type=lock_type,
                holders={requester},
                timestamp=timestamp,
            )
            self.locks[lock_id] = lock
        if requester in self.wait_graph:
            del self.wait_graph[requester]
        logger.debug(f"Lock {lock_id} acquired by {requester}")

    async def _apply_lock_release(self, release_data: dict[str, object]) -> None:
        lock_id_obj = release_data.get("lock_id", "")
        lock_id = str(lock_id_obj) if isinstance(lock_id_obj, str) else ""
        holder_obj = release_data.get("holder", "")
        holder = str(holder_obj) if isinstance(holder_obj, str) else ""
        if lock_id in self.locks:
            lock = self.locks[lock_id]
            lock.holders.discard(holder)
            if not lock.holders:
                del self.locks[lock_id]
            logger.debug(f"Lock {lock_id} released by {holder}")

    async def _forward_to_leader(self, leader_id: str, request: LockRequest) -> bool:
        if leader_id not in self.node_addresses:
            return False
        host, port = self.node_addresses[leader_id]
        message: dict[str, object] = {
            "type": "lock_request",
            "lock_id": request.lock_id,
            "lock_type": request.lock_type.value,
            "requester": request.requester_id,
        }
        response = await self.message_passing.send_message(host, port, message)
        if response is None:
            return False
        success_obj = response.get("success", False)
        return bool(success_obj) if isinstance(success_obj, bool) else False

    async def _handle_lock_request(
        self, message: dict[str, object]
    ) -> dict[str, object]:
        lock_id_obj = message.get("lock_id", "")
        lock_id = str(lock_id_obj) if isinstance(lock_id_obj, str) else ""
        lock_type_obj = message.get("lock_type", "exclusive")
        lock_type_str = (
            str(lock_type_obj) if isinstance(lock_type_obj, str) else "exclusive"
        )
        requester_obj = message.get("requester", "")
        requester = str(requester_obj) if isinstance(requester_obj, str) else ""
        request = LockRequest(
            lock_id=lock_id, lock_type=LockType(lock_type_str), requester_id=requester
        )
        success = await self._process_lock_request(request)
        return {"type": "lock_response", "success": success}

    async def _handle_lock_release(
        self, message: dict[str, object]
    ) -> dict[str, object]:
        lock_id_obj = message.get("lock_id", "")
        lock_id = str(lock_id_obj) if isinstance(lock_id_obj, str) else ""
        success = await self.release_lock(lock_id)
        return {"type": "lock_response", "success": success}

    async def _cleanup_expired_locks(self) -> None:
        while self._running:
            expired = [
                lock_id for lock_id, lock in self.locks.items() if lock.is_expired()
            ]
            for lock_id in expired:
                logger.debug(f"Cleaning up expired lock {lock_id}")
                del self.locks[lock_id]
                self.metrics.increment_counter("locks_expired")
            await asyncio.sleep(5.0)


__all__ = ["LockManager", "LockType", "Lock", "LockRequest"]
