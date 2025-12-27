from __future__ import annotations

from asyncio import Future, get_event_loop, wait_for
from asyncio import TimeoutError as AsyncTimeoutError
from collections import defaultdict
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from loguru import logger
from ormsgpack import packb, unpackb

from src.communication.message_passing import Message, MessageType
from src.consensus.raft import RaftConsensus, RaftRole
from src.nodes.base_node import BaseNode, NodeRole
from src.utils.config import Config
from src.utils.metrics import MetricsCollector, Timer

if TYPE_CHECKING:
    pass


class LockType(Enum):
    SHARED = auto()
    EXCLUSIVE = auto()


class LockState(Enum):
    FREE = auto()
    SHARED = auto()
    EXCLUSIVE = auto()


@dataclass(slots=True)
class LockHandle:
    lock_id: str
    resource_id: str
    lock_type: LockType
    owner_id: str
    fencing_token: int
    acquired_at: float
    expires_at: float


@dataclass(slots=True)
class LockInfo:
    resource_id: str
    state: LockState = LockState.FREE
    exclusive_owner: str | None = None
    shared_owners: set[str] = field(default_factory=set)
    fencing_token: int = 0
    wait_queue: list[tuple[str, LockType, Future[LockHandle | None]]] = field(
        default_factory=list
    )


@dataclass(slots=True)
class LockCommand:
    action: str
    resource_id: str
    lock_type: str
    owner_id: str
    lock_id: str
    timeout: float = 30.0


class DeadlockDetector:
    def __init__(self) -> None:
        self._wait_for: dict[str, set[str]] = defaultdict(set)

    def add_wait(self, waiter: str, holders: set[str]) -> None:
        self._wait_for[waiter] = holders

    def remove_wait(self, waiter: str) -> None:
        self._wait_for.pop(waiter, None)

    def detect_deadlock(self, start: str) -> list[str] | None:
        visited: set[str] = set()
        path: list[str] = []

        def dfs(node: str) -> bool:
            if node in visited:
                return False
            if node in path:
                return True
            path.append(node)
            for next_node in self._wait_for.get(node, set()):
                if dfs(next_node):
                    return True
            path.pop()
            visited.add(node)
            return False

        if dfs(start):
            return path
        return None

    def has_cycle(self) -> bool:
        for node in self._wait_for:
            if self.detect_deadlock(node):
                return True
        return False


class LockManager(BaseNode):
    def __init__(self, config: Config) -> None:
        super().__init__(config, NodeRole.LOCK_MANAGER)
        self._locks: dict[str, LockInfo] = {}
        self._deadlock_detector = DeadlockDetector()
        self._raft: RaftConsensus | None = None
        self._pending_locks: dict[str, Future[LockHandle | None]] = {}
        self._lock_timeout = 30.0
        self._metrics = MetricsCollector()

    async def _on_start(self) -> None:
        peers = [(p.node_id, p.host, p.port) for p in self._config.get_peer_nodes()]
        self._raft = RaftConsensus(
            node_id=self.node_id,
            message_bus=self._message_bus,
            peers=peers,
            election_timeout_min=self._config.node.election_timeout_min,
            election_timeout_max=self._config.node.election_timeout_max,
            heartbeat_interval=self._config.node.heartbeat_interval,
            on_state_change=self._on_raft_state_change,
            on_commit=self._on_raft_commit,
        )
        await self._raft.start()

    async def _on_stop(self) -> None:
        if self._raft:
            await self._raft.stop()
        for future in self._pending_locks.values():
            if not future.done():
                future.set_result(None)

    def _register_handlers(self) -> None:
        super()._register_handlers()
        self._message_bus.register_handler(
            MessageType.LOCK_ACQUIRE,
            self._handle_lock_acquire,
        )
        self._message_bus.register_handler(
            MessageType.LOCK_RELEASE,
            self._handle_lock_release,
        )

    def _on_raft_state_change(self, role: RaftRole, term: int) -> None:
        self._metrics.update_consensus(self.node_id, term, role.value)
        logger.info(f"Lock manager role changed to {role.name} in term {term}")

    def _on_raft_commit(self, command_bytes: bytes) -> None:
        try:
            data = unpackb(command_bytes)
            cmd = LockCommand(
                action=data["action"],
                resource_id=data["resource_id"],
                lock_type=data["lock_type"],
                owner_id=data["owner_id"],
                lock_id=data["lock_id"],
                timeout=data.get("timeout", 30.0),
            )
            self._apply_lock_command(cmd)
        except Exception as e:
            logger.error(f"Failed to apply lock command: {e}")

    def _apply_lock_command(self, cmd: LockCommand) -> None:
        if cmd.resource_id not in self._locks:
            self._locks[cmd.resource_id] = LockInfo(resource_id=cmd.resource_id)

        lock_info = self._locks[cmd.resource_id]
        lock_type = LockType[cmd.lock_type]

        if cmd.action == "acquire":
            handle = self._try_acquire_internal(
                lock_info,
                lock_type,
                cmd.owner_id,
                cmd.lock_id,
                cmd.timeout,
            )
            if cmd.lock_id in self._pending_locks:
                future = self._pending_locks.pop(cmd.lock_id)
                if not future.done():
                    future.set_result(handle)

        elif cmd.action == "release":
            self._release_internal(lock_info, cmd.owner_id, cmd.lock_id)

    def _try_acquire_internal(
        self,
        lock_info: LockInfo,
        lock_type: LockType,
        owner_id: str,
        lock_id: str,
        timeout: float,
    ) -> LockHandle | None:
        loop = get_event_loop()
        now = loop.time()

        if lock_type == LockType.EXCLUSIVE:
            if lock_info.state == LockState.FREE:
                lock_info.state = LockState.EXCLUSIVE
                lock_info.exclusive_owner = owner_id
                lock_info.fencing_token += 1
                self._metrics.record_lock(self.node_id, "exclusive", 1)
                return LockHandle(
                    lock_id=lock_id,
                    resource_id=lock_info.resource_id,
                    lock_type=lock_type,
                    owner_id=owner_id,
                    fencing_token=lock_info.fencing_token,
                    acquired_at=now,
                    expires_at=now + timeout,
                )
        elif lock_type == LockType.SHARED:
            if lock_info.state in (LockState.FREE, LockState.SHARED):
                lock_info.state = LockState.SHARED
                lock_info.shared_owners.add(owner_id)
                lock_info.fencing_token += 1
                self._metrics.record_lock(self.node_id, "shared", 1)
                return LockHandle(
                    lock_id=lock_id,
                    resource_id=lock_info.resource_id,
                    lock_type=lock_type,
                    owner_id=owner_id,
                    fencing_token=lock_info.fencing_token,
                    acquired_at=now,
                    expires_at=now + timeout,
                )
        return None

    def _release_internal(
        self,
        lock_info: LockInfo,
        owner_id: str,
        lock_id: str,
    ) -> bool:
        if lock_info.state == LockState.EXCLUSIVE:
            if lock_info.exclusive_owner == owner_id:
                lock_info.state = LockState.FREE
                lock_info.exclusive_owner = None
                self._metrics.record_lock(self.node_id, "exclusive", -1)
                self._process_wait_queue(lock_info)
                return True
        elif lock_info.state == LockState.SHARED:
            if owner_id in lock_info.shared_owners:
                lock_info.shared_owners.discard(owner_id)
                self._metrics.record_lock(self.node_id, "shared", -1)
                if not lock_info.shared_owners:
                    lock_info.state = LockState.FREE
                    self._process_wait_queue(lock_info)
                return True
        return False

    def _process_wait_queue(self, lock_info: LockInfo) -> None:
        if not lock_info.wait_queue:
            return

        owner_id, lock_type, future = lock_info.wait_queue[0]
        lock_id = str(uuid4())
        handle = self._try_acquire_internal(
            lock_info,
            lock_type,
            owner_id,
            lock_id,
            self._lock_timeout,
        )
        if handle:
            lock_info.wait_queue.pop(0)
            self._deadlock_detector.remove_wait(owner_id)
            if not future.done():
                future.set_result(handle)

    async def acquire_lock(
        self,
        resource_id: str,
        lock_type: LockType,
        timeout: float = 30.0,
        client_id: str | None = None,
    ) -> LockHandle | None:
        if not self._raft or not self._raft.is_leader:
            logger.warning("Not leader, cannot acquire lock")
            return None

        owner_id = client_id or self.node_id
        lock_id = str(uuid4())
        lock_info = self._locks.get(resource_id)

        if lock_info and lock_info.state != LockState.FREE:
            if (
                lock_type == LockType.EXCLUSIVE
                or lock_info.state == LockState.EXCLUSIVE
            ):
                holders = set()
                if lock_info.exclusive_owner:
                    holders.add(lock_info.exclusive_owner)
                holders.update(lock_info.shared_owners)

                self._deadlock_detector.add_wait(owner_id, holders)
                cycle = self._deadlock_detector.detect_deadlock(owner_id)
                if cycle:
                    logger.warning(f"Deadlock detected: {cycle}")
                    self._deadlock_detector.remove_wait(owner_id)
                    return None

        cmd = LockCommand(
            action="acquire",
            resource_id=resource_id,
            lock_type=lock_type.name,
            owner_id=owner_id,
            lock_id=lock_id,
            timeout=timeout,
        )

        command_bytes = packb(
            {
                "action": cmd.action,
                "resource_id": cmd.resource_id,
                "lock_type": cmd.lock_type,
                "owner_id": cmd.owner_id,
                "lock_id": cmd.lock_id,
                "timeout": cmd.timeout,
            }
        )

        future: Future[LockHandle | None] = Future()
        self._pending_locks[lock_id] = future

        with Timer(
            lambda t: self._metrics.record_request(
                self.node_id, "lock_acquire", "success", t
            )
        ):
            if not await self._raft.propose(command_bytes):
                self._pending_locks.pop(lock_id, None)
                return None

            try:
                return await wait_for(future, timeout=timeout)
            except AsyncTimeoutError:
                self._pending_locks.pop(lock_id, None)
                return None

    async def release_lock(self, handle: LockHandle) -> bool:
        if not self._raft or not self._raft.is_leader:
            return False

        cmd = LockCommand(
            action="release",
            resource_id=handle.resource_id,
            lock_type=handle.lock_type.name,
            owner_id=handle.owner_id,
            lock_id=handle.lock_id,
        )

        command_bytes = packb(
            {
                "action": cmd.action,
                "resource_id": cmd.resource_id,
                "lock_type": cmd.lock_type,
                "owner_id": cmd.owner_id,
                "lock_id": cmd.lock_id,
                "timeout": cmd.timeout,
            }
        )

        return await self._raft.propose(command_bytes)

    async def try_acquire(
        self,
        resource_id: str,
        lock_type: LockType,
        client_id: str | None = None,
    ) -> LockHandle | None:
        return await self.acquire_lock(
            resource_id, lock_type, timeout=0.1, client_id=client_id
        )

    async def _handle_lock_acquire(self, message: Message) -> Message:
        resource_id = message.payload["resource_id"]
        lock_type = LockType[message.payload["lock_type"]]
        timeout = message.payload.get("timeout", 30.0)
        client_id = message.sender_id

        handle = await self.acquire_lock(resource_id, lock_type, timeout, client_id)
        if handle:
            return Message(
                msg_type=MessageType.LOCK_RESPONSE,
                sender_id=self.node_id,
                payload={
                    "success": True,
                    "lock_id": handle.lock_id,
                    "fencing_token": handle.fencing_token,
                    "expires_at": handle.expires_at,
                },
            )
        return Message(
            msg_type=MessageType.LOCK_RESPONSE,
            sender_id=self.node_id,
            payload={"success": False},
        )

    async def _handle_lock_release(self, message: Message) -> Message:
        lock_type = LockType[message.payload["lock_type"]]
        handle = LockHandle(
            lock_id=message.payload["lock_id"],
            resource_id=message.payload["resource_id"],
            lock_type=lock_type,
            owner_id=message.sender_id,
            fencing_token=message.payload.get("fencing_token", 0),
            acquired_at=0,
            expires_at=0,
        )
        success = await self.release_lock(handle)
        return Message(
            msg_type=MessageType.LOCK_RESPONSE,
            sender_id=self.node_id,
            payload={"success": success},
        )

    def get_lock_info(self, resource_id: str) -> dict[str, Any] | None:
        lock_info = self._locks.get(resource_id)
        if not lock_info:
            return None
        return {
            "resource_id": lock_info.resource_id,
            "state": lock_info.state.name,
            "exclusive_owner": lock_info.exclusive_owner,
            "shared_owners": list(lock_info.shared_owners),
            "fencing_token": lock_info.fencing_token,
            "queue_length": len(lock_info.wait_queue),
        }

    def get_all_locks(self) -> dict[str, dict[str, Any]]:
        result: dict[str, dict[str, Any]] = {}
        for rid in self._locks:
            info = self.get_lock_info(rid)
            if info is not None:
                result[rid] = info
        return result
