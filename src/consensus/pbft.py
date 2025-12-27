from __future__ import annotations

from asyncio import create_task, gather, get_event_loop, sleep
from dataclasses import dataclass, field
from enum import Enum, auto
from hashlib import sha256
from typing import TYPE_CHECKING, Any

from loguru import logger

from src.communication.message_passing import Message, MessageBus, MessageType

if TYPE_CHECKING:
    from collections.abc import Callable


class PBFTPhase(Enum):
    IDLE = auto()
    PRE_PREPARE = auto()
    PREPARE = auto()
    COMMIT = auto()
    REPLY = auto()


@dataclass(slots=True)
class PBFTMessage:
    view: int
    sequence: int
    digest: str
    node_id: str
    phase: PBFTPhase
    request: bytes | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "view": self.view,
            "sequence": self.sequence,
            "digest": self.digest,
            "node_id": self.node_id,
            "phase": self.phase.value,
            "request": self.request,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> PBFTMessage:
        return cls(
            view=data["view"],
            sequence=data["sequence"],
            digest=data["digest"],
            node_id=data["node_id"],
            phase=PBFTPhase(data["phase"]),
            request=data.get("request"),
        )


@dataclass(slots=True)
class RequestState:
    sequence: int
    digest: str
    request: bytes
    phase: PBFTPhase = PBFTPhase.IDLE
    pre_prepare_received: bool = False
    prepare_count: int = 0
    commit_count: int = 0
    prepared_nodes: set[str] = field(default_factory=set)
    committed_nodes: set[str] = field(default_factory=set)
    result: Any = None


class PBFTConsensus:
    def __init__(
        self,
        node_id: str,
        message_bus: MessageBus,
        peers: list[tuple[str, str, int]],
        is_primary: bool = False,
        on_commit: Callable[[bytes], Any] | None = None,
    ) -> None:
        self._node_id = node_id
        self._message_bus = message_bus
        self._peers: dict[str, tuple[str, int]] = {
            peer_id: (host, port) for peer_id, host, port in peers
        }
        self._is_primary = is_primary
        self._on_commit = on_commit

        self._view = 0
        self._sequence = 0
        self._requests: dict[str, RequestState] = {}
        self._n = len(peers) + 1
        self._f = (self._n - 1) // 3

        self._running = False
        self._register_handlers()

    @property
    def is_primary(self) -> bool:
        return self._is_primary

    @property
    def view(self) -> int:
        return self._view

    @property
    def fault_tolerance(self) -> int:
        return self._f

    def _register_handlers(self) -> None:
        self._message_bus.register_handler(
            MessageType.PRE_PREPARE,
            self._handle_pre_prepare,
        )
        self._message_bus.register_handler(
            MessageType.PREPARE,
            self._handle_prepare,
        )
        self._message_bus.register_handler(
            MessageType.COMMIT,
            self._handle_commit,
        )

    async def start(self) -> None:
        self._running = True
        logger.info(
            f"PBFT consensus started for node {self._node_id} "
            f"(n={self._n}, f={self._f}, primary={self._is_primary})"
        )

    async def stop(self) -> None:
        self._running = False
        logger.info(f"PBFT consensus stopped for node {self._node_id}")

    @staticmethod
    def _compute_digest(data: bytes) -> str:
        return sha256(data).hexdigest()

    async def propose(self, request: bytes) -> bool:
        if not self._is_primary:
            logger.warning("Only primary can propose requests")
            return False

        self._sequence += 1
        digest = self._compute_digest(request)

        state = RequestState(
            sequence=self._sequence,
            digest=digest,
            request=request,
            phase=PBFTPhase.PRE_PREPARE,
            pre_prepare_received=True,
        )
        self._requests[digest] = state

        pre_prepare = PBFTMessage(
            view=self._view,
            sequence=self._sequence,
            digest=digest,
            node_id=self._node_id,
            phase=PBFTPhase.PRE_PREPARE,
            request=request,
        )

        message = Message(
            msg_type=MessageType.PRE_PREPARE,
            sender_id=self._node_id,
            payload=pre_prepare.to_dict(),
        )

        await self._broadcast(message)
        await self._send_prepare(state)
        return True

    async def _broadcast(self, message: Message) -> list[Message | None]:
        tasks = [
            self._message_bus.send(host, port, message)
            for host, port in self._peers.values()
        ]
        return await gather(*tasks)

    async def _handle_pre_prepare(self, message: Message) -> Message | None:
        pbft_msg = PBFTMessage.from_dict(message.payload)

        if pbft_msg.view != self._view:
            return None

        if pbft_msg.request is None:
            return None

        computed_digest = self._compute_digest(pbft_msg.request)
        if computed_digest != pbft_msg.digest:
            logger.warning("Digest mismatch in pre-prepare")
            return None

        if pbft_msg.digest not in self._requests:
            self._requests[pbft_msg.digest] = RequestState(
                sequence=pbft_msg.sequence,
                digest=pbft_msg.digest,
                request=pbft_msg.request,
            )

        state = self._requests[pbft_msg.digest]
        state.pre_prepare_received = True
        state.phase = PBFTPhase.PRE_PREPARE

        await self._send_prepare(state)
        return None

    async def _send_prepare(self, state: RequestState) -> None:
        state.phase = PBFTPhase.PREPARE
        state.prepare_count += 1
        state.prepared_nodes.add(self._node_id)

        prepare = PBFTMessage(
            view=self._view,
            sequence=state.sequence,
            digest=state.digest,
            node_id=self._node_id,
            phase=PBFTPhase.PREPARE,
        )

        message = Message(
            msg_type=MessageType.PREPARE,
            sender_id=self._node_id,
            payload=prepare.to_dict(),
        )

        await self._broadcast(message)
        self._check_prepared(state)

    async def _handle_prepare(self, message: Message) -> Message | None:
        pbft_msg = PBFTMessage.from_dict(message.payload)

        if pbft_msg.view != self._view:
            return None

        state = self._requests.get(pbft_msg.digest)
        if not state:
            return None

        if pbft_msg.node_id not in state.prepared_nodes:
            state.prepared_nodes.add(pbft_msg.node_id)
            state.prepare_count += 1

        self._check_prepared(state)
        return None

    def _check_prepared(self, state: RequestState) -> None:
        if state.phase == PBFTPhase.COMMIT:
            return

        required = 2 * self._f + 1
        if state.pre_prepare_received and state.prepare_count >= required:
            create_task(self._send_commit(state))

    async def _send_commit(self, state: RequestState) -> None:
        state.phase = PBFTPhase.COMMIT
        state.commit_count += 1
        state.committed_nodes.add(self._node_id)

        commit = PBFTMessage(
            view=self._view,
            sequence=state.sequence,
            digest=state.digest,
            node_id=self._node_id,
            phase=PBFTPhase.COMMIT,
        )

        message = Message(
            msg_type=MessageType.COMMIT,
            sender_id=self._node_id,
            payload=commit.to_dict(),
        )

        await self._broadcast(message)
        self._check_committed(state)

    async def _handle_commit(self, message: Message) -> Message | None:
        pbft_msg = PBFTMessage.from_dict(message.payload)

        if pbft_msg.view != self._view:
            return None

        state = self._requests.get(pbft_msg.digest)
        if not state:
            return None

        if pbft_msg.node_id not in state.committed_nodes:
            state.committed_nodes.add(pbft_msg.node_id)
            state.commit_count += 1

        self._check_committed(state)
        return None

    def _check_committed(self, state: RequestState) -> None:
        if state.phase == PBFTPhase.REPLY:
            return

        required = 2 * self._f + 1
        if state.commit_count >= required:
            state.phase = PBFTPhase.REPLY
            self._execute_request(state)

    def _execute_request(self, state: RequestState) -> None:
        logger.info(f"Executing request with sequence {state.sequence}")
        if self._on_commit:
            state.result = self._on_commit(state.request)

    async def wait_for_commit(self, digest: str, timeout: float = 10.0) -> bool:
        start = get_event_loop().time()
        while get_event_loop().time() - start < timeout:
            state = self._requests.get(digest)
            if state and state.phase == PBFTPhase.REPLY:
                return True
            await sleep(0.01)
        return False

    def get_state_summary(self) -> dict[str, Any]:
        return {
            "node_id": self._node_id,
            "is_primary": self._is_primary,
            "view": self._view,
            "sequence": self._sequence,
            "n": self._n,
            "f": self._f,
            "pending_requests": len(
                [r for r in self._requests.values() if r.phase != PBFTPhase.REPLY]
            ),
            "completed_requests": len(
                [r for r in self._requests.values() if r.phase == PBFTPhase.REPLY]
            ),
        }
