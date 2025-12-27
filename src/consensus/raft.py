from __future__ import annotations

from asyncio import Task, create_task, get_event_loop, sleep
from dataclasses import dataclass, field
from enum import Enum
from random import randint
from typing import TYPE_CHECKING, Any

from loguru import logger

from src.communication.message_passing import Message, MessageBus, MessageType

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence


class RaftRole(Enum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


@dataclass(slots=True)
class LogEntry:
    term: int
    index: int
    command: bytes

    def to_dict(self) -> dict[str, Any]:
        return {
            "term": self.term,
            "index": self.index,
            "command": self.command,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> LogEntry:
        return cls(
            term=data["term"],
            index=data["index"],
            command=data["command"],
        )


@dataclass(slots=True)
class RaftState:
    current_term: int = 0
    voted_for: str | None = None
    log: list[LogEntry] = field(default_factory=list)
    commit_index: int = 0
    last_applied: int = 0


@dataclass(slots=True)
class PeerState:
    node_id: str
    next_index: int = 1
    match_index: int = 0


class RaftConsensus:
    def __init__(
        self,
        node_id: str,
        message_bus: MessageBus,
        peers: list[tuple[str, str, int]],
        election_timeout_min: int = 150,
        election_timeout_max: int = 300,
        heartbeat_interval: int = 50,
        on_state_change: Callable[[RaftRole, int], None] | None = None,
        on_commit: Callable[[bytes], None] | None = None,
    ) -> None:
        self._node_id = node_id
        self._message_bus = message_bus
        self._peers: dict[str, tuple[str, int]] = {
            peer_id: (host, port) for peer_id, host, port in peers
        }
        self._peer_states: dict[str, PeerState] = {
            peer_id: PeerState(node_id=peer_id) for peer_id, _, _ in peers
        }
        self._election_timeout_min = election_timeout_min
        self._election_timeout_max = election_timeout_max
        self._heartbeat_interval = heartbeat_interval / 1000.0
        self._on_state_change = on_state_change
        self._on_commit = on_commit

        self._state = RaftState()
        self._role = RaftRole.FOLLOWER
        self._leader_id: str | None = None
        self._votes_received: set[str] = set()

        self._election_timer: Task[None] | None = None
        self._heartbeat_task: Task[None] | None = None
        self._running = False

        self._register_handlers()

    @property
    def role(self) -> RaftRole:
        return self._role

    @property
    def term(self) -> int:
        return self._state.current_term

    @property
    def leader_id(self) -> str | None:
        return self._leader_id

    @property
    def is_leader(self) -> bool:
        return self._role == RaftRole.LEADER

    @property
    def commit_index(self) -> int:
        return self._state.commit_index

    def _register_handlers(self) -> None:
        self._message_bus.register_handler(
            MessageType.REQUEST_VOTE,
            self._handle_request_vote,
        )
        self._message_bus.register_handler(
            MessageType.VOTE_RESPONSE,
            self._handle_vote_response,
        )
        self._message_bus.register_handler(
            MessageType.APPEND_ENTRIES,
            self._handle_append_entries,
        )
        self._message_bus.register_handler(
            MessageType.APPEND_RESPONSE,
            self._handle_append_response,
        )

    async def start(self) -> None:
        self._running = True
        self._reset_election_timer()
        logger.info(f"Raft consensus started for node {self._node_id}")

    async def stop(self) -> None:
        self._running = False
        if self._election_timer:
            self._election_timer.cancel()
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        logger.info(f"Raft consensus stopped for node {self._node_id}")

    def _get_election_timeout(self) -> float:
        return (
            randint(
                self._election_timeout_min,
                self._election_timeout_max,
            )
            / 1000.0
        )

    def _reset_election_timer(self) -> None:
        if self._election_timer:
            self._election_timer.cancel()
        if self._running:
            timeout = self._get_election_timeout()
            self._election_timer = create_task(self._election_timeout_handler(timeout))

    async def _election_timeout_handler(self, timeout: float) -> None:
        await sleep(timeout)
        if self._running and self._role != RaftRole.LEADER:
            await self._start_election()

    async def _start_election(self) -> None:
        self._state.current_term += 1
        self._role = RaftRole.CANDIDATE
        self._state.voted_for = self._node_id
        self._votes_received = {self._node_id}
        self._leader_id = None

        logger.info(
            f"Node {self._node_id} starting election for term {self._state.current_term}"
        )

        if self._on_state_change:
            self._on_state_change(self._role, self._state.current_term)

        last_log_index = len(self._state.log)
        last_log_term = self._state.log[-1].term if self._state.log else 0

        message = Message(
            msg_type=MessageType.REQUEST_VOTE,
            sender_id=self._node_id,
            term=self._state.current_term,
            payload={
                "last_log_index": last_log_index,
                "last_log_term": last_log_term,
            },
        )

        for peer_id, (host, port) in self._peers.items():
            create_task(self._send_vote_request(peer_id, host, port, message))

        self._reset_election_timer()

    async def _send_vote_request(
        self,
        peer_id: str,
        host: str,
        port: int,
        message: Message,
    ) -> None:
        response = await self._message_bus.send(host, port, message)
        if response:
            await self._process_vote_response(peer_id, response)

    async def _process_vote_response(self, peer_id: str, response: Message) -> None:
        if response.term > self._state.current_term:
            self._become_follower(response.term)
            return

        if (
            self._role == RaftRole.CANDIDATE
            and response.term == self._state.current_term
            and response.payload.get("vote_granted")
        ):
            self._votes_received.add(peer_id)
            if len(self._votes_received) > (len(self._peers) + 1) // 2:
                await self._become_leader()

    async def _handle_request_vote(self, message: Message) -> Message:
        if message.term > self._state.current_term:
            self._become_follower(message.term)

        vote_granted = False
        if message.term >= self._state.current_term:
            last_log_index = len(self._state.log)
            last_log_term = self._state.log[-1].term if self._state.log else 0

            candidate_log_ok = message.payload["last_log_term"] > last_log_term or (
                message.payload["last_log_term"] == last_log_term
                and message.payload["last_log_index"] >= last_log_index
            )

            if (
                self._state.voted_for is None
                or self._state.voted_for == message.sender_id
            ) and candidate_log_ok:
                self._state.voted_for = message.sender_id
                vote_granted = True
                self._reset_election_timer()

        return Message(
            msg_type=MessageType.VOTE_RESPONSE,
            sender_id=self._node_id,
            term=self._state.current_term,
            payload={"vote_granted": vote_granted},
        )

    async def _handle_vote_response(self, message: Message) -> Message | None:
        await self._process_vote_response(message.sender_id, message)
        return None

    async def _become_leader(self) -> None:
        if self._role == RaftRole.LEADER:
            return

        self._role = RaftRole.LEADER
        self._leader_id = self._node_id
        logger.info(
            f"Node {self._node_id} became leader for term {self._state.current_term}"
        )

        if self._on_state_change:
            self._on_state_change(self._role, self._state.current_term)

        for peer_state in self._peer_states.values():
            peer_state.next_index = len(self._state.log) + 1
            peer_state.match_index = 0

        if self._election_timer:
            self._election_timer.cancel()

        self._heartbeat_task = create_task(self._heartbeat_loop())

    def _become_follower(self, term: int) -> None:
        self._state.current_term = term
        self._role = RaftRole.FOLLOWER
        self._state.voted_for = None
        self._votes_received = set()

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

        if self._on_state_change:
            self._on_state_change(self._role, self._state.current_term)

        self._reset_election_timer()

    async def _heartbeat_loop(self) -> None:
        while self._running and self._role == RaftRole.LEADER:
            await self._send_heartbeats()
            await sleep(self._heartbeat_interval)

    async def _send_heartbeats(self) -> None:
        for peer_id, (host, port) in self._peers.items():
            create_task(self._send_append_entries(peer_id, host, port))

    async def _send_append_entries(
        self,
        peer_id: str,
        host: str,
        port: int,
    ) -> None:
        peer_state = self._peer_states.get(peer_id)
        if not peer_state:
            return

        prev_log_index = peer_state.next_index - 1
        prev_log_term = 0
        if prev_log_index > 0 and prev_log_index <= len(self._state.log):
            prev_log_term = self._state.log[prev_log_index - 1].term

        entries = [e.to_dict() for e in self._state.log[peer_state.next_index - 1 :]]

        message = Message(
            msg_type=MessageType.APPEND_ENTRIES,
            sender_id=self._node_id,
            term=self._state.current_term,
            payload={
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries,
                "leader_commit": self._state.commit_index,
            },
        )

        response = await self._message_bus.send(host, port, message)
        if response:
            await self._process_append_response(peer_id, response)

    async def _handle_append_entries(self, message: Message) -> Message:
        if message.term < self._state.current_term:
            return Message(
                msg_type=MessageType.APPEND_RESPONSE,
                sender_id=self._node_id,
                term=self._state.current_term,
                payload={"success": False, "match_index": 0},
            )

        if message.term > self._state.current_term:
            self._become_follower(message.term)

        self._leader_id = message.sender_id
        self._reset_election_timer()

        prev_log_index = message.payload["prev_log_index"]
        prev_log_term = message.payload["prev_log_term"]

        if prev_log_index > 0:
            if prev_log_index > len(self._state.log):
                return Message(
                    msg_type=MessageType.APPEND_RESPONSE,
                    sender_id=self._node_id,
                    term=self._state.current_term,
                    payload={"success": False, "match_index": len(self._state.log)},
                )
            if self._state.log[prev_log_index - 1].term != prev_log_term:
                self._state.log = self._state.log[: prev_log_index - 1]
                return Message(
                    msg_type=MessageType.APPEND_RESPONSE,
                    sender_id=self._node_id,
                    term=self._state.current_term,
                    payload={"success": False, "match_index": len(self._state.log)},
                )

        entries = [LogEntry.from_dict(e) for e in message.payload["entries"]]
        for entry in entries:
            if entry.index <= len(self._state.log):
                if self._state.log[entry.index - 1].term != entry.term:
                    self._state.log = self._state.log[: entry.index - 1]
                    self._state.log.append(entry)
            else:
                self._state.log.append(entry)

        if message.payload["leader_commit"] > self._state.commit_index:
            self._state.commit_index = min(
                message.payload["leader_commit"],
                len(self._state.log),
            )
            self._apply_committed_entries()

        return Message(
            msg_type=MessageType.APPEND_RESPONSE,
            sender_id=self._node_id,
            term=self._state.current_term,
            payload={"success": True, "match_index": len(self._state.log)},
        )

    async def _process_append_response(
        self,
        peer_id: str,
        response: Message,
    ) -> None:
        if response.term > self._state.current_term:
            self._become_follower(response.term)
            return

        peer_state = self._peer_states.get(peer_id)
        if not peer_state:
            return

        if response.payload["success"]:
            peer_state.match_index = response.payload["match_index"]
            peer_state.next_index = peer_state.match_index + 1
            self._try_advance_commit_index()
        else:
            peer_state.next_index = max(1, response.payload["match_index"] + 1)

    async def _handle_append_response(self, message: Message) -> Message | None:
        await self._process_append_response(message.sender_id, message)
        return None

    def _try_advance_commit_index(self) -> None:
        if self._role != RaftRole.LEADER:
            return

        for n in range(self._state.commit_index + 1, len(self._state.log) + 1):
            if self._state.log[n - 1].term != self._state.current_term:
                continue

            match_count = 1
            for peer_state in self._peer_states.values():
                if peer_state.match_index >= n:
                    match_count += 1

            if match_count > (len(self._peers) + 1) // 2:
                self._state.commit_index = n
                self._apply_committed_entries()

    def _apply_committed_entries(self) -> None:
        while self._state.last_applied < self._state.commit_index:
            self._state.last_applied += 1
            entry = self._state.log[self._state.last_applied - 1]
            if self._on_commit:
                self._on_commit(entry.command)

    async def propose(self, command: bytes) -> bool:
        if not self.is_leader:
            return False

        entry = LogEntry(
            term=self._state.current_term,
            index=len(self._state.log) + 1,
            command=command,
        )
        self._state.log.append(entry)
        logger.debug(f"Proposed entry at index {entry.index}")
        return True

    async def wait_for_commit(self, index: int, timeout: float = 5.0) -> bool:
        start = get_event_loop().time()
        while get_event_loop().time() - start < timeout:
            if self._state.commit_index >= index:
                return True
            await sleep(0.01)
        return False

    def get_log_entries(self) -> Sequence[LogEntry]:
        return self._state.log

    def get_state_summary(self) -> dict[str, Any]:
        return {
            "node_id": self._node_id,
            "role": self._role.name,
            "term": self._state.current_term,
            "leader_id": self._leader_id,
            "commit_index": self._state.commit_index,
            "last_applied": self._state.last_applied,
            "log_length": len(self._state.log),
        }
