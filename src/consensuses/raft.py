import asyncio
import random
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any

from loguru import logger

from . import ConsensusMessage, ConsensusProtocol, ConsensusState


@dataclass
class LogEntry:
    term: int
    index: int
    data: object
    committed: bool = False


@dataclass
class RaftConfig:
    election_timeout_min: int = 2000
    election_timeout_max: int = 10000
    heartbeat_interval: int = 500
    max_log_entries_per_request: int = 100


class RaftConsensus(ConsensusProtocol):
    config: RaftConfig
    send_message: (
        Callable[[str, ConsensusMessage], Coroutine[Any, Any, ConsensusMessage | None]]
        | None
    )
    voted_for: str | None
    log: list[LogEntry]
    commit_index: int
    last_applied: int
    next_index: dict[str, int]
    match_index: dict[str, int]
    leader_id: str | None
    votes_received: set[str]
    _election_task: asyncio.Task[None] | None
    _heartbeat_task: asyncio.Task[None] | None
    _apply_task: asyncio.Task[None] | None
    _running: bool
    _heartbeat_received: asyncio.Event

    def __init__(
        self,
        node_id: str,
        cluster_nodes: list[str],
        config: RaftConfig | None = None,
        send_message: Callable[
            [str, ConsensusMessage], Coroutine[Any, Any, ConsensusMessage | None]
        ]
        | None = None,
    ) -> None:
        super().__init__(node_id, cluster_nodes)
        self.config = config or RaftConfig()
        self.send_message = send_message
        self.voted_for = None
        self.log = []
        self.commit_index = 0
        self.last_applied = 0
        self.next_index = {}
        self.match_index = {}
        self.leader_id = None
        self.votes_received = set()
        self._election_task = None
        self._heartbeat_task = None
        self._apply_task = None
        self._running = False
        self._heartbeat_received = asyncio.Event()

    async def start(self) -> None:
        self._running = True
        self.state = ConsensusState.FOLLOWER
        self._election_task = asyncio.create_task(self._election_timeout_loop())
        self._apply_task = asyncio.create_task(self._apply_committed_entries())
        logger.info(f"Raft consensus started for node {self.node_id}")

    async def stop(self) -> None:
        self._running = False
        if self._election_task:
            self._election_task.cancel()
            try:
                await self._election_task
            except asyncio.CancelledError:
                pass
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass
        if self._apply_task:
            self._apply_task.cancel()
            try:
                await self._apply_task
            except asyncio.CancelledError:
                pass
        logger.info(f"Raft consensus stopped for node {self.node_id}")

    async def propose(self, data: object) -> bool:
        if not self.is_leader():
            logger.warning(f"Node {self.node_id} is not leader, cannot propose")
            return False
        entry = LogEntry(term=self.current_term, index=len(self.log) + 1, data=data)
        self.log.append(entry)
        logger.debug(f"Leader {self.node_id} proposed entry at index {entry.index}")
        if self.send_message:
            await self._replicate_log()
        return True

    async def handle_message(
        self, message: ConsensusMessage
    ) -> ConsensusMessage | None:
        if message.term > self.current_term:
            await self._step_down(message.term)
        if message.message_type == "request_vote":
            return await self._handle_request_vote(message)
        elif message.message_type == "request_vote_response":
            await self._handle_request_vote_response(message)
        elif message.message_type == "append_entries":
            return await self._handle_append_entries(message)
        elif message.message_type == "append_entries_response":
            await self._handle_append_entries_response(message)
        return None

    def is_leader(self) -> bool:
        return self.state == ConsensusState.LEADER

    def get_leader_id(self) -> str | None:
        return self.leader_id

    async def _election_timeout_loop(self) -> None:
        try:
            while self._running:
                timeout = random.uniform(
                    self.config.election_timeout_min / 1000,
                    self.config.election_timeout_max / 1000,
                )
                self._heartbeat_received.clear()
                try:
                    await asyncio.wait_for(
                        self._heartbeat_received.wait(), timeout=timeout
                    )
                except TimeoutError:
                    if self.state == ConsensusState.FOLLOWER and self._running:
                        await self._start_election()
        except Exception:
            logger.exception(f"Election timeout loop crashed for node {self.node_id}")
            raise

    async def _start_election(self) -> None:
        self.state = ConsensusState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.votes_received = {self.node_id}
        self.leader_id = None
        logger.info(
            f"Node {self.node_id} starting election for term {self.current_term}"
        )
        if not self.send_message:
            return
        last_log_index = len(self.log)
        last_log_term = self.log[-1].term if self.log else 0
        election_term = self.current_term

        async def request_vote_from_node(node: str) -> None:
            if node == self.node_id:
                return
            message = ConsensusMessage(
                message_type="request_vote",
                sender_id=self.node_id,
                term=election_term,
                data={"last_log_index": last_log_index, "last_log_term": last_log_term},
            )
            if self.send_message:
                response = await self.send_message(node, message)
                if response and response.message_type == "request_vote_response":
                    await self._handle_request_vote_response(response)

        tasks = [
            request_vote_from_node(node)
            for node in self.cluster_nodes
            if node != self.node_id
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

        if self.state != ConsensusState.CANDIDATE or self.current_term != election_term:
            return

    async def _handle_request_vote(self, message: ConsensusMessage) -> ConsensusMessage:
        vote_granted = False
        last_log_index_obj = message.data.get("last_log_index", 0)
        last_log_term_obj = message.data.get("last_log_term", 0)
        last_log_index = (
            int(last_log_index_obj) if isinstance(last_log_index_obj, int) else 0
        )
        last_log_term = (
            int(last_log_term_obj) if isinstance(last_log_term_obj, int) else 0
        )
        if message.term < self.current_term:
            vote_granted = False
        elif (
            self.voted_for is None or self.voted_for == message.sender_id
        ) and self._is_log_up_to_date(last_log_index, last_log_term):
            vote_granted = True
            self.voted_for = message.sender_id
            self._heartbeat_received.set()
            logger.debug(f"Node {self.node_id} voted for {message.sender_id}")
        return ConsensusMessage(
            message_type="request_vote_response",
            sender_id=self.node_id,
            term=self.current_term,
            data={"vote_granted": vote_granted},
        )

    async def _handle_request_vote_response(self, message: ConsensusMessage) -> None:
        if self.state != ConsensusState.CANDIDATE or message.term != self.current_term:
            return
        if message.data.get("vote_granted"):
            self.votes_received.add(message.sender_id)
            if len(self.votes_received) > len(self.cluster_nodes) // 2:
                await self._become_leader()

    async def _become_leader(self) -> None:
        self.state = ConsensusState.LEADER
        self.leader_id = self.node_id
        logger.info(f"Node {self.node_id} became leader for term {self.current_term}")
        for node in self.cluster_nodes:
            if node == self.node_id:
                continue
            self.next_index[node] = len(self.log) + 1
            self.match_index[node] = 0
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _step_down(self, new_term: int) -> None:
        logger.info(f"Node {self.node_id} stepping down to follower (term {new_term})")
        self.current_term = new_term
        self.state = ConsensusState.FOLLOWER
        self.voted_for = None
        self.leader_id = None
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            self._heartbeat_task = None

    async def _heartbeat_loop(self) -> None:
        while self._running and self.state == ConsensusState.LEADER:
            await self._replicate_log()
            await asyncio.sleep(self.config.heartbeat_interval / 1000)

    async def _replicate_log(self) -> None:
        if not self.send_message:
            return
        for node in self.cluster_nodes:
            if node == self.node_id:
                continue
            asyncio.create_task(self._send_append_entries(node))

    async def _send_append_entries(self, node_id: str) -> None:
        if not self.send_message:
            return
        next_idx = self.next_index.get(node_id, 1)
        prev_log_index = next_idx - 1
        prev_log_term = self.log[prev_log_index - 1].term if prev_log_index > 0 else 0
        entries = self.log[
            next_idx - 1 : next_idx - 1 + self.config.max_log_entries_per_request
        ]
        entries_data = [
            {"term": e.term, "index": e.index, "data": e.data} for e in entries
        ]
        message = ConsensusMessage(
            message_type="append_entries",
            sender_id=self.node_id,
            term=self.current_term,
            data={
                "prev_log_index": prev_log_index,
                "prev_log_term": prev_log_term,
                "entries": entries_data,
                "leader_commit": self.commit_index,
            },
        )
        if self.send_message:
            await self.send_message(node_id, message)

    async def _handle_append_entries(
        self, message: ConsensusMessage
    ) -> ConsensusMessage:
        success = False
        self.leader_id = message.sender_id
        self._heartbeat_received.set()
        prev_log_index_obj = message.data.get("prev_log_index", 0)
        prev_log_term_obj = message.data.get("prev_log_term", 0)
        entries_data_obj = message.data.get("entries", [])
        leader_commit_obj = message.data.get("leader_commit", 0)
        prev_log_index = (
            int(prev_log_index_obj) if isinstance(prev_log_index_obj, int) else 0
        )
        prev_log_term = (
            int(prev_log_term_obj) if isinstance(prev_log_term_obj, int) else 0
        )
        entries_data = (
            list(entries_data_obj) if isinstance(entries_data_obj, list) else []
        )
        leader_commit = (
            int(leader_commit_obj) if isinstance(leader_commit_obj, int) else 0
        )
        if message.term < self.current_term:
            success = False
        elif prev_log_index > 0 and (
            prev_log_index > len(self.log)
            or self.log[prev_log_index - 1].term != prev_log_term
        ):
            success = False
        else:
            success = True
            for entry_data in entries_data:
                if not isinstance(entry_data, dict):
                    continue
                entry = LogEntry(
                    term=int(entry_data.get("term", 0))
                    if isinstance(entry_data.get("term"), int)
                    else 0,
                    index=int(entry_data.get("index", 0))
                    if isinstance(entry_data.get("index"), int)
                    else 0,
                    data=entry_data.get("data", {}),
                )
                if entry.index <= len(self.log):
                    if self.log[entry.index - 1].term != entry.term:
                        self.log = self.log[: entry.index - 1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
            if leader_commit > self.commit_index:
                self.commit_index = min(leader_commit, len(self.log))
        return ConsensusMessage(
            message_type="append_entries_response",
            sender_id=self.node_id,
            term=self.current_term,
            data={"success": success, "match_index": len(self.log) if success else 0},
        )

    async def _handle_append_entries_response(self, message: ConsensusMessage) -> None:
        if self.state != ConsensusState.LEADER or message.term != self.current_term:
            return
        sender_id = message.sender_id
        success_obj = message.data.get("success", False)
        success = bool(success_obj) if isinstance(success_obj, bool) else False
        if success:
            match_index_obj = message.data.get("match_index", 0)
            match_index = (
                int(match_index_obj) if isinstance(match_index_obj, int) else 0
            )
            self.match_index[sender_id] = match_index
            self.next_index[sender_id] = match_index + 1
            await self._update_commit_index()
        else:
            self.next_index[sender_id] = max(1, self.next_index.get(sender_id, 1) - 1)

    async def _update_commit_index(self) -> None:
        for n in range(self.commit_index + 1, len(self.log) + 1):
            if self.log[n - 1].term != self.current_term:
                continue
            replicated_count = sum(
                (1 for match_idx in self.match_index.values() if match_idx >= n)
            )
            if replicated_count + 1 > len(self.cluster_nodes) // 2:
                self.commit_index = n
                logger.debug(f"Leader {self.node_id} committed index {n}")

    async def _apply_committed_entries(self) -> None:
        while self._running:
            if self.last_applied < self.commit_index:
                self.last_applied += 1
                entry = self.log[self.last_applied - 1]
                entry.committed = True
                entry_data = entry.data if isinstance(entry.data, dict) else {}
                await self._notify_committed(entry_data)
                logger.debug(f"Node {self.node_id} applied entry {self.last_applied}")
            await asyncio.sleep(0.01)

    def _is_log_up_to_date(
        self, candidate_last_log_index: int, candidate_last_log_term: int
    ) -> bool:
        if not self.log:
            return True
        last_log_term = self.log[-1].term
        last_log_index = len(self.log)
        if candidate_last_log_term != last_log_term:
            return candidate_last_log_term > last_log_term
        return candidate_last_log_index >= last_log_index


__all__ = ["RaftConsensus", "RaftConfig", "LogEntry"]
