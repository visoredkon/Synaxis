from __future__ import annotations

from asyncio import (
    CancelledError,
    Task,
    create_task,
    get_event_loop,
    sleep,
)
from asyncio import (
    TimeoutError as AsyncTimeoutError,
)
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import TYPE_CHECKING

from aiohttp import ClientError, ClientSession, ClientTimeout
from loguru import logger

if TYPE_CHECKING:
    from collections.abc import Callable


class NodeStatus(Enum):
    ALIVE = auto()
    SUSPECTED = auto()
    DEAD = auto()


@dataclass(slots=True)
class NodeHealth:
    node_id: str
    host: str
    port: int
    status: NodeStatus = NodeStatus.ALIVE
    last_heartbeat: float = 0.0
    missed_heartbeats: int = 0


@dataclass(slots=True)
class FailureDetector:
    node_id: str
    heartbeat_interval: float = 0.5
    suspicion_threshold: int = 3
    dead_threshold: int = 5
    _nodes: dict[str, NodeHealth] = field(default_factory=dict)
    _running: bool = False
    _task: Task[None] | None = None
    _session: ClientSession | None = None
    _on_status_change: Callable[[str, NodeStatus], None] | None = None

    def add_node(self, node_id: str, host: str, port: int) -> None:
        if node_id != self.node_id and node_id not in self._nodes:
            self._nodes[node_id] = NodeHealth(
                node_id=node_id,
                host=host,
                port=port,
                last_heartbeat=get_event_loop().time(),
            )

    def remove_node(self, node_id: str) -> None:
        self._nodes.pop(node_id, None)

    def set_status_callback(
        self,
        callback: Callable[[str, NodeStatus], None],
    ) -> None:
        self._on_status_change = callback

    async def start(self) -> None:
        self._session = ClientSession(timeout=ClientTimeout(total=1.0))
        self._running = True
        self._task = create_task(self._heartbeat_loop())
        logger.info("FailureDetector started")

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except CancelledError:
                pass
        if self._session:
            await self._session.close()
        logger.info("FailureDetector stopped")

    async def _heartbeat_loop(self) -> None:
        while self._running:
            await self._check_nodes()
            await sleep(self.heartbeat_interval)

    async def _check_nodes(self) -> None:
        current_time = get_event_loop().time()
        for health in list(self._nodes.values()):
            is_alive = await self._ping_node(health)
            if is_alive:
                health.last_heartbeat = current_time
                health.missed_heartbeats = 0
                if health.status != NodeStatus.ALIVE:
                    self._update_status(health, NodeStatus.ALIVE)
            else:
                health.missed_heartbeats += 1
                if health.missed_heartbeats >= self.dead_threshold:
                    if health.status != NodeStatus.DEAD:
                        self._update_status(health, NodeStatus.DEAD)
                elif health.missed_heartbeats >= self.suspicion_threshold:
                    if health.status != NodeStatus.SUSPECTED:
                        self._update_status(health, NodeStatus.SUSPECTED)

    async def _ping_node(self, health: NodeHealth) -> bool:
        if not self._session:
            return False
        try:
            url = f"http://{health.host}:{health.port}/health"
            async with self._session.get(url) as resp:
                return resp.status == 200
        except (ClientError, AsyncTimeoutError):
            return False

    def _update_status(self, health: NodeHealth, new_status: NodeStatus) -> None:
        old_status = health.status
        health.status = new_status
        logger.info(
            f"Node {health.node_id} status changed: {old_status.name} -> {new_status.name}"
        )
        if self._on_status_change:
            self._on_status_change(health.node_id, new_status)

    def get_node_status(self, node_id: str) -> NodeStatus | None:
        health = self._nodes.get(node_id)
        return health.status if health else None

    def get_alive_nodes(self) -> list[str]:
        return [h.node_id for h in self._nodes.values() if h.status == NodeStatus.ALIVE]

    def get_all_nodes(self) -> dict[str, NodeStatus]:
        return {h.node_id: h.status for h in self._nodes.values()}

    @property
    def alive_count(self) -> int:
        return len(self.get_alive_nodes())
