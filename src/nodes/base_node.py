from __future__ import annotations

from abc import ABC, abstractmethod
from asyncio import AbstractEventLoop, get_running_loop
from dataclasses import dataclass
from enum import Enum, auto
from typing import TYPE_CHECKING

from loguru import logger

from src.communication.failure_detector import FailureDetector, NodeStatus
from src.communication.message_passing import Message, MessageBus, MessageType
from src.utils.config import Config
from src.utils.metrics import MetricsCollector

if TYPE_CHECKING:
    pass


class NodeRole(Enum):
    LOCK_MANAGER = auto()
    QUEUE_NODE = auto()
    CACHE_NODE = auto()


class NodeState(Enum):
    INITIALIZING = auto()
    RUNNING = auto()
    STOPPING = auto()
    STOPPED = auto()


@dataclass(frozen=True, slots=True)
class NodeInfo:
    node_id: str
    host: str
    port: int
    role: NodeRole


class BaseNode(ABC):
    def __init__(self, config: Config, role: NodeRole) -> None:
        self._config = config
        self._role = role
        self._state = NodeState.INITIALIZING
        self._message_bus = MessageBus(
            node_id=config.node.node_id,
            host=config.node.host,
            port=config.node.port,
            request_timeout=config.node.request_timeout,
        )
        self._failure_detector = FailureDetector(
            node_id=config.node.node_id,
            heartbeat_interval=config.node.heartbeat_interval / 1000.0,
        )
        self._metrics = MetricsCollector()
        self._peers: list[NodeInfo] = []
        self._loop: AbstractEventLoop | None = None

    @property
    def node_id(self) -> str:
        return self._config.node.node_id

    @property
    def role(self) -> NodeRole:
        return self._role

    @property
    def state(self) -> NodeState:
        return self._state

    @property
    def config(self) -> Config:
        return self._config

    @property
    def message_bus(self) -> MessageBus:
        return self._message_bus

    @property
    def failure_detector(self) -> FailureDetector:
        return self._failure_detector

    async def start(self) -> None:
        self._loop = get_running_loop()
        self._state = NodeState.INITIALIZING
        logger.info(f"Starting node {self.node_id} as {self._role.name}")

        for peer in self._config.get_peer_nodes():
            self._peers.append(
                NodeInfo(
                    node_id=peer.node_id,
                    host=peer.host,
                    port=peer.port,
                    role=self._role,
                )
            )
            self._failure_detector.add_node(peer.node_id, peer.host, peer.port)

        self._register_handlers()
        await self._message_bus.start()
        await self._failure_detector.start()

        self._failure_detector.set_status_callback(self._on_peer_status_change)

        await self._on_start()
        self._state = NodeState.RUNNING
        logger.info(f"Node {self.node_id} started successfully")

    async def stop(self) -> None:
        logger.info(f"Stopping node {self.node_id}")
        self._state = NodeState.STOPPING
        await self._on_stop()
        await self._failure_detector.stop()
        await self._message_bus.stop()
        self._state = NodeState.STOPPED
        logger.info(f"Node {self.node_id} stopped")

    def _register_handlers(self) -> None:
        self._message_bus.register_handler(MessageType.PING, self._handle_ping)

    async def _handle_ping(self, message: Message) -> Message:
        return Message(
            msg_type=MessageType.PONG,
            sender_id=self.node_id,
            payload={"status": "ok"},
        )

    def _on_peer_status_change(self, peer_id: str, status: NodeStatus) -> None:
        logger.info(f"Peer {peer_id} status changed to {status.name}")
        self._metrics.update_cluster_health(
            self.node_id,
            self._failure_detector.alive_count,
        )

    def get_peer_targets(self) -> list[tuple[str, int]]:
        return [(p.host, p.port) for p in self._peers]

    def get_alive_peers(self) -> list[NodeInfo]:
        alive_ids = set(self._failure_detector.get_alive_nodes())
        return [p for p in self._peers if p.node_id in alive_ids]

    async def send_to_peer(
        self,
        peer_id: str,
        message: Message,
    ) -> Message | None:
        for peer in self._peers:
            if peer.node_id == peer_id:
                return await self._message_bus.send(peer.host, peer.port, message)
        return None

    async def broadcast_to_peers(self, message: Message) -> list[Message | None]:
        return await self._message_bus.broadcast(self.get_peer_targets(), message)

    @abstractmethod
    async def _on_start(self) -> None:
        pass

    @abstractmethod
    async def _on_stop(self) -> None:
        pass
