import asyncio
import time
from collections.abc import Awaitable, Callable

from loguru import logger


class FailureDetector:
    node_id: str
    cluster_nodes: list[str]
    heartbeat_interval: float
    timeout: float
    last_heartbeat: dict[str, float]
    failed_nodes: set[str]
    _running: bool
    _detector_task: asyncio.Task[None] | None
    failure_callbacks: list[Callable[[str], Awaitable[None]]]
    recovery_callbacks: list[Callable[[str], Awaitable[None]]]

    def __init__(
        self,
        node_id: str,
        cluster_nodes: list[str],
        heartbeat_interval: float = 1.0,
        timeout: float = 5.0,
    ) -> None:
        self.node_id = node_id
        self.cluster_nodes = cluster_nodes
        self.heartbeat_interval = heartbeat_interval
        self.timeout = timeout
        self.last_heartbeat = {}
        self.failed_nodes = set()
        self._running = False
        self._detector_task = None
        self.failure_callbacks = []
        self.recovery_callbacks = []

    async def start(self) -> None:
        self._running = True
        for node in self.cluster_nodes:
            if node != self.node_id:
                self.last_heartbeat[node] = time.time()
        self._detector_task = asyncio.create_task(self._detection_loop())
        logger.info(f"Failure detector started for node {self.node_id}")

    async def stop(self) -> None:
        self._running = False
        if self._detector_task:
            self._detector_task.cancel()
            try:
                await self._detector_task
            except asyncio.CancelledError:
                pass
        logger.info(f"Failure detector stopped for node {self.node_id}")

    def register_failure_callback(
        self, callback: Callable[[str], Awaitable[None]]
    ) -> None:
        self.failure_callbacks.append(callback)

    def register_recovery_callback(
        self, callback: Callable[[str], Awaitable[None]]
    ) -> None:
        self.recovery_callbacks.append(callback)

    def record_heartbeat(self, node_id: str) -> None:
        self.last_heartbeat[node_id] = time.time()
        if node_id in self.failed_nodes:
            self.failed_nodes.remove(node_id)
            logger.info(f"Node {node_id} recovered")
            asyncio.create_task(self._notify_recovery(node_id))

    def is_node_alive(self, node_id: str) -> bool:
        return node_id not in self.failed_nodes

    def get_alive_nodes(self) -> list[str]:
        return [node for node in self.cluster_nodes if node not in self.failed_nodes]

    async def _detection_loop(self) -> None:
        while self._running:
            current_time = time.time()
            for node_id, last_time in self.last_heartbeat.items():
                if current_time - last_time > self.timeout:
                    if node_id not in self.failed_nodes:
                        self.failed_nodes.add(node_id)
                        logger.warning(f"Node {node_id} detected as failed")
                        await self._notify_failure(node_id)
            await asyncio.sleep(self.heartbeat_interval)

    async def _notify_failure(self, node_id: str) -> None:
        for callback in self.failure_callbacks:
            try:
                await callback(node_id)
            except Exception as e:
                logger.error(f"Error in failure callback: {e}")

    async def _notify_recovery(self, node_id: str) -> None:
        for callback in self.recovery_callbacks:
            try:
                await callback(node_id)
            except Exception as e:
                logger.error(f"Error in recovery callback: {e}")


__all__ = ["FailureDetector"]
