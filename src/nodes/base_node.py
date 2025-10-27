from loguru import logger

from ..communications import FailureDetector, MessagePassing
from ..utils import MetricsCollector, get_metrics_collector


class BaseNode:
    node_id: str
    host: str
    port: int
    cluster_nodes: list[str]
    message_passing: MessagePassing
    failure_detector: FailureDetector | None
    metrics: MetricsCollector
    _running: bool

    def __init__(
        self, node_id: str, host: str, port: int, cluster_nodes: list[str]
    ) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.cluster_nodes = cluster_nodes
        self.message_passing = MessagePassing(node_id, host, port)
        self.failure_detector = None
        self.metrics = get_metrics_collector()
        self._running = False

    async def start(self) -> None:
        self._running = True
        await self.message_passing.start()
        logger.info(f"Base node {self.node_id} started on {self.host}:{self.port}")

    async def stop(self) -> None:
        self._running = False
        if self.failure_detector:
            await self.failure_detector.stop()
        await self.message_passing.stop()
        logger.info(f"Base node {self.node_id} stopped")

    def is_running(self) -> bool:
        return self._running

    async def _handle_failure(self, failed_node_id: str) -> None:
        logger.warning(f"Node {self.node_id} detected failure of {failed_node_id}")
        self.metrics.increment_counter("node_failures")

    async def _handle_recovery(self, recovered_node_id: str) -> None:
        logger.info(f"Node {self.node_id} detected recovery of {recovered_node_id}")
        self.metrics.increment_counter("node_recoveries")


__all__ = ["BaseNode"]
