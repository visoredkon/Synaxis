from __future__ import annotations

from asyncio import Event, get_running_loop, run
from os import getenv
from signal import SIGINT, SIGTERM
from sys import stderr
from typing import TYPE_CHECKING

from loguru import logger

from src.nodes.base_node import BaseNode
from src.nodes.cache_node import CacheNode
from src.nodes.lock_manager import LockManager
from src.nodes.queue_node import QueueNode
from src.utils.config import Config
from src.utils.metrics import MetricsCollector

if TYPE_CHECKING:
    pass


def get_node(config: Config, node_type: str) -> BaseNode:
    node_map = {
        "lock_manager": LockManager,
        "queue_node": QueueNode,
        "cache_node": CacheNode,
    }
    node_class = node_map.get(node_type)
    if not node_class:
        raise ValueError(f"Unknown node type: {node_type}")
    return node_class(config)


async def run_node(node: BaseNode, metrics_port: int) -> None:
    loop = get_running_loop()
    stop_event = Event()

    def signal_handler() -> None:
        stop_event.set()

    for sig in (SIGTERM, SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    metrics = MetricsCollector()
    metrics.start_server(metrics_port)

    await node.start()

    await stop_event.wait()

    await node.stop()


def main() -> None:
    config = Config.from_env()

    logger.remove()
    logger.add(
        stderr,
        level=config.log_level,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
    )

    node_type = getenv("NODE_TYPE", "lock_manager")
    node = get_node(config, node_type)

    logger.info(f"Starting {node_type} node: {config.node.node_id}")

    run(run_node(node, config.metrics_port))


if __name__ == "__main__":
    main()
