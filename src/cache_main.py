import asyncio
import sys

from loguru import logger

from .nodes import CacheNode
from .utils import get_config


async def main() -> None:
    config = get_config()
    logger.remove()
    logger.add(sys.stderr, level=config.log_level)
    node_addresses = {
        node.node_id: (node.host, node.port) for node in config.cluster.nodes
    }
    cache_node = CacheNode(
        node_id=config.node.node_id,
        host=config.node.host,
        port=config.node.port,
        cluster_nodes=[node.node_id for node in config.cluster.nodes],
        node_addresses=node_addresses,
        cache_size=config.cache.size,
    )
    try:
        await cache_node.start()
        logger.info(f"Cache node {config.node.node_id} running")
        while cache_node.is_running():
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down cache node")
    finally:
        await cache_node.stop()


if __name__ == "__main__":
    asyncio.run(main())
