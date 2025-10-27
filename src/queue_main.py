import asyncio
import sys

from loguru import logger

from .nodes import QueueNode
from .utils import get_config


async def main() -> None:
    config = get_config()
    logger.remove()
    logger.add(sys.stderr, level=config.log_level)
    redis_url = f"redis://{config.redis.host}:{config.redis.port}/{config.redis.db}"
    queue_node = QueueNode(
        node_id=config.node.node_id,
        host=config.node.host,
        port=config.node.port,
        cluster_nodes=[node.node_id for node in config.cluster.nodes],
        redis_url=redis_url,
        partitions=config.queue.partitions,
    )
    try:
        await queue_node.start()
        logger.info(f"Queue node {config.node.node_id} running")
        while queue_node.is_running():
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down queue node")
    finally:
        await queue_node.stop()


if __name__ == "__main__":
    asyncio.run(main())
