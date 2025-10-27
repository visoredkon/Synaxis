import asyncio
import sys

from loguru import logger

from .nodes import LockManager
from .utils import get_config


async def main() -> None:
    config = get_config()
    logger.remove()
    logger.add(sys.stderr, level=config.log_level)
    node_addresses = {
        node.node_id: (node.host, node.port) for node in config.cluster.nodes
    }
    from .consensuses.raft import RaftConfig

    raft_config = RaftConfig(
        election_timeout_min=config.raft.election_timeout_min,
        election_timeout_max=config.raft.election_timeout_max,
        heartbeat_interval=config.raft.heartbeat_interval,
    )
    lock_manager = LockManager(
        node_id=config.node.node_id,
        host=config.node.host,
        port=config.node.port,
        cluster_nodes=[node.node_id for node in config.cluster.nodes],
        node_addresses=node_addresses,
        raft_config=raft_config,
    )
    try:
        await lock_manager.start()
        logger.info(f"Lock manager {config.node.node_id} running")
        while lock_manager.is_running():
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        logger.info("Shutting down lock manager")
    finally:
        await lock_manager.stop()


if __name__ == "__main__":
    asyncio.run(main())
