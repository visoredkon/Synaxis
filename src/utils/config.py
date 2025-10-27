from dataclasses import dataclass
from pathlib import Path

from environs import Env


@dataclass
class NodeConfig:
    node_id: str
    host: str
    port: int


@dataclass
class ClusterConfig:
    nodes: list[NodeConfig]

    @classmethod
    def from_string(cls, nodes_str: str) -> "ClusterConfig":
        nodes = []
        for node_str in nodes_str.split(","):
            node_str = node_str.strip()
            if ":" in node_str:
                host, port_str = node_str.split(":", 1)
                nodes.append(NodeConfig(node_id=host, host=host, port=int(port_str)))
        return cls(nodes=nodes)


@dataclass
class RedisConfig:
    host: str
    port: int
    db: int


@dataclass
class RaftConfig:
    election_timeout_min: int
    election_timeout_max: int
    heartbeat_interval: int


@dataclass
class CacheConfig:
    size: int


@dataclass
class QueueConfig:
    partitions: int


@dataclass
class LockConfig:
    timeout: int


@dataclass
class SynaxisConfig:
    node: NodeConfig
    cluster: ClusterConfig
    redis: RedisConfig
    raft: RaftConfig
    cache: CacheConfig
    queue: QueueConfig
    lock: LockConfig
    log_level: str

    @classmethod
    def from_env(cls, env_file: Path | None = None) -> "SynaxisConfig":
        env = Env()
        if env_file:
            env.read_env(str(env_file))
        node_id = env.str("NODE_ID", "node1")
        node_host = env.str("NODE_HOST", "0.0.0.0")
        node_port = env.int("NODE_PORT", 8000)
        cluster_nodes_str = env.str("CLUSTER_NODES", "node1:8000,node2:8001,node3:8002")
        cluster = ClusterConfig.from_string(cluster_nodes_str)
        redis_host = env.str("REDIS_HOST", "localhost")
        redis_port = env.int("REDIS_PORT", 6379)
        redis_db = env.int("REDIS_DB", 0)
        raft_election_timeout_min = env.int("RAFT_ELECTION_TIMEOUT_MIN", 150)
        raft_election_timeout_max = env.int("RAFT_ELECTION_TIMEOUT_MAX", 300)
        raft_heartbeat_interval = env.int("RAFT_HEARTBEAT_INTERVAL", 50)
        cache_size = env.int("CACHE_SIZE", 1000)
        queue_partitions = env.int("QUEUE_PARTITIONS", 16)
        lock_timeout = env.int("LOCK_TIMEOUT", 30)
        log_level = env.str("LOG_LEVEL", "INFO")
        return cls(
            node=NodeConfig(node_id=node_id, host=node_host, port=node_port),
            cluster=cluster,
            redis=RedisConfig(host=redis_host, port=redis_port, db=redis_db),
            raft=RaftConfig(
                election_timeout_min=raft_election_timeout_min,
                election_timeout_max=raft_election_timeout_max,
                heartbeat_interval=raft_heartbeat_interval,
            ),
            cache=CacheConfig(size=cache_size),
            queue=QueueConfig(partitions=queue_partitions),
            lock=LockConfig(timeout=lock_timeout),
            log_level=log_level,
        )


def get_config() -> SynaxisConfig:
    env_file = Path(".env")
    if env_file.exists():
        return SynaxisConfig.from_env(env_file)
    return SynaxisConfig.from_env()


__all__ = [
    "SynaxisConfig",
    "NodeConfig",
    "ClusterConfig",
    "RedisConfig",
    "RaftConfig",
    "CacheConfig",
    "QueueConfig",
    "LockConfig",
    "get_config",
]
