from __future__ import annotations

from dataclasses import dataclass
from os import getenv
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(frozen=True, slots=True)
class NodeConfig:
    node_id: str
    host: str
    port: int
    redis_url: str = "redis://localhost:6379"
    election_timeout_min: int = 150
    election_timeout_max: int = 300
    heartbeat_interval: int = 50
    request_timeout: float = 5.0
    max_retries: int = 3
    consensus_type: str = "raft"


@dataclass(frozen=True, slots=True)
class ClusterConfig:
    nodes: tuple[NodeConfig, ...]
    replication_factor: int = 3
    min_quorum: int = 2


@dataclass(slots=True)
class Config:
    node: NodeConfig
    cluster: ClusterConfig
    metrics_port: int = 9090
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> Config:
        node_id = getenv("NODE_ID", "node-1")
        host = getenv("NODE_HOST", "0.0.0.0")
        port = int(getenv("NODE_PORT", "8000"))
        redis_url = getenv("REDIS_URL", "redis://localhost:6379")
        consensus_type = getenv("CONSENSUS_TYPE", "raft")

        node = NodeConfig(
            node_id=node_id,
            host=host,
            port=port,
            redis_url=redis_url,
            election_timeout_min=int(getenv("ELECTION_TIMEOUT_MIN", "150")),
            election_timeout_max=int(getenv("ELECTION_TIMEOUT_MAX", "300")),
            heartbeat_interval=int(getenv("HEARTBEAT_INTERVAL", "50")),
            request_timeout=float(getenv("REQUEST_TIMEOUT", "5.0")),
            max_retries=int(getenv("MAX_RETRIES", "3")),
            consensus_type=consensus_type,
        )

        cluster_nodes_str = getenv("CLUSTER_NODES", "")
        cluster_nodes: list[NodeConfig] = []
        if cluster_nodes_str:
            for node_spec in cluster_nodes_str.split(","):
                parts = node_spec.strip().split(":")
                if len(parts) == 2:
                    n_id, n_port = parts[0], int(parts[1])
                    cluster_nodes.append(
                        NodeConfig(
                            node_id=n_id,
                            host=n_id,
                            port=n_port,
                            redis_url=redis_url,
                            consensus_type=consensus_type,
                        )
                    )

        cluster = ClusterConfig(
            nodes=tuple(cluster_nodes),
            replication_factor=int(getenv("REPLICATION_FACTOR", "3")),
            min_quorum=int(getenv("MIN_QUORUM", "2")),
        )

        return cls(
            node=node,
            cluster=cluster,
            metrics_port=int(getenv("METRICS_PORT", "9090")),
            log_level=getenv("LOG_LEVEL", "INFO"),
        )

    def get_peer_nodes(self) -> Sequence[NodeConfig]:
        return [n for n in self.cluster.nodes if n.node_id != self.node.node_id]
