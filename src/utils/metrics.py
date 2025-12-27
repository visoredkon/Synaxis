from __future__ import annotations

from dataclasses import dataclass, field
from time import perf_counter
from typing import TYPE_CHECKING

from prometheus_client import Counter, Gauge, Histogram, start_http_server

if TYPE_CHECKING:
    from collections.abc import Callable


@dataclass(slots=True)
class Metrics:
    requests_total: Counter = field(
        default_factory=lambda: Counter(
            "synaxis_requests_total",
            "Total number of requests",
            ["node_id", "request_type", "status"],
        )
    )
    request_latency: Histogram = field(
        default_factory=lambda: Histogram(
            "synaxis_request_latency_seconds",
            "Request latency in seconds",
            ["node_id", "request_type"],
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0),
        )
    )
    active_locks: Gauge = field(
        default_factory=lambda: Gauge(
            "synaxis_active_locks",
            "Number of active locks",
            ["node_id", "lock_type"],
        )
    )
    queue_size: Gauge = field(
        default_factory=lambda: Gauge(
            "synaxis_queue_size",
            "Current queue size",
            ["node_id", "topic"],
        )
    )
    cache_hits: Counter = field(
        default_factory=lambda: Counter(
            "synaxis_cache_hits_total",
            "Total cache hits",
            ["node_id"],
        )
    )
    cache_misses: Counter = field(
        default_factory=lambda: Counter(
            "synaxis_cache_misses_total",
            "Total cache misses",
            ["node_id"],
        )
    )
    consensus_term: Gauge = field(
        default_factory=lambda: Gauge(
            "synaxis_consensus_term",
            "Current consensus term",
            ["node_id"],
        )
    )
    consensus_role: Gauge = field(
        default_factory=lambda: Gauge(
            "synaxis_consensus_role",
            "Current consensus role (0=follower, 1=candidate, 2=leader)",
            ["node_id"],
        )
    )
    cluster_nodes_alive: Gauge = field(
        default_factory=lambda: Gauge(
            "synaxis_cluster_nodes_alive",
            "Number of alive nodes in cluster",
            ["node_id"],
        )
    )


class MetricsCollector:
    _instance: MetricsCollector | None = None
    _metrics: Metrics | None = None
    _started: bool = False

    def __new__(cls) -> MetricsCollector:
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._metrics = Metrics()
        return cls._instance

    @property
    def metrics(self) -> Metrics:
        if self._metrics is None:
            self._metrics = Metrics()
        return self._metrics

    def start_server(self, port: int = 9090) -> None:
        if not self._started:
            start_http_server(port)
            self._started = True

    def record_request(
        self,
        node_id: str,
        request_type: str,
        status: str,
        latency: float,
    ) -> None:
        self.metrics.requests_total.labels(
            node_id=node_id,
            request_type=request_type,
            status=status,
        ).inc()
        self.metrics.request_latency.labels(
            node_id=node_id,
            request_type=request_type,
        ).observe(latency)

    def record_lock(self, node_id: str, lock_type: str, delta: int) -> None:
        self.metrics.active_locks.labels(node_id=node_id, lock_type=lock_type).inc(
            delta
        )

    def record_queue_size(self, node_id: str, topic: str, size: int) -> None:
        self.metrics.queue_size.labels(node_id=node_id, topic=topic).set(size)

    def record_cache_hit(self, node_id: str) -> None:
        self.metrics.cache_hits.labels(node_id=node_id).inc()

    def record_cache_miss(self, node_id: str) -> None:
        self.metrics.cache_misses.labels(node_id=node_id).inc()

    def update_consensus(self, node_id: str, term: int, role: int) -> None:
        self.metrics.consensus_term.labels(node_id=node_id).set(term)
        self.metrics.consensus_role.labels(node_id=node_id).set(role)

    def update_cluster_health(self, node_id: str, alive_count: int) -> None:
        self.metrics.cluster_nodes_alive.labels(node_id=node_id).set(alive_count)


class Timer:
    def __init__(
        self,
        callback: Callable[[float], None] | None = None,
    ) -> None:
        self._start: float = 0.0
        self._callback = callback

    def __enter__(self) -> Timer:
        self._start = perf_counter()
        return self

    def __exit__(self, *_: object) -> None:
        elapsed = perf_counter() - self._start
        if self._callback:
            self._callback(elapsed)

    @property
    def elapsed(self) -> float:
        return perf_counter() - self._start
