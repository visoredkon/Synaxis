"""Unit tests for utilities."""

from src.utils.config import ClusterConfig, NodeConfig
from src.utils.metrics import MetricsCollector


def test_node_config_creation() -> None:
    """Test node configuration creation."""
    config = NodeConfig(node_id="node1", host="localhost", port=8000)

    assert config.node_id == "node1"
    assert config.host == "localhost"
    assert config.port == 8000


def test_cluster_config_from_string() -> None:
    """Test cluster configuration parsing."""
    cluster = ClusterConfig.from_string("node1:8001,node2:8002,node3:8003")

    assert len(cluster.nodes) == 3
    assert cluster.nodes[0].node_id == "node1"
    assert cluster.nodes[0].port == 8001
    assert cluster.nodes[1].node_id == "node2"
    assert cluster.nodes[1].port == 8002


def test_metrics_collector_counter() -> None:
    """Test metrics collector counter operations."""
    metrics = MetricsCollector()

    metrics.increment_counter("test_counter")
    assert metrics.get_counter("test_counter") == 1.0

    metrics.increment_counter("test_counter", 5.0)
    assert metrics.get_counter("test_counter") == 6.0


def test_metrics_collector_gauge() -> None:
    """Test metrics collector gauge operations."""
    metrics = MetricsCollector()

    metrics.set_gauge("test_gauge", 42.0)
    assert metrics.get_gauge("test_gauge") == 42.0

    metrics.set_gauge("test_gauge", 100.0)
    assert metrics.get_gauge("test_gauge") == 100.0


def test_metrics_collector_histogram() -> None:
    """Test metrics collector histogram operations."""
    metrics = MetricsCollector()

    metrics.record_histogram("test_histogram", 10.0)
    metrics.record_histogram("test_histogram", 20.0)
    metrics.record_histogram("test_histogram", 30.0)

    values = metrics.get_histogram("test_histogram")
    assert len(values) == 3
    assert 10.0 in values
    assert 20.0 in values
    assert 30.0 in values

    stats = metrics.get_histogram_stats("test_histogram")
    assert stats is not None
    assert stats["min"] == 10.0
    assert stats["max"] == 30.0
    assert stats["avg"] == 20.0
    assert stats["count"] == 3


def test_metrics_collector_timer() -> None:
    """Test metrics collector timer operations."""
    metrics = MetricsCollector()

    metrics.start_timer("test_timer")
    duration = metrics.stop_timer("test_timer")

    assert duration is not None
    assert duration >= 0

    histogram = metrics.get_histogram("test_timer_duration")
    assert len(histogram) == 1


def test_metrics_collector_reset() -> None:
    """Test metrics collector reset."""
    metrics = MetricsCollector()

    metrics.increment_counter("counter1")
    metrics.set_gauge("gauge1", 50.0)
    metrics.record_histogram("histogram1", 10.0)

    metrics.reset()

    assert metrics.get_counter("counter1") == 0.0
    assert metrics.get_gauge("gauge1") is None
    assert len(metrics.get_histogram("histogram1")) == 0
