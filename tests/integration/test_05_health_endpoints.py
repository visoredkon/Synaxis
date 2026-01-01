from __future__ import annotations

from utils.testing import (
    get_cache_stats,
    get_node_health,
    get_raft_state,
)


class TestLockManagerHealthEndpoints:
    def test_health_endpoint_returns_200(
        self,
        all_lock_managers: list[str],
    ) -> None:
        for url in all_lock_managers:
            status, response = get_node_health(url)
            assert status == 200
            assert "status" in response or "healthy" in response or response == {}

    def test_raft_state_endpoint(
        self,
        all_lock_managers: list[str],
    ) -> None:
        for url in all_lock_managers:
            status, response = get_raft_state(url)
            assert status == 200
            assert "role" in response
            assert "term" in response
            assert response["role"] in ("LEADER", "FOLLOWER", "CANDIDATE")


class TestQueueNodeHealthEndpoints:
    def test_health_endpoint_returns_200(
        self,
        all_queue_nodes: list[str],
    ) -> None:
        for url in all_queue_nodes:
            status, response = get_node_health(url)
            assert status == 200


class TestCacheNodeHealthEndpoints:
    def test_health_endpoint_returns_200(
        self,
        all_cache_nodes: list[str],
    ) -> None:
        for url in all_cache_nodes:
            status, response = get_node_health(url)
            assert status == 200

    def test_stats_endpoint(
        self,
        all_cache_nodes: list[str],
    ) -> None:
        for url in all_cache_nodes:
            status, response = get_cache_stats(url)
            assert status == 200
            assert "hits" in response
            assert "misses" in response


class TestClusterHealthOverview:
    def test_all_nodes_healthy(
        self,
        all_lock_managers: list[str],
        all_queue_nodes: list[str],
        all_cache_nodes: list[str],
    ) -> None:
        all_nodes = all_lock_managers + all_queue_nodes + all_cache_nodes
        healthy_count = 0

        for url in all_nodes:
            status, _ = get_node_health(url)
            if status == 200:
                healthy_count += 1

        total = len(all_nodes)
        health_rate = healthy_count / total
        assert health_rate >= 0.9, (
            f"Minimal 90% nodes harus healthy, actual: {health_rate * 100:.1f}%"
        )

    def test_lock_cluster_has_leader(
        self,
        all_lock_managers: list[str],
    ) -> None:
        leaders = 0
        for url in all_lock_managers:
            status, state = get_raft_state(url)
            if status == 200 and state.get("role") == "LEADER":
                leaders += 1

        assert leaders == 1, (
            f"Lock cluster harus punya tepat 1 leader, ditemukan {leaders}"
        )
