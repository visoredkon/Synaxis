from __future__ import annotations

from pytest import fixture
from src.utils.consistent_hash import ConsistentHash


class TestConsistentHash:
    @fixture
    def hash_ring(self) -> ConsistentHash:
        ch = ConsistentHash(virtual_nodes=100)
        return ch

    def test_empty_ring_returns_none(self, hash_ring: ConsistentHash) -> None:
        assert hash_ring.get_node("any-key") is None

    def test_single_node(self, hash_ring: ConsistentHash) -> None:
        hash_ring.add_node("node-1")
        assert hash_ring.get_node("any-key") == "node-1"

    def test_node_count(self, hash_ring: ConsistentHash) -> None:
        hash_ring.add_node("node-1")
        hash_ring.add_node("node-2")
        hash_ring.add_node("node-3")
        assert hash_ring.node_count == 3

    def test_consistent_distribution(self, hash_ring: ConsistentHash) -> None:
        hash_ring.add_node("node-1")
        hash_ring.add_node("node-2")
        result1 = hash_ring.get_node("my-key")
        result2 = hash_ring.get_node("my-key")
        assert result1 == result2

    def test_remove_node(self, hash_ring: ConsistentHash) -> None:
        hash_ring.add_node("node-1")
        hash_ring.add_node("node-2")
        hash_ring.remove_node("node-1")
        assert hash_ring.node_count == 1
        assert hash_ring.get_node("any-key") == "node-2"

    def test_get_multiple_nodes(self, hash_ring: ConsistentHash) -> None:
        hash_ring.add_node("node-1")
        hash_ring.add_node("node-2")
        hash_ring.add_node("node-3")
        nodes = hash_ring.get_nodes_for_key("test-key", count=2)
        assert len(nodes) == 2
        assert len(set(nodes)) == 2

    def test_add_duplicate_node_is_idempotent(
        self,
        hash_ring: ConsistentHash,
    ) -> None:
        hash_ring.add_node("node-1")
        hash_ring.add_node("node-1")
        assert hash_ring.node_count == 1


class TestKeyDistribution:
    def test_keys_distributed_across_nodes(self) -> None:
        ch = ConsistentHash(virtual_nodes=100)
        ch.add_node("node-1")
        ch.add_node("node-2")
        ch.add_node("node-3")

        distribution: dict[str, int] = {"node-1": 0, "node-2": 0, "node-3": 0}
        for i in range(1000):
            node = ch.get_node(f"key-{i}")
            if node:
                distribution[node] += 1

        for count in distribution.values():
            assert count > 0

    def test_get_all_nodes(self) -> None:
        ch = ConsistentHash(virtual_nodes=50)
        ch.add_node("alpha")
        ch.add_node("beta")
        ch.add_node("gamma")
        all_nodes = set(ch.get_all_nodes())
        assert all_nodes == {"alpha", "beta", "gamma"}
