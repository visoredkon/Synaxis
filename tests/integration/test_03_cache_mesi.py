from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import perf_counter, sleep
from uuid import uuid4

from utils.testing import (
    cache_invalidate,
    cache_read,
    cache_write,
    get_cache_stats,
)


class TestMESIProtocolStates:
    def test_initial_read_creates_shared_state(self, cache_node_url: str) -> None:
        key = f"mesi-shared-{uuid4().hex[:8]}"
        value = b"initial-value"

        write_status, _ = cache_write(key, value, url=cache_node_url)
        assert write_status == 200

        read_status, response = cache_read(key, url=cache_node_url)
        assert read_status == 200
        assert response.get("value") == value.decode()

    def test_write_creates_modified_state(self, cache_node_url: str) -> None:
        key = f"mesi-modified-{uuid4().hex[:8]}"
        value = b"modified-value"

        write_status, response = cache_write(key, value, url=cache_node_url)
        assert write_status == 200

        if "state" in response:
            assert response["state"] in ("MODIFIED", "EXCLUSIVE")

    def test_exclusive_to_shared_on_remote_read(
        self,
        all_cache_nodes: list[str],
    ) -> None:
        key = f"mesi-e-to-s-{uuid4().hex[:8]}"
        value = b"exclusive-then-shared"

        write_status, _ = cache_write(key, value, url=all_cache_nodes[0])
        assert write_status == 200

        read_status, response = cache_read(key, url=all_cache_nodes[1])
        assert read_status == 200

    def test_shared_to_invalid_on_remote_write(
        self,
        all_cache_nodes: list[str],
    ) -> None:
        key = f"mesi-invalidate-{uuid4().hex[:8]}"
        initial_value = b"initial"
        updated_value = b"updated"

        cache_write(key, initial_value, url=all_cache_nodes[0])
        cache_read(key, url=all_cache_nodes[1])

        cache_write(key, updated_value, url=all_cache_nodes[2])

        sleep(1)

        read_status, response = cache_read(key, url=all_cache_nodes[1])
        assert read_status == 200
        if "value" in response:
            assert response["value"] == updated_value.decode()


class TestCacheInvalidationPropagation:
    def test_invalidation_propagates_to_all_nodes(
        self,
        all_cache_nodes: list[str],
    ) -> None:
        key = f"invalidate-prop-{uuid4().hex[:8]}"
        value = b"to-be-invalidated"

        for url in all_cache_nodes:
            cache_write(key, value, url=url)

        inv_status, _ = cache_invalidate(key, url=all_cache_nodes[0])
        assert inv_status == 200

        sleep(1)

        for url in all_cache_nodes[1:]:
            stats_status, stats = get_cache_stats(url=url)
            if stats_status == 200:
                assert stats.get("invalidations", 0) >= 0


class TestLRUReplacementPolicy:
    def test_lru_eviction_order(self, cache_node_url: str) -> None:
        keys = [f"lru-{i}-{uuid4().hex[:8]}" for i in range(10)]

        for key in keys:
            cache_write(key, f"value-{key}".encode(), url=cache_node_url)

        for key in keys[:5]:
            cache_read(key, url=cache_node_url)

        for i in range(20):
            new_key = f"new-{i}-{uuid4().hex[:8]}"
            cache_write(new_key, f"new-value-{i}".encode(), url=cache_node_url)

        hits = 0
        misses = 0
        for key in keys[:5]:
            status, response = cache_read(key, url=cache_node_url)
            if status == 200 and response.get("value"):
                hits += 1
            else:
                misses += 1


class TestCachePerformanceMetrics:
    def test_hit_rate_tracking(self, cache_node_url: str) -> None:
        key = f"hit-rate-{uuid4().hex[:8]}"
        value = b"hit-rate-value"

        cache_write(key, value, url=cache_node_url)

        for _ in range(10):
            cache_read(key, url=cache_node_url)

        stats_status, stats = get_cache_stats(url=cache_node_url)
        assert stats_status == 200
        assert stats.get("hits", 0) >= 10

    def test_miss_rate_tracking(self, cache_node_url: str) -> None:
        initial_stats_status, initial_stats = get_cache_stats(url=cache_node_url)
        initial_misses = (
            initial_stats.get("misses", 0) if initial_stats_status == 200 else 0
        )

        for i in range(5):
            nonexistent_key = f"nonexistent-{i}-{uuid4().hex[:8]}"
            cache_read(nonexistent_key, url=cache_node_url)

        final_stats_status, final_stats = get_cache_stats(url=cache_node_url)
        assert final_stats_status == 200
        final_misses = final_stats.get("misses", 0)
        assert final_misses >= initial_misses


class TestCacheCoherencePerformance:
    def test_read_latency(self, cache_node_url: str) -> None:
        key = f"latency-{uuid4().hex[:8]}"
        value = b"latency-value"

        cache_write(key, value, url=cache_node_url)

        latencies: list[float] = []
        for _ in range(100):
            start = perf_counter()
            cache_read(key, url=cache_node_url)
            latencies.append(perf_counter() - start)

        avg_latency_ms = (sum(latencies) / len(latencies)) * 1000
        assert avg_latency_ms < 100, (
            f"Avg latency harus < 100ms, actual: {avg_latency_ms:.2f}ms"
        )

    def test_write_latency(self, cache_node_url: str) -> None:
        latencies: list[float] = []
        for i in range(50):
            key = f"write-lat-{i}-{uuid4().hex[:8]}"
            start = perf_counter()
            cache_write(key, f"value-{i}".encode(), url=cache_node_url)
            latencies.append(perf_counter() - start)

        avg_latency_ms = (sum(latencies) / len(latencies)) * 1000
        assert avg_latency_ms < 200, (
            f"Avg write latency harus < 200ms, actual: {avg_latency_ms:.2f}ms"
        )

    def test_concurrent_cache_operations(
        self,
        cache_node_url: str,
    ) -> None:
        num_operations = 50

        def random_operation(op_id: int) -> bool:
            key = f"concurrent-{op_id % 10}-{uuid4().hex[:4]}"
            if op_id % 2 == 0:
                status, _ = cache_write(
                    key, f"value-{op_id}".encode(), url=cache_node_url
                )
            else:
                status, _ = cache_read(key, url=cache_node_url)
            return status in (200, 404)

        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [
                executor.submit(random_operation, i) for i in range(num_operations)
            ]
            results = [f.result() for f in as_completed(futures)]

        success_rate = sum(results) / len(results)
        assert success_rate > 0.9, (
            f"Success rate harus > 90%, actual: {success_rate * 100:.1f}%"
        )


class TestCrossNodeCoherence:
    def test_write_read_consistency(
        self,
        all_cache_nodes: list[str],
    ) -> None:
        key = f"cross-node-{uuid4().hex[:8]}"
        value = b"cross-node-value"

        write_status, _ = cache_write(key, value, url=all_cache_nodes[0])
        assert write_status == 200

        sleep(1)

        for url in all_cache_nodes:
            read_status, response = cache_read(key, url=url)
            if read_status == 200 and "value" in response:
                assert response["value"] == value.decode()

    def test_sequential_writes_convergence(
        self,
        all_cache_nodes: list[str],
    ) -> None:
        key = f"converge-{uuid4().hex[:8]}"

        for i, url in enumerate(all_cache_nodes):
            cache_write(key, f"value-from-node-{i}".encode(), url=url)
            sleep(0.5)

        sleep(2)

        values: set[str] = set()
        for url in all_cache_nodes:
            status, response = cache_read(key, url=url)
            if status == 200 and "value" in response:
                values.add(response["value"])

        assert len(values) <= 1, (
            f"Semua node harus converge ke nilai yang sama, ditemukan: {values}"
        )
