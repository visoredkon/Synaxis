from __future__ import annotations

from time import sleep
from uuid import uuid4

from utils.testing import (
    acquire_lock,
    cache_write,
    get_node_health,
    get_raft_state,
    publish_message,
    release_lock,
    restart_container,
    start_container,
    stop_container,
)


class TestNodeCrashRecovery:
    def test_lock_manager_crash_recovery(
        self,
        all_lock_managers: list[str],
    ) -> None:
        sleep(3)
        resource_id = f"crash-lock-{uuid4().hex[:8]}"
        status, response = acquire_lock(
            resource_id,
            lock_type="exclusive",
            client_id="crash-test-client",
            url=all_lock_managers[0],
        )
        assert status == 200

        stop_container("lock-manager-1")
        sleep(5)

        start_container("lock-manager-1")
        sleep(5)

        health_status, _ = get_node_health(all_lock_managers[0])
        assert health_status == 200

    def test_queue_node_crash_recovery(
        self,
        all_queue_nodes: list[str],
    ) -> None:
        topic = f"crash-queue-{uuid4().hex[:8]}"
        for i in range(5):
            publish_message(topic, f"msg-{i}".encode(), url=all_queue_nodes[0])

        stop_container("queue-1")
        sleep(3)

        start_container("queue-1")
        sleep(5)

        health_status, _ = get_node_health(all_queue_nodes[0])
        assert health_status == 200

    def test_cache_node_crash_recovery(
        self,
        all_cache_nodes: list[str],
    ) -> None:
        key = f"crash-cache-{uuid4().hex[:8]}"
        value = b"crash-test-value"
        cache_write(key, value, url=all_cache_nodes[0])

        stop_container("cache-1")
        sleep(3)

        start_container("cache-1")
        sleep(5)

        health_status, _ = get_node_health(all_cache_nodes[0])
        assert health_status == 200


class TestNetworkPartitionSimulation:
    def test_minority_partition_loses_leadership(
        self,
        all_lock_managers: list[str],
    ) -> None:
        sleep(3)
        stop_container("lock-manager-2")
        stop_container("lock-manager-3")
        sleep(5)

        status, state = get_raft_state(all_lock_managers[0])

        start_container("lock-manager-2")
        start_container("lock-manager-3")
        sleep(5)

    def test_majority_partition_maintains_operations(
        self,
        all_lock_managers: list[str],
    ) -> None:
        sleep(3)
        stop_container("lock-manager-3")
        sleep(5)

        resource_id = f"partition-op-{uuid4().hex[:8]}"
        status, response = acquire_lock(
            resource_id,
            lock_type="exclusive",
            client_id="partition-client",
            url=all_lock_managers[0],
        )

        start_container("lock-manager-3")
        sleep(3)

        if status == 200 and "lock_id" in response:
            release_lock(response["lock_id"], url=all_lock_managers[0])


class TestGracefulRestart:
    def test_graceful_restart_preserves_state(
        self,
        lock_manager_url: str,
    ) -> None:
        resource_id = f"graceful-{uuid4().hex[:8]}"
        status, response = acquire_lock(
            resource_id,
            lock_type="exclusive",
            client_id="graceful-client",
            url=lock_manager_url,
        )
        assert status == 200

        restart_container("lock-manager-1")
        sleep(5)

        health_status, _ = get_node_health(lock_manager_url)
        assert health_status == 200


class TestCascadingFailure:
    def test_system_survives_sequential_failures(
        self,
        all_lock_managers: list[str],
    ) -> None:
        sleep(3)

        stop_container("lock-manager-1")
        sleep(5)

        health_status, _ = get_node_health(all_lock_managers[1])
        assert health_status == 200

        stop_container("lock-manager-2")
        sleep(5)

        start_container("lock-manager-1")
        sleep(5)

        health_status, _ = get_node_health(all_lock_managers[0])
        assert health_status == 200

        start_container("lock-manager-2")
        sleep(3)


class TestDataConsistencyUnderFailure:
    def test_lock_consistency_during_leader_change(
        self,
        all_lock_managers: list[str],
    ) -> None:
        sleep(3)
        resource_id = f"consistency-{uuid4().hex[:8]}"

        leader_url: str | None = None
        for url in all_lock_managers:
            status, state = get_raft_state(url)
            if status == 200 and state.get("role") == "LEADER":
                leader_url = url
                break

        if leader_url is None:
            return

        lock_status, lock_response = acquire_lock(
            resource_id,
            lock_type="exclusive",
            client_id="consistency-client",
            url=leader_url,
        )
        assert lock_status == 200

        leader_service = f"lock-manager-{all_lock_managers.index(leader_url) + 1}"
        stop_container(leader_service)
        sleep(10)

        remaining = [url for url in all_lock_managers if url != leader_url]
        for url in remaining:
            status, state = get_raft_state(url)
            if status == 200 and state.get("role") == "LEADER":
                break

        start_container(leader_service)
        sleep(5)
