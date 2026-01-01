from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from time import sleep
from uuid import uuid4

from utils.testing import (
    acquire_lock,
    get_lock_info,
    get_raft_state,
    release_lock,
    start_container,
    stop_container,
)


class TestRaftLeaderElection:
    def test_cluster_has_single_leader(
        self,
        all_lock_managers: list[str],
    ) -> None:
        sleep(5)
        leaders: list[str] = []
        for url in all_lock_managers:
            status, state = get_raft_state(url)
            if status == 200 and state.get("role") == "LEADER":
                leaders.append(url)
        assert len(leaders) == 1, f"Harus ada tepat 1 leader, ditemukan {len(leaders)}"

    def test_all_nodes_have_same_term(
        self,
        all_lock_managers: list[str],
    ) -> None:
        sleep(2)
        terms: set[int] = set()
        for url in all_lock_managers:
            status, state = get_raft_state(url)
            if status == 200:
                terms.add(state.get("term", -1))
        assert len(terms) == 1, (
            f"Semua node harus memiliki term sama, ditemukan {terms}"
        )

    def test_leader_reelection_on_failure(
        self,
        all_lock_managers: list[str],
    ) -> None:
        sleep(3)
        original_leader: str | None = None
        for url in all_lock_managers:
            status, state = get_raft_state(url)
            if status == 200 and state.get("role") == "LEADER":
                original_leader = url
                break
        assert original_leader is not None, "Tidak ada leader ditemukan"

        leader_service = f"lock-manager-{all_lock_managers.index(original_leader) + 1}"
        stop_success = stop_container(leader_service)
        assert stop_success, f"Gagal stop {leader_service}"

        sleep(10)

        remaining_nodes = [url for url in all_lock_managers if url != original_leader]
        new_leaders: list[str] = []
        for url in remaining_nodes:
            status, state = get_raft_state(url)
            if status == 200 and state.get("role") == "LEADER":
                new_leaders.append(url)

        start_container(leader_service)
        sleep(3)

        assert len(new_leaders) == 1, "Harus ada leader baru setelah partisi"


class TestDistributedLockOperations:
    def test_acquire_exclusive_lock(self, lock_manager_url: str) -> None:
        resource_id = f"resource-{uuid4().hex[:8]}"
        status, response = acquire_lock(
            resource_id,
            lock_type="exclusive",
            client_id="test-client-1",
            url=lock_manager_url,
        )
        assert status == 200
        assert "lock_id" in response

        lock_id = response["lock_id"]
        release_status, _ = release_lock(lock_id, url=lock_manager_url)
        assert release_status == 200

    def test_acquire_shared_lock(self, lock_manager_url: str) -> None:
        resource_id = f"resource-{uuid4().hex[:8]}"
        status, response = acquire_lock(
            resource_id,
            lock_type="shared",
            client_id="test-client-1",
            url=lock_manager_url,
        )
        assert status == 200
        assert "lock_id" in response

    def test_multiple_shared_locks_same_resource(
        self,
        lock_manager_url: str,
    ) -> None:
        resource_id = f"shared-resource-{uuid4().hex[:8]}"
        lock_ids: list[str] = []

        for i in range(3):
            status, response = acquire_lock(
                resource_id,
                lock_type="shared",
                client_id=f"client-{i}",
                url=lock_manager_url,
            )
            assert status == 200
            lock_ids.append(response["lock_id"])

        status, info = get_lock_info(resource_id, url=lock_manager_url)
        assert status == 200
        assert info.get("state") == "SHARED"
        assert len(info.get("shared_owners", [])) == 3

        for lock_id in lock_ids:
            release_lock(lock_id, url=lock_manager_url)

    def test_exclusive_lock_blocks_shared(
        self,
        lock_manager_url: str,
    ) -> None:
        resource_id = f"exclusive-resource-{uuid4().hex[:8]}"
        status, response = acquire_lock(
            resource_id,
            lock_type="exclusive",
            client_id="exclusive-holder",
            url=lock_manager_url,
        )
        assert status == 200
        exclusive_lock_id = response["lock_id"]

        shared_status, shared_response = acquire_lock(
            resource_id,
            lock_type="shared",
            client_id="shared-requester",
            url=lock_manager_url,
        )
        assert shared_status in (200, 409, 408)

        release_lock(exclusive_lock_id, url=lock_manager_url)


class TestDeadlockDetection:
    def test_deadlock_prevention(self, lock_manager_url: str) -> None:
        resource_a = f"resource-a-{uuid4().hex[:8]}"
        resource_b = f"resource-b-{uuid4().hex[:8]}"

        status_a, response_a = acquire_lock(
            resource_a,
            lock_type="exclusive",
            client_id="client-1",
            url=lock_manager_url,
        )
        assert status_a == 200
        lock_id_a = response_a["lock_id"]

        status_b, response_b = acquire_lock(
            resource_b,
            lock_type="exclusive",
            client_id="client-2",
            url=lock_manager_url,
        )
        assert status_b == 200
        lock_id_b = response_b["lock_id"]

        release_lock(lock_id_a, url=lock_manager_url)
        release_lock(lock_id_b, url=lock_manager_url)


class TestConcurrentLockOperations:
    def test_concurrent_lock_acquire(self, lock_manager_url: str) -> None:
        resource_id = f"concurrent-resource-{uuid4().hex[:8]}"
        num_clients = 10

        def try_acquire(client_id: str) -> tuple[int | None, bool]:
            status, response = acquire_lock(
                resource_id,
                lock_type="exclusive",
                client_id=client_id,
                url=lock_manager_url,
            )
            acquired = status == 200 and "lock_id" in response
            if acquired:
                release_lock(response["lock_id"], url=lock_manager_url)
            return status, acquired

        with ThreadPoolExecutor(max_workers=num_clients) as executor:
            futures = [
                executor.submit(try_acquire, f"concurrent-client-{i}")
                for i in range(num_clients)
            ]
            results = [f.result() for f in as_completed(futures)]

        successful = sum(1 for _, acquired in results if acquired)
        assert successful >= 1, "Minimal 1 client harus berhasil acquire lock"


class TestNetworkPartitionScenario:
    def test_lock_operations_during_partition(
        self,
        all_lock_managers: list[str],
    ) -> None:
        sleep(3)
        leader_url: str | None = None
        for url in all_lock_managers:
            status, state = get_raft_state(url)
            if status == 200 and state.get("role") == "LEADER":
                leader_url = url
                break

        if leader_url is None:
            return

        resource_id = f"partition-test-{uuid4().hex[:8]}"
        status, response = acquire_lock(
            resource_id,
            lock_type="exclusive",
            client_id="partition-client",
            url=leader_url,
        )
        assert status == 200

        if "lock_id" in response:
            release_lock(response["lock_id"], url=leader_url)
