import time

import ormsgpack
from locust import HttpUser, between, task


class LockManagerUser(HttpUser):
    wait_time = between(1, 3)
    host = "http://localhost:8001"

    def on_start(self) -> None:
        self.lock_counter = 0

    @task(3)
    def acquire_exclusive_lock(self) -> None:
        lock_id = f"lock_{self.lock_counter}"
        self.lock_counter += 1

        payload = {
            "type": "lock_request",
            "lock_id": lock_id,
            "lock_type": "exclusive",
            "requester": f"user_{id(self)}",
        }

        with self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to acquire lock: {response.status_code}")

    @task(1)
    def acquire_shared_lock(self) -> None:
        lock_id = "shared_lock"

        payload = {
            "type": "lock_request",
            "lock_id": lock_id,
            "lock_type": "shared",
            "requester": f"user_{id(self)}",
        }

        with self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(
                    f"Failed to acquire shared lock: {response.status_code}"
                )

    @task(2)
    def release_lock(self) -> None:
        lock_id = f"lock_{self.lock_counter - 1}" if self.lock_counter > 0 else "lock_0"

        payload = {
            "type": "lock_release",
            "lock_id": lock_id,
        }

        with self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to release lock: {response.status_code}")


class QueueUser(HttpUser):
    wait_time = between(0.5, 2)
    host = "http://localhost:8011"

    def on_start(self) -> None:
        self.message_counter = 0

    @task(5)
    def enqueue_message(self) -> None:
        message_id = f"msg_{id(self)}_{self.message_counter}"
        self.message_counter += 1

        payload = {
            "type": "enqueue",
            "queue_name": "test_queue",
            "data": {"content": f"test message {self.message_counter}"},
            "message_id": message_id,
            "timestamp": time.time(),
        }

        with self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to enqueue: {response.status_code}")

    @task(3)
    def dequeue_message(self) -> None:
        payload = {
            "type": "dequeue",
            "queue_name": "test_queue",
            "timeout": 1.0,
        }

        with self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                if response.content:
                    data = ormsgpack.unpackb(response.content)
                    if data.get("message"):
                        response.success()
                    else:
                        response.success()
                else:
                    response.failure("Empty response")
            else:
                response.failure(f"Failed to dequeue: {response.status_code}")


class CacheUser(HttpUser):
    wait_time = between(0.1, 1)
    host = "http://localhost:8021"

    def on_start(self) -> None:
        self.key_counter = 0

    @task(7)
    def cache_get(self) -> None:
        key = f"key_{self.key_counter % 1000}"

        payload = {
            "type": "cache_get",
            "key": key,
        }

        with self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                if response.content:
                    data = ormsgpack.unpackb(response.content)
                    if data.get("found"):
                        response.success()
                    else:
                        response.success()
                else:
                    response.failure("Empty response")
            else:
                response.failure(f"Failed to get from cache: {response.status_code}")

    @task(3)
    def cache_put(self) -> None:
        key = f"key_{self.key_counter}"
        self.key_counter += 1

        payload = {
            "type": "cache_put",
            "key": key,
            "value": {"data": f"value_{self.key_counter}"},
        }

        with self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
            catch_response=True,
        ) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Failed to put to cache: {response.status_code}")


class MixedWorkloadUser(HttpUser):
    wait_time = between(0.5, 2)
    host = "http://localhost:8001"

    def on_start(self) -> None:
        self.counter = 0
        self.locks_host = "http://localhost:8001"
        self.queue_host = "http://localhost:8011"
        self.cache_host = "http://localhost:8021"

    @task(3)
    def lock_operation(self) -> None:
        self.client.base_url = self.locks_host
        lock_id = f"mixed_lock_{self.counter}"
        self.counter += 1

        payload = {
            "type": "lock_request",
            "lock_id": lock_id,
            "lock_type": "exclusive",
            "requester": f"mixed_{id(self)}",
        }

        self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
        )

    @task(5)
    def queue_operation(self) -> None:
        self.client.base_url = self.queue_host

        payload = {
            "type": "enqueue",
            "queue_name": "mixed_queue",
            "data": {"content": f"mixed message {self.counter}"},
            "message_id": f"mixed_msg_{self.counter}",
            "timestamp": time.time(),
        }

        self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
        )

    @task(7)
    def cache_operation(self) -> None:
        self.client.base_url = self.cache_host

        if self.counter % 3 == 0:
            payload = {
                "type": "cache_put",
                "key": f"mixed_key_{self.counter}",
                "value": {"data": f"mixed_value_{self.counter}"},
            }
        else:
            payload = {
                "type": "cache_get",
                "key": f"mixed_key_{self.counter % 100}",
            }

        self.client.post(
            "/message",
            data=ormsgpack.packb(payload),
            headers={"Content-Type": "application/msgpack"},
        )
