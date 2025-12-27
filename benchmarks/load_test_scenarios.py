from __future__ import annotations

from locust import HttpUser, between, events, task
from locust.env import Environment
from locust.runners import MasterRunner


class LockManagerUser(HttpUser):
    wait_time = between(0.1, 0.5)
    host = "http://localhost:8000"

    @task(3)
    def acquire_release_lock(self) -> None:
        resource_id = f"resource-{self.environment.runner.user_count % 10}"
        acquire_response = self.client.post(
            "/message",
            json={
                "msg_type": 7,
                "sender_id": f"client-{id(self)}",
                "payload": {
                    "resource_id": resource_id,
                    "lock_type": "EXCLUSIVE",
                    "timeout": 5.0,
                },
                "term": 0,
            },
            name="acquire_lock",
        )
        if acquire_response.status_code == 200:
            self.client.post(
                "/message",
                json={
                    "msg_type": 8,
                    "sender_id": f"client-{id(self)}",
                    "payload": {
                        "resource_id": resource_id,
                        "lock_type": "EXCLUSIVE",
                        "lock_id": "test",
                    },
                    "term": 0,
                },
                name="release_lock",
            )

    @task(1)
    def health_check(self) -> None:
        self.client.get("/health", name="health_check")


class QueueUser(HttpUser):
    wait_time = between(0.1, 0.5)
    host = "http://localhost:8100"

    @task(2)
    def publish_message(self) -> None:
        self.client.post(
            "/message",
            json={
                "msg_type": 10,
                "sender_id": f"producer-{id(self)}",
                "payload": {
                    "topic": "test-topic",
                    "payload": b"test message".hex(),
                    "key": None,
                },
                "term": 0,
            },
            name="publish",
        )

    @task(2)
    def consume_message(self) -> None:
        self.client.post(
            "/message",
            json={
                "msg_type": 11,
                "sender_id": f"consumer-{id(self)}",
                "payload": {
                    "topic": "test-topic",
                    "consumer_group": "test-group",
                    "max_messages": 10,
                },
                "term": 0,
            },
            name="consume",
        )


class CacheUser(HttpUser):
    wait_time = between(0.1, 0.5)
    host = "http://localhost:8200"

    @task(3)
    def read_cache(self) -> None:
        key = f"key-{self.environment.runner.user_count % 100}"
        self.client.post(
            "/message",
            json={
                "msg_type": 14,
                "sender_id": f"client-{id(self)}",
                "payload": {"key": key},
                "term": 0,
            },
            name="cache_read",
        )

    @task(1)
    def write_cache(self) -> None:
        key = f"key-{self.environment.runner.user_count % 100}"
        self.client.post(
            "/message",
            json={
                "msg_type": 15,
                "sender_id": f"client-{id(self)}",
                "payload": {"key": key, "value": b"test-value".hex()},
                "term": 0,
            },
            name="cache_write",
        )


@events.init.add_listener
def on_locust_init(environment: Environment, **kwargs: object) -> None:
    if isinstance(environment.runner, MasterRunner):
        print("Master node started")
