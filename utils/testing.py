from __future__ import annotations

from pathlib import Path
from subprocess import PIPE, run
from time import sleep
from typing import Any, TypeAlias
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from orjson import dumps, loads

ResponseData: TypeAlias = dict[str, Any]

DEFAULT_COMPOSE_DIR: str = "docker"
LOCK_MANAGER_URL: str = "http://localhost:18000"
QUEUE_NODE_URL: str = "http://localhost:18100"
CACHE_NODE_URL: str = "http://localhost:18200"


def _get_compose_path(compose_dir: str = DEFAULT_COMPOSE_DIR) -> Path:
    return Path(__file__).parent.parent / compose_dir


def _run_compose_command(
    args: list[str],
    compose_dir: str = DEFAULT_COMPOSE_DIR,
    timeout: int = 60,
) -> bool:
    result = run(
        ["docker", "compose", *args],
        cwd=_get_compose_path(compose_dir),
        stdout=PIPE,
        stderr=PIPE,
        timeout=timeout,
    )
    return result.returncode == 0


def get_request(url: str, timeout: int = 30) -> tuple[int | None, str | None]:
    try:
        request = Request(url)
        response = urlopen(url=request, timeout=timeout)
        with response:
            return response.getcode(), response.read().decode("utf-8")
    except HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception:
        return None, None


def post_request(
    url: str,
    data: dict[str, Any],
    timeout: int = 30,
) -> tuple[int | None, str | None]:
    try:
        request = Request(
            url,
            data=dumps(data),
            headers={"Content-Type": "application/json"},
        )
        response = urlopen(url=request, timeout=timeout)
        with response:
            return response.getcode(), response.read().decode("utf-8")
    except HTTPError as e:
        return e.code, e.read().decode("utf-8")
    except Exception:
        return None, None


def wait_for_server(url: str, max_retries: int = 30) -> bool:
    for _ in range(max_retries):
        status, _ = get_request(f"{url}/health")
        if status == 200:
            return True
        sleep(1)
    return False


def is_environment_running() -> bool:
    lock_ok = get_request(f"{LOCK_MANAGER_URL}/health")[0] == 200
    queue_ok = get_request(f"{QUEUE_NODE_URL}/health")[0] == 200
    cache_ok = get_request(f"{CACHE_NODE_URL}/health")[0] == 200
    return lock_ok and queue_ok and cache_ok


def flush_redis(compose_dir: str = DEFAULT_COMPOSE_DIR) -> bool:
    return _run_compose_command(
        ["exec", "-T", "redis", "redis-cli", "FLUSHDB"],
        compose_dir,
        timeout=5,
    )


def reset_environment(
    compose_dir: str = DEFAULT_COMPOSE_DIR,
    max_wait: int = 120,
) -> bool:
    if not _run_compose_command(
        ["down", "-v", "--remove-orphans"],
        compose_dir,
        timeout=60,
    ):
        return False

    if not _run_compose_command(
        ["up", "-d", "--build", "--wait"],
        compose_dir,
        timeout=300,
    ):
        return False

    lock_ready = wait_for_server(LOCK_MANAGER_URL, max_retries=max_wait)
    queue_ready = wait_for_server(QUEUE_NODE_URL, max_retries=max_wait)
    cache_ready = wait_for_server(CACHE_NODE_URL, max_retries=max_wait)
    return lock_ready and queue_ready and cache_ready


def fast_reset_environment() -> bool:
    redis_success = flush_redis()
    sleep(0.5)
    return redis_success


def restart_container(
    service_name: str,
    compose_dir: str = DEFAULT_COMPOSE_DIR,
    max_wait: int = 60,
) -> bool:
    if not _run_compose_command(
        ["restart", service_name],
        compose_dir,
        timeout=30,
    ):
        return False

    port_map = {
        "lock-manager-1": 18000,
        "lock-manager-2": 18001,
        "lock-manager-3": 18002,
        "queue-1": 18100,
        "queue-2": 18101,
        "queue-3": 18102,
        "cache-1": 18200,
        "cache-2": 18201,
        "cache-3": 18202,
    }
    port = port_map.get(service_name, 18000)
    return wait_for_server(f"http://localhost:{port}", max_retries=max_wait)


def stop_container(
    service_name: str,
    compose_dir: str = DEFAULT_COMPOSE_DIR,
) -> bool:
    return _run_compose_command(
        ["stop", service_name],
        compose_dir,
        timeout=30,
    )


def start_container(
    service_name: str,
    compose_dir: str = DEFAULT_COMPOSE_DIR,
) -> bool:
    return _run_compose_command(
        ["start", service_name],
        compose_dir,
        timeout=30,
    )


def acquire_lock(
    resource_id: str,
    lock_type: str = "exclusive",
    client_id: str = "test-client",
    url: str = LOCK_MANAGER_URL,
) -> tuple[int | None, ResponseData]:
    status, response = post_request(
        f"{url}/lock/acquire",
        {
            "resource_id": resource_id,
            "lock_type": lock_type,
            "client_id": client_id,
        },
    )
    return status, loads(response or "{}")


def release_lock(
    lock_id: str,
    url: str = LOCK_MANAGER_URL,
) -> tuple[int | None, ResponseData]:
    status, response = post_request(
        f"{url}/lock/release",
        {"lock_id": lock_id},
    )
    return status, loads(response or "{}")


def get_lock_info(
    resource_id: str,
    url: str = LOCK_MANAGER_URL,
) -> tuple[int | None, ResponseData]:
    status, response = get_request(f"{url}/lock/{resource_id}")
    return status, loads(response or "{}")


def publish_message(
    topic: str,
    payload: bytes | str,
    key: str | None = None,
    url: str = QUEUE_NODE_URL,
) -> tuple[int | None, ResponseData]:
    if isinstance(payload, str):
        payload = payload.encode()
    data: dict[str, Any] = {
        "topic": topic,
        "payload": payload.decode("utf-8"),
    }
    if key:
        data["key"] = key
    status, response = post_request(f"{url}/publish", data)
    return status, loads(response or "{}")


def consume_messages(
    topic: str,
    consumer_group: str,
    consumer_id: str,
    max_messages: int = 10,
    url: str = QUEUE_NODE_URL,
) -> tuple[int | None, ResponseData]:
    status, response = post_request(
        f"{url}/consume",
        {
            "topic": topic,
            "consumer_group": consumer_group,
            "consumer_id": consumer_id,
            "max_messages": max_messages,
        },
    )
    return status, loads(response or "{}")


def acknowledge_message(
    message_id: str,
    url: str = QUEUE_NODE_URL,
) -> tuple[int | None, ResponseData]:
    status, response = post_request(
        f"{url}/ack",
        {"message_id": message_id},
    )
    return status, loads(response or "{}")


def cache_read(
    key: str,
    url: str = CACHE_NODE_URL,
) -> tuple[int | None, ResponseData]:
    status, response = get_request(f"{url}/cache/{key}")
    return status, loads(response or "{}")


def cache_write(
    key: str,
    value: bytes | str,
    url: str = CACHE_NODE_URL,
) -> tuple[int | None, ResponseData]:
    if isinstance(value, str):
        value = value.encode()
    status, response = post_request(
        f"{url}/cache",
        {"key": key, "value": value.decode("utf-8")},
    )
    return status, loads(response or "{}")


def cache_invalidate(
    key: str,
    url: str = CACHE_NODE_URL,
) -> tuple[int | None, ResponseData]:
    status, response = post_request(
        f"{url}/cache/invalidate",
        {"key": key},
    )
    return status, loads(response or "{}")


def get_cache_stats(url: str = CACHE_NODE_URL) -> tuple[int | None, ResponseData]:
    status, response = get_request(f"{url}/stats")
    return status, loads(response or "{}")


def get_node_health(url: str) -> tuple[int | None, ResponseData]:
    status, response = get_request(f"{url}/health")
    return status, loads(response or "{}")


def get_raft_state(url: str = LOCK_MANAGER_URL) -> tuple[int | None, ResponseData]:
    status, response = get_request(f"{url}/raft/state")
    return status, loads(response or "{}")
