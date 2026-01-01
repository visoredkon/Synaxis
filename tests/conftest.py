from __future__ import annotations

from pathlib import Path
from sys import path
from typing import TYPE_CHECKING, Any

path.insert(0, str(Path(__file__).parent.parent))

from pytest import fixture
from utils.testing import (
    CACHE_NODE_URL,
    LOCK_MANAGER_URL,
    QUEUE_NODE_URL,
    fast_reset_environment,
    is_environment_running,
    reset_environment,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Generator

    from src.communication.message_passing import Message, MessageType


class MockMessageBus:
    def __init__(self) -> None:
        self._handlers: dict[MessageType, Callable[[Message], Any]] = {}
        self._sent: list[tuple[str, int, Message]] = []

    def register_handler(
        self,
        msg_type: MessageType,
        handler: Callable[[Message], Any],
    ) -> None:
        self._handlers[msg_type] = handler

    async def send(
        self,
        host: str,
        port: int,
        message: Message,
    ) -> Message | None:
        self._sent.append((host, port, message))
        return None


def _ensure_environment_up() -> None:
    if is_environment_running():
        return
    success = reset_environment()
    assert success, "Gagal menginisialisasi environment"


@fixture(scope="session", autouse=True)
def test_session_lifecycle() -> Generator[None, None, None]:
    _ensure_environment_up()
    yield


@fixture(scope="module")
def fresh_environment() -> Generator[dict[str, str], None, None]:
    _ensure_environment_up()
    success = fast_reset_environment()
    assert success, "Gagal reset environment"
    yield {
        "lock_manager": LOCK_MANAGER_URL,
        "queue_node": QUEUE_NODE_URL,
        "cache_node": CACHE_NODE_URL,
    }


@fixture
def lock_manager_url(fresh_environment: dict[str, str]) -> str:
    return fresh_environment["lock_manager"]


@fixture
def queue_node_url(fresh_environment: dict[str, str]) -> str:
    return fresh_environment["queue_node"]


@fixture
def cache_node_url(fresh_environment: dict[str, str]) -> str:
    return fresh_environment["cache_node"]


@fixture
def all_lock_managers() -> list[str]:
    return [
        "http://localhost:18000",
        "http://localhost:18001",
        "http://localhost:18002",
    ]


@fixture
def all_queue_nodes() -> list[str]:
    return [
        "http://localhost:18100",
        "http://localhost:18101",
        "http://localhost:18102",
    ]


@fixture
def all_cache_nodes() -> list[str]:
    return [
        "http://localhost:18200",
        "http://localhost:18201",
        "http://localhost:18202",
    ]


@fixture
def mock_bus() -> MockMessageBus:
    return MockMessageBus()
