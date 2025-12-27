from __future__ import annotations

from asyncio import (
    Lock,
)
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum, auto
from time import monotonic
from typing import TYPE_CHECKING, Any

from loguru import logger

from src.communication.message_passing import Message, MessageType
from src.nodes.base_node import BaseNode, NodeRole
from src.utils.config import Config
from src.utils.metrics import MetricsCollector, Timer

if TYPE_CHECKING:
    pass


class MESIState(Enum):
    MODIFIED = auto()
    EXCLUSIVE = auto()
    SHARED = auto()
    INVALID = auto()


class BusTransaction(Enum):
    BUS_READ = auto()
    BUS_READ_X = auto()
    BUS_UPGRADE = auto()
    FLUSH = auto()


@dataclass(slots=True)
class CacheLine:
    key: str
    value: bytes
    state: MESIState
    version: int = 0
    last_access: float = 0.0


@dataclass(slots=True)
class CacheStats:
    hits: int = 0
    misses: int = 0
    invalidations: int = 0
    flushes: int = 0


class LRUCache:
    def __init__(self, capacity: int) -> None:
        self._capacity = capacity
        self._cache: OrderedDict[str, CacheLine] = OrderedDict()

    def get(self, key: str) -> CacheLine | None:
        if key not in self._cache:
            return None
        self._cache.move_to_end(key)
        entry = self._cache[key]
        entry.last_access = monotonic()
        return entry

    def put(self, key: str, line: CacheLine) -> CacheLine | None:
        evicted: CacheLine | None = None
        if key in self._cache:
            self._cache.move_to_end(key)
        else:
            if len(self._cache) >= self._capacity:
                _, evicted = self._cache.popitem(last=False)
        self._cache[key] = line
        return evicted

    def remove(self, key: str) -> CacheLine | None:
        return self._cache.pop(key, None)

    def contains(self, key: str) -> bool:
        return key in self._cache

    def keys(self) -> list[str]:
        return list(self._cache.keys())

    def values(self) -> list[CacheLine]:
        return list(self._cache.values())

    def __len__(self) -> int:
        return len(self._cache)


class CacheNode(BaseNode):
    def __init__(
        self,
        config: Config,
        cache_size: int = 1000,
    ) -> None:
        super().__init__(config, NodeRole.CACHE_NODE)
        self._cache = LRUCache(cache_size)
        self._stats = CacheStats()
        self._memory_store: dict[str, tuple[bytes, int]] = {}
        self._metrics = MetricsCollector()
        self._bus_lock = Lock()

    async def _on_start(self) -> None:
        logger.info(f"Cache node started with size {self._cache._capacity}")

    async def _on_stop(self) -> None:
        await self._flush_all_modified()

    def _register_handlers(self) -> None:
        super()._register_handlers()
        self._message_bus.register_handler(
            MessageType.CACHE_READ,
            self._handle_cache_read,
        )
        self._message_bus.register_handler(
            MessageType.CACHE_WRITE,
            self._handle_cache_write,
        )
        self._message_bus.register_handler(
            MessageType.CACHE_INVALIDATE,
            self._handle_cache_invalidate,
        )
        self._message_bus.register_handler(
            MessageType.BUS_READ,
            self._handle_bus_read,
        )
        self._message_bus.register_handler(
            MessageType.BUS_READ_X,
            self._handle_bus_read_x,
        )
        self._message_bus.register_handler(
            MessageType.BUS_UPGRADE,
            self._handle_bus_upgrade,
        )

    async def read(self, key: str) -> bytes | None:
        with Timer(
            lambda t: self._metrics.record_request(
                self.node_id, "cache_read", "success", t
            )
        ):
            line = self._cache.get(key)

            if line and line.state != MESIState.INVALID:
                self._stats.hits += 1
                self._metrics.record_cache_hit(self.node_id)
                return line.value

            self._stats.misses += 1
            self._metrics.record_cache_miss(self.node_id)

            value, version = await self._fetch_from_memory_or_peers(key)
            if value is None:
                return None

            other_cached = await self._bus_read(key)

            if other_cached:
                new_line = CacheLine(
                    key=key,
                    value=value,
                    state=MESIState.SHARED,
                    version=version,
                    last_access=monotonic(),
                )
            else:
                new_line = CacheLine(
                    key=key,
                    value=value,
                    state=MESIState.EXCLUSIVE,
                    version=version,
                    last_access=monotonic(),
                )

            evicted = self._cache.put(key, new_line)
            if evicted and evicted.state == MESIState.MODIFIED:
                await self._write_back(evicted)

            return value

    async def write(self, key: str, value: bytes) -> bool:
        with Timer(
            lambda t: self._metrics.record_request(
                self.node_id, "cache_write", "success", t
            )
        ):
            line = self._cache.get(key)

            if line:
                if line.state == MESIState.MODIFIED:
                    line.value = value
                    line.version += 1
                    return True
                elif line.state == MESIState.EXCLUSIVE:
                    line.value = value
                    line.state = MESIState.MODIFIED
                    line.version += 1
                    return True
                elif line.state == MESIState.SHARED:
                    await self._bus_upgrade(key)
                    line.value = value
                    line.state = MESIState.MODIFIED
                    line.version += 1
                    return True
                else:
                    await self._bus_read_x(key)

            else:
                await self._bus_read_x(key)

            new_line = CacheLine(
                key=key,
                value=value,
                state=MESIState.MODIFIED,
                version=1,
                last_access=monotonic(),
            )
            evicted = self._cache.put(key, new_line)
            if evicted and evicted.state == MESIState.MODIFIED:
                await self._write_back(evicted)

            return True

    async def invalidate(self, key: str) -> bool:
        line = self._cache.get(key)
        if not line:
            return True

        if line.state == MESIState.MODIFIED:
            await self._write_back(line)

        line.state = MESIState.INVALID
        self._stats.invalidations += 1
        return True

    async def _bus_read(self, key: str) -> bool:
        async with self._bus_lock:
            message = Message(
                msg_type=MessageType.BUS_READ,
                sender_id=self.node_id,
                payload={"key": key},
            )
            responses = await self.broadcast_to_peers(message)
            return any(r and r.payload.get("has_copy") for r in responses if r)

    async def _bus_read_x(self, key: str) -> None:
        async with self._bus_lock:
            message = Message(
                msg_type=MessageType.BUS_READ_X,
                sender_id=self.node_id,
                payload={"key": key},
            )
            await self.broadcast_to_peers(message)

    async def _bus_upgrade(self, key: str) -> None:
        async with self._bus_lock:
            message = Message(
                msg_type=MessageType.BUS_UPGRADE,
                sender_id=self.node_id,
                payload={"key": key},
            )
            await self.broadcast_to_peers(message)

    async def _handle_bus_read(self, message: Message) -> Message:
        key = message.payload["key"]
        line = self._cache.get(key)

        has_copy = False
        if line and line.state != MESIState.INVALID:
            has_copy = True
            if line.state == MESIState.EXCLUSIVE:
                line.state = MESIState.SHARED
            elif line.state == MESIState.MODIFIED:
                await self._write_back(line)
                line.state = MESIState.SHARED

        return Message(
            msg_type=MessageType.CACHE_RESPONSE,
            sender_id=self.node_id,
            payload={"has_copy": has_copy},
        )

    async def _handle_bus_read_x(self, message: Message) -> Message:
        key = message.payload["key"]
        await self.invalidate(key)
        return Message(
            msg_type=MessageType.CACHE_RESPONSE,
            sender_id=self.node_id,
            payload={"invalidated": True},
        )

    async def _handle_bus_upgrade(self, message: Message) -> Message:
        key = message.payload["key"]
        await self.invalidate(key)
        return Message(
            msg_type=MessageType.CACHE_RESPONSE,
            sender_id=self.node_id,
            payload={"invalidated": True},
        )

    async def _fetch_from_memory_or_peers(
        self,
        key: str,
    ) -> tuple[bytes | None, int]:
        if key in self._memory_store:
            return self._memory_store[key]

        for peer in self.get_alive_peers():
            message = Message(
                msg_type=MessageType.CACHE_READ,
                sender_id=self.node_id,
                payload={"key": key, "from_memory": True},
            )
            response = await self.send_to_peer(peer.node_id, message)
            if response and response.payload.get("found"):
                return response.payload["value"], response.payload["version"]

        return None, 0

    async def _write_back(self, line: CacheLine) -> None:
        self._memory_store[line.key] = (line.value, line.version)
        self._stats.flushes += 1
        logger.debug(f"Write-back for key {line.key}")

    async def _flush_all_modified(self) -> None:
        for line in self._cache.values():
            if line.state == MESIState.MODIFIED:
                await self._write_back(line)

    async def _handle_cache_read(self, message: Message) -> Message:
        key = message.payload["key"]
        from_memory = message.payload.get("from_memory", False)

        if from_memory:
            if key in self._memory_store:
                value, version = self._memory_store[key]
                return Message(
                    msg_type=MessageType.CACHE_RESPONSE,
                    sender_id=self.node_id,
                    payload={"found": True, "value": value, "version": version},
                )
            return Message(
                msg_type=MessageType.CACHE_RESPONSE,
                sender_id=self.node_id,
                payload={"found": False},
            )

        value = await self.read(key)
        return Message(
            msg_type=MessageType.CACHE_RESPONSE,
            sender_id=self.node_id,
            payload={"found": value is not None, "value": value},
        )

    async def _handle_cache_write(self, message: Message) -> Message:
        key = message.payload["key"]
        value = message.payload["value"]
        success = await self.write(key, value)
        return Message(
            msg_type=MessageType.CACHE_RESPONSE,
            sender_id=self.node_id,
            payload={"success": success},
        )

    async def _handle_cache_invalidate(self, message: Message) -> Message:
        key = message.payload["key"]
        success = await self.invalidate(key)
        return Message(
            msg_type=MessageType.CACHE_RESPONSE,
            sender_id=self.node_id,
            payload={"success": success},
        )

    def get_stats(self) -> dict[str, Any]:
        total = self._stats.hits + self._stats.misses
        hit_rate = self._stats.hits / total if total > 0 else 0.0
        return {
            "hits": self._stats.hits,
            "misses": self._stats.misses,
            "hit_rate": hit_rate,
            "invalidations": self._stats.invalidations,
            "flushes": self._stats.flushes,
            "cache_size": len(self._cache),
        }

    def get_cache_state(self) -> dict[str, dict[str, Any]]:
        return {
            line.key: {
                "state": line.state.name,
                "version": line.version,
                "last_access": line.last_access,
            }
            for line in self._cache.values()
        }
