import asyncio
import time
from collections import OrderedDict
from dataclasses import dataclass
from enum import Enum

from loguru import logger

from .base_node import BaseNode


class MESIState(Enum):
    MODIFIED = "modified"
    EXCLUSIVE = "exclusive"
    SHARED = "shared"
    INVALID = "invalid"


@dataclass
class CacheEntry:
    key: str
    value: object
    state: MESIState
    timestamp: float
    last_access: float

    def is_valid(self) -> bool:
        return self.state != MESIState.INVALID


class LRUCache:
    capacity: int
    cache: OrderedDict[str, CacheEntry]

    def __init__(self, capacity: int) -> None:
        self.capacity = capacity
        self.cache = OrderedDict()

    def get(self, key: str) -> CacheEntry | None:
        if key in self.cache:
            entry = self.cache[key]
            entry.last_access = time.time()
            self.cache.move_to_end(key)
            return entry
        return None

    def put(self, key: str, entry: CacheEntry) -> CacheEntry | None:
        evicted = None
        if key in self.cache:
            self.cache.move_to_end(key)
        elif len(self.cache) >= self.capacity:
            evicted_key, evicted = self.cache.popitem(last=False)
            logger.debug(f"Evicted cache entry {evicted_key}")
        self.cache[key] = entry
        return evicted

    def remove(self, key: str) -> CacheEntry | None:
        return self.cache.pop(key, None)

    def clear(self) -> None:
        self.cache.clear()

    def size(self) -> int:
        return len(self.cache)


class CacheNode(BaseNode):
    node_addresses: dict[str, tuple[str, int]]
    cache: LRUCache
    cache_size: int
    _metrics_task: asyncio.Task[None] | None

    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        cluster_nodes: list[str],
        node_addresses: dict[str, tuple[str, int]],
        cache_size: int = 1000,
    ) -> None:
        super().__init__(node_id, host, port, cluster_nodes)
        self.node_addresses = node_addresses
        self.cache = LRUCache(cache_size)
        self.cache_size = cache_size
        self._metrics_task = None

    async def start(self) -> None:
        await super().start()
        self.message_passing.register_handler("cache_get", self._handle_get)
        self.message_passing.register_handler("cache_put", self._handle_put)
        self.message_passing.register_handler(
            "cache_invalidate", self._handle_invalidate
        )
        self.message_passing.register_handler(
            "cache_read_request", self._handle_read_request
        )
        self.message_passing.register_handler(
            "cache_write_request", self._handle_write_request
        )
        self._metrics_task = asyncio.create_task(self._metrics_loop())
        logger.info(f"Cache node {self.node_id} started with size {self.cache_size}")

    async def stop(self) -> None:
        if self._metrics_task:
            self._metrics_task.cancel()
            try:
                await self._metrics_task
            except asyncio.CancelledError:
                pass
        await super().stop()
        logger.info(f"Cache node {self.node_id} stopped")

    async def get(self, key: str) -> object | None:
        self.metrics.start_timer(f"cache_get_{key}")
        entry = self.cache.get(key)
        if entry and entry.is_valid():
            if entry.state == MESIState.INVALID:
                await self._handle_cache_miss(key)
                entry = self.cache.get(key)
            if entry:
                self.metrics.increment_counter("cache_hits")
                self.metrics.stop_timer(f"cache_get_{key}")
                return entry.value
        self.metrics.increment_counter("cache_misses")
        self.metrics.stop_timer(f"cache_get_{key}")
        return None

    async def put(self, key: str, value: object) -> bool:
        self.metrics.start_timer(f"cache_put_{key}")
        entry = CacheEntry(
            key=key,
            value=value,
            state=MESIState.MODIFIED,
            timestamp=time.time(),
            last_access=time.time(),
        )
        await self._invalidate_other_caches(key)
        evicted = self.cache.put(key, entry)
        if evicted and evicted.state == MESIState.MODIFIED:
            await self._write_back(evicted)
        self.metrics.increment_counter("cache_puts")
        self.metrics.stop_timer(f"cache_put_{key}")
        return True

    async def invalidate(self, key: str) -> bool:
        entry = self.cache.get(key)
        if entry:
            entry.state = MESIState.INVALID
            logger.debug(f"Invalidated cache entry {key}")
            self.metrics.increment_counter("cache_invalidations")
            return True
        return False

    async def _handle_get(self, message: dict[str, object]) -> dict[str, object]:
        key_obj = message.get("key", "")
        key = str(key_obj) if isinstance(key_obj, str) else ""
        value = await self.get(key)
        return {
            "type": "cache_get_response",
            "key": key,
            "value": value,
            "found": value is not None,
        }

    async def _handle_put(self, message: dict[str, object]) -> dict[str, object]:
        key_obj = message.get("key", "")
        key = str(key_obj) if isinstance(key_obj, str) else ""
        value = message.get("value")
        success = await self.put(key, value)
        return {"type": "cache_put_response", "key": key, "success": success}

    async def _handle_invalidate(self, message: dict[str, object]) -> dict[str, object]:
        key_obj = message.get("key", "")
        key = str(key_obj) if isinstance(key_obj, str) else ""
        success = await self.invalidate(key)
        return {"type": "cache_invalidate_response", "key": key, "success": success}

    async def _handle_read_request(
        self, message: dict[str, object]
    ) -> dict[str, object]:
        key_obj = message.get("key", "")
        key = str(key_obj) if isinstance(key_obj, str) else ""
        entry = self.cache.get(key)
        if entry and entry.is_valid():
            if entry.state == MESIState.MODIFIED or entry.state == MESIState.EXCLUSIVE:
                entry.state = MESIState.SHARED
            return {
                "type": "cache_read_response",
                "key": key,
                "value": entry.value,
                "found": True,
            }
        return {
            "type": "cache_read_response",
            "key": key,
            "value": None,
            "found": False,
        }

    async def _handle_write_request(
        self, message: dict[str, object]
    ) -> dict[str, object]:
        key_obj = message.get("key", "")
        key = str(key_obj) if isinstance(key_obj, str) else ""
        entry = self.cache.get(key)
        if entry:
            if entry.state == MESIState.MODIFIED:
                await self._write_back(entry)
            entry.state = MESIState.INVALID
        return {"type": "cache_write_response", "key": key, "success": True}

    async def _handle_cache_miss(self, key: str) -> None:
        for node_id in self.cluster_nodes:
            if node_id == self.node_id:
                continue
            if node_id not in self.node_addresses:
                continue
            host, port = self.node_addresses[node_id]
            message: dict[str, object] = {"type": "cache_read_request", "key": key}
            response = await self.message_passing.send_message(host, port, message)
            if response and response.get("found"):
                value = response.get("value")
                entry = CacheEntry(
                    key=key,
                    value=value,
                    state=MESIState.SHARED,
                    timestamp=time.time(),
                    last_access=time.time(),
                )
                self.cache.put(key, entry)
                logger.debug(f"Retrieved {key} from node {node_id}")
                return

    async def _invalidate_other_caches(self, key: str) -> None:
        tasks = []
        for node_id in self.cluster_nodes:
            if node_id == self.node_id:
                continue
            if node_id not in self.node_addresses:
                continue
            host, port = self.node_addresses[node_id]
            message: dict[str, object] = {"type": "cache_write_request", "key": key}
            task = self.message_passing.send_message(host, port, message)
            tasks.append(task)
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def _write_back(self, entry: CacheEntry) -> None:
        logger.debug(f"Writing back modified entry {entry.key}")
        self.metrics.increment_counter("cache_writebacks")

    async def _metrics_loop(self) -> None:
        while self._running:
            self.metrics.set_gauge("cache_size", self.cache.size())
            hit_count = self.metrics.get_counter("cache_hits")
            miss_count = self.metrics.get_counter("cache_misses")
            total = hit_count + miss_count
            if total > 0:
                hit_rate = hit_count / total
                self.metrics.set_gauge("cache_hit_rate", hit_rate)
            await asyncio.sleep(5.0)


__all__ = ["CacheNode", "MESIState", "CacheEntry", "LRUCache"]
