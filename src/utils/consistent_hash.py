from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass, field
from hashlib import md5
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Sequence


@dataclass(slots=True)
class ConsistentHash:
    virtual_nodes: int = 150
    _ring: dict[int, str] = field(default_factory=dict)
    _sorted_keys: list[int] = field(default_factory=list)
    _nodes: set[str] = field(default_factory=set)

    def _hash(self, key: str) -> int:
        return int(md5(key.encode(), usedforsecurity=False).hexdigest(), 16)

    def add_node(self, node_id: str) -> None:
        if node_id in self._nodes:
            return
        self._nodes.add(node_id)
        for i in range(self.virtual_nodes):
            virtual_key = f"{node_id}:{i}"
            h = self._hash(virtual_key)
            self._ring[h] = node_id
        self._sorted_keys = sorted(self._ring.keys())

    def remove_node(self, node_id: str) -> None:
        if node_id not in self._nodes:
            return
        self._nodes.discard(node_id)
        for i in range(self.virtual_nodes):
            virtual_key = f"{node_id}:{i}"
            h = self._hash(virtual_key)
            self._ring.pop(h, None)
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> str | None:
        if not self._ring:
            return None
        h = self._hash(key)
        idx = bisect_right(self._sorted_keys, h)
        if idx >= len(self._sorted_keys):
            idx = 0
        return self._ring[self._sorted_keys[idx]]

    def get_nodes_for_key(self, key: str, count: int = 1) -> list[str]:
        if not self._ring:
            return []
        h = self._hash(key)
        idx = bisect_right(self._sorted_keys, h)
        result: list[str] = []
        seen: set[str] = set()
        for _ in range(len(self._sorted_keys)):
            if idx >= len(self._sorted_keys):
                idx = 0
            node = self._ring[self._sorted_keys[idx]]
            if node not in seen:
                result.append(node)
                seen.add(node)
                if len(result) >= count:
                    break
            idx += 1
        return result

    def get_all_nodes(self) -> Sequence[str]:
        return list(self._nodes)

    @property
    def node_count(self) -> int:
        return len(self._nodes)
