from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from enum import Enum


class ConsensusState(Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"
    PRE_PREPARE = "pre_prepare"
    PREPARE = "prepare"
    COMMIT = "commit"


class ConsensusMessage:
    message_type: str
    sender_id: str
    term: int
    data: dict[str, object]
    timestamp: float

    def __init__(
        self,
        message_type: str,
        sender_id: str,
        term: int,
        data: dict[str, object] | None = None,
    ) -> None:
        self.message_type = message_type
        self.sender_id = sender_id
        self.term = term
        self.data = data or {}
        self.timestamp = 0.0

    def to_dict(self) -> dict[str, object]:
        return {
            "message_type": self.message_type,
            "sender_id": self.sender_id,
            "term": self.term,
            "data": self.data,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, data: dict[str, object]) -> "ConsensusMessage":
        data_field = data.get("data")
        data_dict: dict[str, object] | None = (
            data_field if isinstance(data_field, dict) else None
        )
        msg = cls(
            message_type=str(data["message_type"]),
            sender_id=str(data["sender_id"]),
            term=int(data["term"]) if isinstance(data["term"], (int, float)) else 0,
            data=data_dict,
        )
        timestamp_val = data.get("timestamp", 0.0)
        msg.timestamp = (
            float(timestamp_val) if isinstance(timestamp_val, (int, float)) else 0.0
        )
        return msg


class ConsensusProtocol(ABC):
    node_id: str
    cluster_nodes: list[str]
    state: ConsensusState
    current_term: int
    commit_callbacks: list[Callable[[dict[str, object]], Awaitable[None]]]

    def __init__(self, node_id: str, cluster_nodes: list[str]) -> None:
        self.node_id = node_id
        self.cluster_nodes = cluster_nodes
        self.state = ConsensusState.FOLLOWER
        self.current_term = 0
        self.commit_callbacks = []

    @abstractmethod
    async def start(self) -> None:
        pass

    @abstractmethod
    async def stop(self) -> None:
        pass

    @abstractmethod
    async def propose(self, data: object) -> bool:
        pass

    @abstractmethod
    async def handle_message(
        self, message: ConsensusMessage
    ) -> ConsensusMessage | None:
        pass

    @abstractmethod
    def is_leader(self) -> bool:
        pass

    @abstractmethod
    def get_leader_id(self) -> str | None:
        pass

    def register_commit_callback(
        self, callback: Callable[[dict[str, object]], Awaitable[None]]
    ) -> None:
        self.commit_callbacks.append(callback)

    async def _notify_committed(self, data: dict[str, object]) -> None:
        for callback in self.commit_callbacks:
            await callback(data)


__all__ = ["ConsensusProtocol", "ConsensusMessage", "ConsensusState"]
