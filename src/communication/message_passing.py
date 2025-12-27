from __future__ import annotations

from asyncio import TimeoutError as AsyncTimeoutError
from asyncio import gather
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum, auto
from time import monotonic
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from aiohttp import ClientError, ClientSession, ClientTimeout
from aiohttp.web import (
    Application,
    AppRunner,
    Request,
    Response,
    TCPSite,
    json_response,
)
from loguru import logger
from ormsgpack import packb, unpackb

if TYPE_CHECKING:
    pass


class MessageType(Enum):
    REQUEST_VOTE = auto()
    VOTE_RESPONSE = auto()
    APPEND_ENTRIES = auto()
    APPEND_RESPONSE = auto()
    HEARTBEAT = auto()
    HEARTBEAT_RESPONSE = auto()

    LOCK_ACQUIRE = auto()
    LOCK_RELEASE = auto()
    LOCK_RESPONSE = auto()

    QUEUE_PUBLISH = auto()
    QUEUE_CONSUME = auto()
    QUEUE_ACK = auto()
    QUEUE_RESPONSE = auto()

    CACHE_READ = auto()
    CACHE_WRITE = auto()
    CACHE_INVALIDATE = auto()
    CACHE_RESPONSE = auto()

    BUS_READ = auto()
    BUS_READ_X = auto()
    BUS_UPGRADE = auto()
    BUS_FLUSH = auto()

    PRE_PREPARE = auto()
    PREPARE = auto()
    COMMIT = auto()
    REPLY = auto()

    PING = auto()
    PONG = auto()
    ERROR = auto()


@dataclass(slots=True)
class Message:
    msg_type: MessageType
    sender_id: str
    payload: dict[str, Any]
    term: int = 0
    message_id: str = field(default_factory=lambda: str(uuid4()))
    timestamp: float = field(default_factory=monotonic)

    def serialize(self) -> bytes:
        return packb(
            {
                "msg_type": self.msg_type.value,
                "sender_id": self.sender_id,
                "payload": self.payload,
                "term": self.term,
                "message_id": self.message_id,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def deserialize(cls, data: bytes) -> Message:
        obj = unpackb(data)
        return cls(
            msg_type=MessageType(obj["msg_type"]),
            sender_id=obj["sender_id"],
            payload=obj["payload"],
            term=obj["term"],
            message_id=obj["message_id"],
            timestamp=obj["timestamp"],
        )


MessageHandler = Callable[[Message], Awaitable[Message | None]]


class MessageBus:
    def __init__(
        self,
        node_id: str,
        host: str,
        port: int,
        request_timeout: float = 5.0,
    ) -> None:
        self._node_id = node_id
        self._host = host
        self._port = port
        self._timeout = ClientTimeout(total=request_timeout)
        self._handlers: dict[MessageType, MessageHandler] = {}
        self._session: ClientSession | None = None
        self._app: Application | None = None
        self._runner: AppRunner | None = None
        self._running = False

    def register_handler(
        self,
        msg_type: MessageType,
        handler: MessageHandler,
    ) -> None:
        self._handlers[msg_type] = handler

    async def start(self) -> None:
        self._session = ClientSession(timeout=self._timeout)
        self._app = Application()
        self._app.router.add_post("/message", self._handle_incoming)
        self._app.router.add_get("/health", self._health_check)
        self._runner = AppRunner(self._app)
        await self._runner.setup()
        site = TCPSite(self._runner, self._host, self._port)
        await site.start()
        self._running = True
        logger.info(f"MessageBus started on {self._host}:{self._port}")

    async def stop(self) -> None:
        self._running = False
        if self._session:
            await self._session.close()
        if self._runner:
            await self._runner.cleanup()
        logger.info("MessageBus stopped")

    async def _handle_incoming(
        self,
        request: Request,
    ) -> Response:
        try:
            data = await request.read()
            message = Message.deserialize(data)
            handler = self._handlers.get(message.msg_type)
            if handler:
                response = await handler(message)
                if response:
                    return Response(
                        body=response.serialize(),
                        content_type="application/octet-stream",
                    )
            return Response(status=204)
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            return Response(status=500, text=str(e))

    async def _health_check(
        self,
        _request: Request,
    ) -> Response:
        return json_response({"status": "healthy", "node_id": self._node_id})

    async def send(
        self,
        target_host: str,
        target_port: int,
        message: Message,
    ) -> Message | None:
        if not self._session:
            return None
        url = f"http://{target_host}:{target_port}/message"
        try:
            async with self._session.post(
                url,
                data=message.serialize(),
                headers={"Content-Type": "application/octet-stream"},
            ) as resp:
                if resp.status == 200:
                    data = await resp.read()
                    return Message.deserialize(data)
                return None
        except AsyncTimeoutError:
            logger.warning(f"Timeout sending to {target_host}:{target_port}")
            return None
        except ClientError as e:
            logger.warning(f"Error sending to {target_host}:{target_port}: {e}")
            return None

    async def broadcast(
        self,
        targets: list[tuple[str, int]],
        message: Message,
    ) -> list[Message | None]:
        tasks = [self.send(host, port, message) for host, port in targets]
        return await gather(*tasks)

    @property
    def is_running(self) -> bool:
        return self._running
