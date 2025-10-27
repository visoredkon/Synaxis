from collections.abc import Awaitable, Callable

import ormsgpack
from aiohttp import ClientSession, ClientTimeout, web
from loguru import logger

from ..consensuses import ConsensusMessage


class MessagePassing:
    node_id: str
    host: str
    port: int
    app: web.Application
    runner: web.AppRunner | None
    site: web.TCPSite | None
    session: ClientSession | None
    message_handlers: dict[
        str, Callable[[dict[str, object]], Awaitable[dict[str, object] | None]]
    ]

    def __init__(self, node_id: str, host: str, port: int) -> None:
        self.node_id = node_id
        self.host = host
        self.port = port
        self.app = web.Application()
        self.runner = None
        self.site = None
        self.session = None
        self.message_handlers = {}

    async def start(self) -> None:
        self.session = ClientSession()
        self.app.router.add_post("/message", self._handle_message)
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        self.site = web.TCPSite(self.runner, self.host, self.port)
        await self.site.start()
        logger.info(f"Message passing started on {self.host}:{self.port}")

    async def stop(self) -> None:
        if self.session:
            await self.session.close()
        if self.site:
            await self.site.stop()
        if self.runner:
            await self.runner.cleanup()
        logger.info("Message passing stopped")

    def register_handler(
        self,
        message_type: str,
        handler: Callable[[dict[str, object]], Awaitable[dict[str, object] | None]],
    ) -> None:
        self.message_handlers[message_type] = handler

    async def send_message(
        self, target_node: str, target_port: int, message: dict[str, object]
    ) -> dict[str, object] | None:
        if not self.session:
            logger.error("Session not initialized")
            return None
        url = f"http://{target_node}:{target_port}/message"
        try:
            data = ormsgpack.packb(message)
            timeout = ClientTimeout(total=5)
            async with self.session.post(url, data=data, timeout=timeout) as response:
                if response.status == 200:
                    response_data = await response.read()
                    return ormsgpack.unpackb(response_data)
                logger.warning(
                    f"Failed to send message to {target_node}: {response.status}"
                )
        except Exception as e:
            logger.debug(f"Error sending message to {target_node}: {e}")
        return None

    async def _handle_message(self, request: web.Request) -> web.Response:
        try:
            data = await request.read()
            message = ormsgpack.unpackb(data)
            message_type = message.get("type", "unknown")
            if message_type in self.message_handlers:
                handler = self.message_handlers[message_type]
                response = await handler(message)
                if response:
                    return web.Response(
                        body=ormsgpack.packb(response),
                        content_type="application/msgpack",
                    )
            return web.Response(status=200)
        except Exception as e:
            logger.error(f"Error handling message: {e}")
            return web.Response(status=500)


class ConsensusMessagePassing:
    message_passing: MessagePassing
    node_addresses: dict[str, tuple[str, int]]

    def __init__(
        self,
        message_passing: MessagePassing,
        node_addresses: dict[str, tuple[str, int]],
    ) -> None:
        self.message_passing = message_passing
        self.node_addresses = node_addresses

    async def send_consensus_message(
        self, target_node: str, message: ConsensusMessage
    ) -> ConsensusMessage | None:
        if target_node not in self.node_addresses:
            logger.warning(f"Unknown target node: {target_node}")
            return None
        host, port = self.node_addresses[target_node]
        message_dict = {"type": "consensus", "payload": message.to_dict()}
        response = await self.message_passing.send_message(host, port, message_dict)
        if response and "payload" in response:
            payload = response["payload"]
            if isinstance(payload, dict):
                return ConsensusMessage.from_dict(payload)
        return None


__all__ = ["MessagePassing", "ConsensusMessagePassing"]
