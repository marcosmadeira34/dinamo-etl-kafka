from collections.abc import Awaitable, Callable
from typing import Any

from src.oracle_cloud.queue import QueueServiceInterface


class QueueService:
    def __init__(self, queue_interface: QueueServiceInterface):
        self.queue = queue_interface

    async def publish(self, routing_key: str, message: dict[str, Any]) -> None:
        await self.queue.publish(routing_key=routing_key, message=message)

    async def consume(
        self,
        queue_name: str,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
        routing_key: str | None = None,
    ) -> None:
        await self.queue.consume(queue_name=queue_name, callback=callback, routing_key=routing_key)