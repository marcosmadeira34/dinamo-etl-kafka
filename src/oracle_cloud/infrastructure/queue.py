from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any


class QueueServiceInterface(ABC):

    @abstractmethod
    async def publish(self, routing_key: str, message: dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def publish_to_queue(self, queue_name: str, message: dict[str, Any]) -> None:
        pass

    @abstractmethod
    async def consume(
        self,
        queue_name: str,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
        routing_key: str | None = None,
    ) -> None:
        pass

    @abstractmethod
    async def close(self) -> None:
        pass


_queue_service: QueueServiceInterface | None = None


async def get_queue_service() -> QueueServiceInterface:
    global _queue_service
    if _queue_service is None:
        from src.oracle_cloud.infrastructure.oci_queue import OciQueueService

        _queue_service = OciQueueService()
        await _queue_service.connect()
    return _queue_service


async def close_queue_service() -> None:
    global _queue_service
    if _queue_service is not None:
        await _queue_service.close()
        _queue_service = None
