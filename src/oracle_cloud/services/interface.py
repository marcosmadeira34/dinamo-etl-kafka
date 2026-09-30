from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from typing import Any


class QueueServiceInterface(ABC):
    """Contrato que qualquer provedor de fila (OCI Queue, Kafka, RabbitMQ, ...)
    precisa implementar para ser usado pelo QueueService.

    Mesmo contrato usado no lion-api (src/oracle_cloud/queue.py), para que o
    launcher e o BFF falem a mesma "língua" de fila.
    """

    @abstractmethod
    async def publish(self, routing_key: str, message: dict[str, Any]) -> None:
        """Publica em `routing_key` (mapeado para o channel_id da fila OCI)."""
        raise NotImplementedError

    @abstractmethod
    async def publish_to_queue(self, queue_name: str, message: dict[str, Any]) -> None:
        """Publica em uma fila/canal explicitamente nomeado."""
        raise NotImplementedError

    @abstractmethod
    async def consume(
        self,
        queue_name: str,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
        routing_key: str | None = None,
    ) -> None:
        """Consome mensagens continuamente, chamando `callback` para cada uma.

        Args:
            queue_name: identifica de onde consumir. Na implementação OCI,
                é usado como channel_filter quando `routing_key` não é informado.
            callback: função assíncrona chamada com o payload (dict) de cada
                mensagem recebida. Só há confirmação (delete) da mensagem se o
                callback não lançar exceção; se lançar, a mensagem some da
                visibilidade até o timeout e volta a ficar disponível
                (retry) — depois de N tentativas, o próprio OCIQ manda para a DLQ.
            routing_key: quando informado, tem prioridade sobre `queue_name`
                para filtrar o canal de origem das mensagens.
        """
        raise NotImplementedError

    @abstractmethod
    async def close(self) -> None:
        """Encerra a conexão com o provedor de fila."""
        raise NotImplementedError
