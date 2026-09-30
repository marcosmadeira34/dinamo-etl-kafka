import asyncio
import json
import logging
import os
import tempfile
from collections.abc import Awaitable, Callable
from typing import Any

from src.oracle_cloud.queue import QueueServiceInterface

logger = logging.getLogger("dinamo.launcher.app")


class OCIQueueService(QueueServiceInterface):
    """Consome/publica mensagens usando o OCI Queue (OCIQ) no lugar do Kafka.

    Mesmo padrão de auth/config do lion-api (src/oracle_cloud/oci_queue.py):
    credenciais compartilhadas entre BFF e launcher, config-based auth (env
    vars) com fallback para instance principal (OKE). O lion-api implementa
    só o `publish`; o `consume` (que ele deixa como NotImplementedError,
    "use ag-platform-launcher") é implementado aqui.
    """

    def __init__(self, queue_id: str | None = None, endpoint: str | None = None):
        self._client = None
        self._queue_id = queue_id or os.getenv("OCI_QUEUE_ID")
        self._endpoint = endpoint or os.getenv("OCI_QUEUE_SERVICE_ENDPOINT")
        self._temp_key_file = None

    async def connect(self) -> None:
        import oci

        tenancy_id = os.getenv("OCI_TENANCY_ID")
        user_id = os.getenv("OCI_USER_ID")
        fingerprint = os.getenv("OCI_FINGERPRINT")
        key_file = os.getenv("OCI_KEY_FILE")
        region = os.getenv("OCI_REGION")

        if tenancy_id and user_id:
            # Config-based auth (mesmas credenciais/env vars do lion-api)
            key_file_path = key_file
            if key_file and not os.path.isfile(key_file):
                self._temp_key_file = tempfile.NamedTemporaryFile(
                    mode="w", suffix=".pem", delete=False
                )
                self._temp_key_file.write(key_file.replace("\\n", "\n"))
                self._temp_key_file.flush()
                key_file_path = self._temp_key_file.name

            config = {
                "tenancy": tenancy_id,
                "user": user_id,
                "fingerprint": fingerprint,
                "key_file": key_file_path,
                "region": region,
            }
            self._client = oci.queue.QueueClient(
                config=config,
                service_endpoint=self._endpoint,
            )
        else:
            # Instance principal (OKE) - sem credenciais explícitas no deploy
            signer = oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
            self._client = oci.queue.QueueClient(
                config={},
                signer=signer,
                service_endpoint=self._endpoint,
            )

        logger.info(f"Connected to OCI Queue: queue_id={self._queue_id}")

    async def publish(self, routing_key: str, message: dict[str, Any]) -> None:
        await self.publish_to_queue(queue_name=routing_key, message=message)

    async def publish_to_queue(self, queue_name: str, message: dict[str, Any]) -> None:
        import oci

        last_error: Exception | None = None
        for attempt in range(2):
            if not self._client:
                await self.connect()

            try:
                message_body = json.dumps(message)
                put_messages_details = oci.queue.models.PutMessagesDetails(
                    messages=[oci.queue.models.PutMessagesDetailsEntry(content=message_body)]
                )

                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda _d=put_messages_details: self._client.put_messages(
                        queue_id=self._queue_id,
                        put_messages_details=_d,
                    ),
                )

                entries = response.data.messages if response.data else []
                if entries and entries[0].id:
                    logger.debug(
                        f"Published message to OCI Queue {self._queue_id} (msg_id={entries[0].id})"
                    )
                else:
                    logger.debug(f"Published message to OCI Queue {self._queue_id}")
                return

            except Exception as e:
                last_error = e
                logger.warning(
                    f"Failed to publish to OCI Queue (attempt {attempt + 1}/2): {e} — reconnecting"
                )
                self._client = None

        raise last_error

    async def consume(
        self,
        queue_name: str,
        callback: Callable[[dict[str, Any]], Awaitable[None]],
        routing_key: str | None = None,
        visibility_in_seconds: int | None = None,
        polling_timeout_in_seconds: int = 20,
        max_messages_per_poll: int = 10,
    ) -> None:
        channel_filter = routing_key or queue_name
        logger.info(f"Consuming OCI Queue {self._queue_id} (channel={channel_filter})")

        loop = asyncio.get_event_loop()

        while True:
            if not self._client:
                await self.connect()

            try:
                response = await loop.run_in_executor(
                    None,
                    lambda: self._client.get_messages(
                        queue_id=self._queue_id,
                        channel_filter=channel_filter,
                        visibility_in_seconds=visibility_in_seconds,
                        timeout_in_seconds=polling_timeout_in_seconds,
                        limit=max_messages_per_poll,
                    ),
                )
            except Exception as e:
                logger.warning(f"Failed to poll OCI Queue: {e} — reconnecting")
                self._client = None
                continue

            messages = response.data.messages if response.data else []
            for oci_message in messages:
                try:
                    payload = json.loads(oci_message.content)
                    await callback(payload)
                    await loop.run_in_executor(
                        None,
                        lambda _receipt=oci_message.receipt: self._client.delete_message(
                            queue_id=self._queue_id, message_receipt=_receipt
                        ),
                    )
                except Exception:
                    logger.exception(
                        f"Falha ao processar mensagem {oci_message.id} do OCIQ; "
                        "não confirmando (delete) para permitir reprocessamento/DLQ."
                    )

    async def close(self) -> None:
        self._client = None
        if self._temp_key_file:
            try:
                os.unlink(self._temp_key_file.name)
            except OSError:
                pass
            self._temp_key_file = None
        logger.info("Closed OCI Queue connection")