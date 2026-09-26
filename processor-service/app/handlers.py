"""Lógica de negocio del procesador: manejo del evento ``orders.created``.

Garantías frente a entrega *at-least-once* (duplicados y re-entregas):

1. **Inbox / processed_events**: si el ``event_id`` ya se procesó, se ignora.
2. **Bloqueo de fila** (``SELECT ... FOR UPDATE``): dos réplicas que reciban el
   mismo pedido a la vez se serializan.
3. **Comprobación de estado**: un pedido ya ``COMPLETED`` no se vuelve a
   completar ni emite un segundo ``orders.completed``.
4. **Outbox**: la actualización del pedido, la marca de "procesado" y el evento
   ``orders.completed`` se confirman en UNA sola transacción.
"""

from __future__ import annotations

import asyncio
import logging
import random
from collections.abc import Awaitable, Callable
from uuid import UUID

from pydantic import ValidationError

from app.repositories import OrderRepository, ProcessedEventRepository
from cafe_common.clock import Clock, utcnow
from cafe_common.db.outbox import build_outbox_event
from cafe_common.db.session import SessionFactory
from cafe_common.events import (
    EventEnvelope,
    OrderCompletedPayload,
    OrderCreatedPayload,
    OrderItemPayload,
    OrderStatus,
    Streams,
)
from cafe_common.retry import PermanentError

logger = logging.getLogger(__name__)


class TransientProcessingError(Exception):
    """Fallo transitorio (simulado) durante la preparación: se reintenta."""


class OrderProcessor:
    """Procesa pedidos recibidos por el evento ``orders.created``."""

    CONSUMER_NAME = "processor-service"  # identidad en processed_events

    def __init__(
        self,
        session_factory: SessionFactory,
        *,
        min_seconds: float = 2.0,
        max_seconds: float = 5.0,
        failure_rate: float = 0.0,
        clock: Clock = utcnow,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        rng: random.Random | None = None,
    ) -> None:
        """Crea el procesador.

        Args:
            session_factory: fábrica de sesiones SQL.
            min_seconds: duración mínima simulada de la preparación.
            max_seconds: duración máxima simulada.
            failure_rate: probabilidad de fallo transitorio simulado (0..1).
            clock: reloj inyectable.
            sleep: función de espera inyectable (tests sin esperas).
            rng: generador aleatorio inyectable (tests deterministas).
        """
        self._session_factory = session_factory
        self._min_seconds = min_seconds
        self._max_seconds = max_seconds
        self._failure_rate = failure_rate
        self._clock = clock
        self._sleep = sleep
        self._rng = rng or random.Random()

    async def handle(self, envelope: EventEnvelope) -> None:
        """Procesa un evento ``orders.created`` de forma idempotente.

        Args:
            envelope: evento recibido del broker.

        Raises:
            PermanentError: payload inválido o pedido inexistente (-> DLQ).
            TransientProcessingError: fallo simulado (-> reintento con backoff).
        """
        try:
            payload = OrderCreatedPayload.model_validate(envelope.payload)
        except ValidationError as exc:
            raise PermanentError(f"payload de orders.created no válido: {exc}") from exc

        # Comprobación rápida (sin bloqueo) para no "preparar" dos veces un
        # pedido cuyo evento ya se procesó. La comprobación definitiva se
        # repite abajo dentro de la transacción con la fila bloqueada.
        if await self._already_processed(envelope.event_id):
            logger.info("duplicate_event_ignored", extra={"event_id": str(envelope.event_id)})
            return

        await self._prepare(payload)  # FUERA de la transacción: no retiene locks
        await self._complete(envelope, payload)

    async def _already_processed(self, event_id: UUID) -> bool:
        """Consulta la tabla inbox en una sesión de solo lectura."""
        async with self._session_factory() as session:
            return await ProcessedEventRepository(session, self.CONSUMER_NAME).exists(event_id)

    async def _prepare(self, payload: OrderCreatedPayload) -> None:
        """Simula la preparación del café (2-5 s por defecto).

        Se usa ``asyncio.sleep`` (equivalente no bloqueante a ``time.sleep``)
        para que el proceso siga atendiendo /health y otros mensajes.
        """
        duration = self._rng.uniform(self._min_seconds, self._max_seconds)
        logger.info(
            "order_preparation_started",
            extra={"order_id": str(payload.order_id), "duration_seconds": round(duration, 2)},
        )
        await self._sleep(duration)
        if self._rng.random() < self._failure_rate:
            raise TransientProcessingError("fallo simulado del barista")

    async def _complete(self, envelope: EventEnvelope, payload: OrderCreatedPayload) -> None:
        """Transacción: marca COMPLETED + inbox + outbox ``orders.completed``."""
        async with self._session_factory() as session, session.begin():
            inbox = ProcessedEventRepository(session, self.CONSUMER_NAME)
            order = await OrderRepository(session).get_for_update(payload.order_id)
            if order is None:
                # Con el outbox el pedido siempre existe antes que su evento;
                # si no está, el mensaje es incoherente: reintentar no ayuda.
                raise PermanentError(f"el pedido {payload.order_id} no existe")
            if await inbox.exists(envelope.event_id):
                logger.info("duplicate_event_ignored", extra={"order_id": str(order.id)})
                return
            inbox.add(envelope.event_id)  # marca de "procesado" (misma transacción)
            if order.status == OrderStatus.COMPLETED.value:
                logger.info("order_already_completed", extra={"order_id": str(order.id)})
                return

            now = self._clock()
            order.status = OrderStatus.COMPLETED.value
            order.completed_at = now
            order.updated_at = now
            session.add(
                build_outbox_event(
                    stream=Streams.ORDERS_COMPLETED,
                    aggregate_id=order.id,
                    payload=OrderCompletedPayload(
                        order_id=order.id,
                        customer_id=order.customer_id,
                        status=OrderStatus.COMPLETED,
                        items=[OrderItemPayload(name=i.name, qty=i.qty) for i in order.items],
                        completed_at=now,
                    ),
                    trace_id=envelope.trace_id,  # la traza continúa
                    created_at=now,
                )
            )
        logger.info("order_completed", extra={"order_id": str(payload.order_id)})

    async def mark_failed(self, envelope: EventEnvelope, error: BaseException) -> None:
        """Hook de DLQ: marca el pedido como ``FAILED`` si sigue ``PENDING``.

        Así el estado en BD refleja que el pedido no se pudo preparar (en vez
        de quedarse ``PENDING`` para siempre).

        Args:
            envelope: evento que acabó en la DLQ.
            error: causa del fallo.
        """
        order_id = envelope.payload.get("order_id")
        if order_id is None:
            return  # payload inválido: no hay pedido que marcar
        async with self._session_factory() as session, session.begin():
            order = await OrderRepository(session).get_for_update(UUID(str(order_id)))
            if order is not None and order.status == OrderStatus.PENDING.value:
                order.status = OrderStatus.FAILED.value
                order.updated_at = self._clock()
                logger.error(
                    "order_marked_failed", extra={"order_id": str(order_id), "error": repr(error)}
                )
