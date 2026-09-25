"""Manejo del evento ``orders.completed``: crea la notificación del cliente."""

from __future__ import annotations

import logging

from pydantic import ValidationError

from app.models import Notification
from app.repository import NotificationRepository
from cafe_common.clock import Clock, utcnow
from cafe_common.events import EventEnvelope, OrderCompletedPayload
from cafe_common.metrics import EVENTS_CONSUMED_TOTAL
from cafe_common.retry import PermanentError

logger = logging.getLogger(__name__)


class NotificationHandler:
    """Convierte eventos ``orders.completed`` en notificaciones persistidas."""

    def __init__(self, repository: NotificationRepository, *, clock: Clock = utcnow) -> None:
        """Crea el handler.

        Args:
            repository: dónde guardar las notificaciones.
            clock: reloj inyectable.
        """
        self._repository = repository
        self._clock = clock

    async def handle(self, envelope: EventEnvelope) -> None:
        """Crea la notificación de forma idempotente.

        Args:
            envelope: evento ``orders.completed``.

        Raises:
            PermanentError: si el payload no cumple el contrato (-> DLQ).
        """
        try:
            payload = OrderCompletedPayload.model_validate(envelope.payload)
        except ValidationError as exc:
            raise PermanentError(f"invalid orders.completed payload: {exc}") from exc

        summary = ", ".join(f"{item.qty}x {item.name}" for item in payload.items)
        notification = Notification(
            id=str(envelope.event_id),  # clave de idempotencia
            order_id=str(payload.order_id),
            customer_id=payload.customer_id,
            message=f"Your order {payload.order_id} is ready: {summary}. Enjoy! ☕",
            items=payload.items,
            trace_id=envelope.trace_id,
            created_at=self._clock(),
        )
        created = await self._repository.add_if_absent(notification)
        if created:
            logger.info(
                "notification_created",
                extra={"order_id": notification.order_id, "customer_id": payload.customer_id},
            )
        else:
            EVENTS_CONSUMED_TOTAL.labels(stream=envelope.event_type, result="duplicate").inc()
            logger.info("duplicate_event_ignored", extra={"event_id": notification.id})
