"""Patrón Transactional Outbox.

Problema (*dual write*): guardar el pedido en la BD y publicar el evento en el
broker son dos operaciones sobre sistemas distintos; si el proceso cae entre
ambas, o el broker está caído, quedaría un pedido sin evento (o un evento sin
pedido). No existe una transacción distribuida simple entre PostgreSQL y Redis.

Solución:

1. En la MISMA transacción SQL del cambio de negocio se inserta una fila en
   ``outbox_events`` (:func:`build_outbox_event`). O se guardan ambas cosas o
   ninguna (atomicidad garantizada por PostgreSQL).
2. Un proceso en segundo plano (:class:`OutboxRelay`) lee las filas sin
   publicar, las publica en el broker y marca ``published_at``.

Si el relay publica y cae antes de marcar la fila, la volverá a publicar
(duplicado con el MISMO ``event_id``): por eso los consumidores son
idempotentes. Resultado: entrega *at-least-once* y consistencia eventual.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import uuid
from collections.abc import Sequence
from datetime import datetime

from pydantic import BaseModel
from sqlalchemy import select

from cafe_common.clock import Clock, utcnow
from cafe_common.db.models import OutboxEvent
from cafe_common.db.session import SessionFactory
from cafe_common.events import EventEnvelope
from cafe_common.messaging import EventPublisher

logger = logging.getLogger(__name__)


def build_outbox_event(
    *,
    stream: str,
    aggregate_id: uuid.UUID,
    payload: BaseModel,
    trace_id: str,
    created_at: datetime | None = None,
) -> OutboxEvent:
    """Crea (sin guardar) la fila del outbox para un evento de dominio.

    El llamador debe añadirla a la sesión dentro de su transacción.

    Args:
        stream: stream destino (y tipo de evento).
        aggregate_id: id de la entidad afectada (el pedido).
        payload: modelo Pydantic con los datos del evento.
        trace_id: traza a propagar.
        created_at: marca de tiempo opcional (por defecto, ahora).

    Returns:
        Instancia ``OutboxEvent`` lista para ``session.add``.
    """
    event = OutboxEvent(
        id=uuid.uuid4(),  # será el event_id estable del envelope
        aggregate_id=aggregate_id,
        event_type=stream,
        stream=stream,
        payload=payload.model_dump(mode="json"),  # UUID/datetime -> str JSON
        trace_id=trace_id,
    )
    if created_at is not None:
        event.created_at = created_at
    return event


class OutboxRelay:
    """Publica en el broker los eventos pendientes del outbox (*polling publisher*).

    Cada servicio solo publica los streams que le pertenecen (``streams``), de
    modo que la responsabilidad de cada evento es clara.
    """

    def __init__(
        self,
        session_factory: SessionFactory,
        publisher: EventPublisher,
        *,
        streams: Sequence[str],
        producer: str,
        batch_size: int = 100,
        poll_interval: float = 0.5,
        clock: Clock = utcnow,
    ) -> None:
        """Crea el relay.

        Args:
            session_factory: fábrica de sesiones SQL.
            publisher: publicador del broker.
            streams: streams cuyos eventos publica este relay.
            producer: nombre del servicio (campo ``producer`` del envelope).
            batch_size: filas máximas por iteración.
            poll_interval: segundos de espera cuando no hay nada pendiente.
            clock: reloj inyectable.
        """
        self._session_factory = session_factory
        self._publisher = publisher
        self._streams = list(streams)
        self._producer = producer
        self._batch_size = batch_size
        self._poll_interval = poll_interval
        self._clock = clock

    async def publish_pending(self) -> int:
        """Publica un lote de eventos pendientes.

        Usa ``SELECT ... FOR UPDATE SKIP LOCKED``: si hay varias réplicas del
        servicio, cada una bloquea filas distintas y no se pisan entre sí.

        Returns:
            Número de eventos publicados en esta iteración.
        """
        published = 0
        async with self._session_factory() as session, session.begin():
            query = (
                select(OutboxEvent)
                .where(
                    OutboxEvent.published_at.is_(None),  # solo pendientes
                    OutboxEvent.stream.in_(self._streams),  # solo los míos
                )
                .order_by(OutboxEvent.created_at)  # orden de creación (FIFO)
                .limit(self._batch_size)
                .with_for_update(skip_locked=True)
            )
            rows = (await session.scalars(query)).all()
            for row in rows:
                envelope = EventEnvelope(
                    event_id=row.id,  # id estable -> deduplicación aguas abajo
                    event_type=row.event_type,
                    occurred_at=row.created_at,
                    producer=self._producer,
                    trace_id=row.trace_id,
                    payload=row.payload,
                )
                try:
                    await self._publisher.publish(row.stream, envelope)
                except Exception:
                    # Broker caído: se deja de publicar este lote. Las filas ya
                    # publicadas se marcan (commit al salir del with) y el resto
                    # se reintentará en la siguiente iteración.
                    logger.exception("outbox_publish_failed", extra={"event_id": str(row.id)})
                    break
                row.published_at = self._clock()  # marcado como publicado
                published += 1
        return published

    async def run(self, stop_event: asyncio.Event) -> None:
        """Bucle del relay hasta que se active ``stop_event``.

        Si un lote viene lleno, se vuelve a consultar de inmediato (hay
        backlog); si no, se espera ``poll_interval``.

        Args:
            stop_event: evento de parada ordenada.
        """
        logger.info("outbox_relay_started", extra={"streams": self._streams})
        while not stop_event.is_set():
            try:
                count = await self.publish_pending()
            except Exception:
                logger.exception("outbox_relay_error")  # p. ej. BD caída
                count = 0
            if count >= self._batch_size:
                continue  # hay más pendientes: no esperar
            # Espera poll_interval o hasta que se pida la parada (lo que ocurra antes).
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval)
        logger.info("outbox_relay_stopped")
