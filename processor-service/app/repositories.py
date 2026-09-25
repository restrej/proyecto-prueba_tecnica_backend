"""Repositorios de processor-service sobre una ``AsyncSession``."""

from __future__ import annotations

from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from cafe_common.db.models import Order, ProcessedEvent


class OrderRepository:
    """Acceso de lectura/escritura a ``orders`` desde el procesador."""

    def __init__(self, session: AsyncSession) -> None:
        """Recibe la sesión (transacción) en la que operará."""
        self._session = session

    async def get_for_update(self, order_id: UUID) -> Order | None:
        """Obtiene el pedido BLOQUEANDO su fila hasta el fin de la transacción.

        ``SELECT ... FOR UPDATE`` serializa el procesamiento concurrente del
        mismo pedido (p. ej. un duplicado entregado a otra réplica a la vez):
        la segunda transacción espera a que la primera termine y entonces ve
        el pedido ya ``COMPLETED``.

        Args:
            order_id: id del pedido.

        Returns:
            El pedido o ``None`` si no existe.
        """
        query = select(Order).where(Order.id == order_id).with_for_update()
        return (await self._session.scalars(query)).one_or_none()


class ProcessedEventRepository:
    """Tabla ``processed_events`` (patrón Idempotent Consumer / Inbox)."""

    def __init__(self, session: AsyncSession, consumer: str) -> None:
        """Crea el repositorio para un consumidor concreto.

        Args:
            session: sesión/transacción actual.
            consumer: nombre del consumidor (parte de la clave primaria).
        """
        self._session = session
        self._consumer = consumer

    async def exists(self, event_id: UUID) -> bool:
        """Indica si este consumidor ya procesó ``event_id``."""
        found = await self._session.get(ProcessedEvent, (event_id, self._consumer))
        return found is not None

    def add(self, event_id: UUID) -> None:
        """Registra ``event_id`` como procesado (en la transacción actual)."""
        self._session.add(ProcessedEvent(event_id=event_id, consumer=self._consumer))
