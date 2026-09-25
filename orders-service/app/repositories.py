"""Repositorios (patrón Repository) sobre una ``AsyncSession``.

Encapsulan las consultas SQL: la capa de servicio trabaja con objetos de
dominio y no sabe cómo se construyen las queries. Los repositorios NO hacen
commit: la transacción la controla el servicio (patrón *Unit of Work* a
través de ``session.begin()``), de modo que varias operaciones de distintos
repositorios se confirman o se deshacen juntas.
"""

from __future__ import annotations

from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from cafe_common.db.models import IdempotencyKey, Order, OutboxEvent


class OrderRepository:
    """Acceso a la tabla ``orders`` (y a sus líneas vía relación)."""

    def __init__(self, session: AsyncSession) -> None:
        """Recibe la sesión (transacción) en la que operará."""
        self._session = session

    def add(self, order: Order) -> None:
        """Marca el pedido (y sus líneas, por cascada) para ser insertado."""
        self._session.add(order)

    async def get(self, order_id: UUID) -> Order | None:
        """Busca un pedido por clave primaria.

        Args:
            order_id: id del pedido.

        Returns:
            El pedido con sus líneas cargadas, o ``None`` si no existe.
        """
        return await self._session.get(Order, order_id)


class IdempotencyRepository:
    """Acceso a la tabla ``idempotency_keys``."""

    def __init__(self, session: AsyncSession) -> None:
        """Recibe la sesión (transacción) en la que operará."""
        self._session = session

    async def get(self, key: str) -> IdempotencyKey | None:
        """Devuelve el registro de la clave, o ``None`` si es nueva."""
        return await self._session.get(IdempotencyKey, key)

    def add(self, record: IdempotencyKey) -> None:
        """Marca el registro de idempotencia para ser insertado."""
        self._session.add(record)


class OutboxRepository:
    """Acceso a la tabla ``outbox_events`` (solo escritura desde la API)."""

    def __init__(self, session: AsyncSession) -> None:
        """Recibe la sesión (transacción) en la que operará."""
        self._session = session

    def add(self, event: OutboxEvent) -> None:
        """Marca el evento para ser insertado en la misma transacción."""
        self._session.add(event)
