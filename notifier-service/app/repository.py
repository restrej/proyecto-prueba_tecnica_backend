"""Repositorio de notificaciones (patrón Repository + interfaz Protocol).

La interfaz :class:`NotificationRepository` permite sustituir MongoDB por una
implementación en memoria en los tests (o por otra base NoSQL en el futuro)
sin tocar la lógica de negocio.
"""

from __future__ import annotations

from typing import Any, Protocol

from pymongo import ASCENDING, DESCENDING
from pymongo.asynchronous.collection import AsyncCollection
from pymongo.errors import DuplicateKeyError

from app.models import Notification


class NotificationRepository(Protocol):
    """Operaciones de persistencia que necesita el servicio."""

    async def add_if_absent(self, notification: Notification) -> bool:
        """Guarda la notificación si no existe. Devuelve True si la creó."""
        ...

    async def list_by_customer(self, customer_id: str, *, limit: int) -> list[Notification]:
        """Devuelve las notificaciones del cliente (más recientes primero)."""
        ...


class MongoNotificationRepository:
    """Implementación sobre MongoDB con el driver asíncrono oficial (PyMongo)."""

    def __init__(self, collection: AsyncCollection[dict[str, Any]]) -> None:
        """Recibe la colección ``notifications``."""
        self._collection = collection

    async def ensure_indexes(self) -> None:
        """Crea los índices necesarios (operación idempotente).

        * ``(customer_id, created_at desc)``: sirve el GET por cliente ya ordenado.
        * ``created_at``: lo usa cleanup-job para borrar las antiguas.
        """
        await self._collection.create_index(
            [("customer_id", ASCENDING), ("created_at", DESCENDING)],
            name="customer_created_at",
        )
        await self._collection.create_index([("created_at", ASCENDING)], name="created_at")

    async def add_if_absent(self, notification: Notification) -> bool:
        """Inserta la notificación; si el ``_id`` ya existe, no hace nada.

        Returns:
            True si se insertó; False si era un duplicado.
        """
        try:
            await self._collection.insert_one(notification.to_document())
        except DuplicateKeyError:
            return False  # mismo event_id ya guardado -> evento duplicado
        return True

    async def list_by_customer(self, customer_id: str, *, limit: int) -> list[Notification]:
        """Consulta las notificaciones del cliente, más recientes primero."""
        cursor = (
            self._collection.find({"customer_id": customer_id})
            .sort("created_at", DESCENDING)
            .limit(limit)
        )
        return [Notification.from_document(doc) async for doc in cursor]
