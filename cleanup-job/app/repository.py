"""Acceso a la colección de notificaciones para la limpieza."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Protocol

from pymongo.asynchronous.collection import AsyncCollection


class CleanupRepository(Protocol):
    """Operación de borrado que necesita el servicio de limpieza."""

    async def delete_older_than(self, cutoff: datetime) -> int:
        """Borra las notificaciones con ``created_at < cutoff``; devuelve cuántas."""
        ...


class MongoCleanupRepository:
    """Implementación sobre MongoDB."""

    def __init__(self, collection: AsyncCollection[dict[str, Any]]) -> None:
        """Recibe la colección ``notifications``."""
        self._collection = collection

    async def delete_older_than(self, cutoff: datetime) -> int:
        """Ejecuta ``deleteMany({created_at: {$lt: cutoff}})``.

        Usa el índice ``created_at`` creado por notifier-service.
        """
        result = await self._collection.delete_many({"created_at": {"$lt": cutoff}})
        return int(result.deleted_count)
