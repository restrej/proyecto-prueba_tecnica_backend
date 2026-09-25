"""Dobles de prueba (fakes) para notifier-service."""

from __future__ import annotations

from app.models import Notification


class InMemoryNotificationRepository:
    """Implementación en memoria de ``NotificationRepository``.

    Reproduce la semántica relevante de MongoDB: ``_id`` único.
    """

    def __init__(self) -> None:
        """Inicializa el almacén vacío (id -> notificación)."""
        self.items: dict[str, Notification] = {}

    async def add_if_absent(self, notification: Notification) -> bool:
        """Guarda si el id no existe; False si es duplicado."""
        if notification.id in self.items:
            return False
        self.items[notification.id] = notification
        return True

    async def list_by_customer(self, customer_id: str, *, limit: int) -> list[Notification]:
        """Filtra por cliente y ordena por fecha descendente."""
        found = [n for n in self.items.values() if n.customer_id == customer_id]
        return sorted(found, key=lambda n: n.created_at, reverse=True)[:limit]
