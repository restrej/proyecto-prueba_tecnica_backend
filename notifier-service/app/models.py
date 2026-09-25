"""Modelo de dominio de una notificación (también es el DTO de la API)."""

from __future__ import annotations

from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict

from cafe_common.events import OrderItemPayload


class Notification(BaseModel):
    """Notificación enviada a un cliente cuando su pedido se completa.

    Attributes:
        id: identificador; es el ``event_id`` de ``orders.completed`` que la
            originó. Usarlo como ``_id`` en MongoDB hace que un evento
            duplicado NO pueda crear una segunda notificación (idempotencia
            garantizada por el índice único de ``_id``).
        order_id: pedido al que se refiere.
        customer_id: cliente destinatario.
        message: texto legible de la notificación.
        items: productos del pedido.
        trace_id: traza del flujo completo (útil para depurar).
        created_at: cuándo se creó (base de la limpieza de >24 h).
    """

    model_config = ConfigDict(frozen=True)

    id: str
    order_id: str
    customer_id: str
    message: str
    items: list[OrderItemPayload]
    trace_id: str
    created_at: datetime

    def to_document(self) -> dict[str, Any]:
        """Convierte el modelo a documento MongoDB (``id`` -> ``_id``)."""
        document = self.model_dump(mode="python")  # datetime se mantiene como tal
        document["_id"] = document.pop("id")
        return document

    @classmethod
    def from_document(cls, document: dict[str, Any]) -> Notification:
        """Construye el modelo desde un documento MongoDB (``_id`` -> ``id``)."""
        data = dict(document)
        data["id"] = str(data.pop("_id"))
        return cls.model_validate(data)
