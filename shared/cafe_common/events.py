"""Contrato de los eventos de dominio que viajan por el broker.

Todos los eventos usan el mismo *envelope* (sobre) con metadatos comunes
(``event_id``, ``event_type``, ``trace_id``...) y un ``payload`` específico.
Separar sobre y contenido permite a la infraestructura (consumidor, DLQ, logs)
trabajar con cualquier evento sin conocer su contenido, y versionar payloads
(``event_version``) sin romper consumidores existentes.
"""

from __future__ import annotations

from datetime import datetime  # tipo de las marcas de tiempo
from enum import StrEnum  # enumeración cuyos valores son strings (3.11+)
from typing import Any  # para el payload genérico del envelope
from uuid import UUID, uuid4  # identificadores únicos

from pydantic import BaseModel, ConfigDict, Field  # validación declarativa

from cafe_common.clock import utcnow  # hora UTC por defecto


class Streams:
    """Nombres de los streams (colas) de Redis usados en el sistema.

    Se centralizan como constantes para evitar errores tipográficos: un
    productor y un consumidor que escriban el nombre a mano podrían no
    coincidir y los mensajes se perderían silenciosamente.
    """

    ORDERS_CREATED = "orders.created"  # orders-service -> processor-service
    ORDERS_COMPLETED = "orders.completed"  # processor-service -> notifier-service


def dead_letter_stream(stream: str) -> str:
    """Devuelve el nombre del stream de *dead letters* (DLQ) de ``stream``.

    Los mensajes que no pueden procesarse (malformados o tras agotar
    reintentos) se copian aquí para inspección manual en vez de perderse.

    Args:
        stream: stream original, p. ej. ``orders.created``.

    Returns:
        p. ej. ``orders.created.dlq``.
    """
    return f"{stream}.dlq"


class OrderStatus(StrEnum):
    """Estados posibles de un pedido (máquina de estados simple).

    ``PENDING`` -> ``COMPLETED`` (camino feliz) o ``PENDING`` -> ``FAILED``
    (el procesamiento agotó los reintentos y el mensaje fue a la DLQ).
    """

    PENDING = "PENDING"  # registrado y encolado, pendiente de preparar
    COMPLETED = "COMPLETED"  # preparado por processor-service
    FAILED = "FAILED"  # no se pudo preparar tras todos los reintentos


class EventEnvelope(BaseModel):
    """Sobre común a todos los eventos publicados en el broker.

    Attributes:
        event_id: identificador único y ESTABLE del evento. Si el mismo evento
            se re-publica (entrega at-least-once), conserva el mismo id; los
            consumidores lo usan para detectar duplicados.
        event_type: tipo de evento (coincide con el nombre del stream).
        event_version: versión del esquema del payload.
        occurred_at: cuándo ocurrió el hecho de negocio (UTC).
        producer: servicio que originó el evento.
        trace_id: identificador de traza propagado entre servicios.
        payload: datos específicos del evento (ver ``*Payload``).
    """

    # frozen=True: el envelope es inmutable una vez creado (value object).
    model_config = ConfigDict(frozen=True)

    event_id: UUID = Field(default_factory=uuid4)
    event_type: str
    event_version: int = 1
    occurred_at: datetime = Field(default_factory=utcnow)
    producer: str
    trace_id: str
    payload: dict[str, Any]


class OrderItemPayload(BaseModel):
    """Línea de un pedido dentro de un evento."""

    name: str  # producto, p. ej. "latte"
    qty: int  # cantidad pedida (>0)


class OrderCreatedPayload(BaseModel):
    """Payload del evento ``orders.created``."""

    order_id: UUID
    customer_id: str
    status: OrderStatus
    items: list[OrderItemPayload]
    created_at: datetime


class OrderCompletedPayload(BaseModel):
    """Payload del evento ``orders.completed``.

    Incluye ``customer_id`` e ``items`` para que ``notifier-service`` pueda
    construir la notificación SIN consultar la base de datos de pedidos
    (evita acoplamiento entre servicios: *event-carried state transfer*).
    """

    order_id: UUID
    customer_id: str
    status: OrderStatus
    items: list[OrderItemPayload]
    completed_at: datetime
