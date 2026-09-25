"""Esquemas Pydantic (DTOs) de la API HTTP.

Se separan de los modelos ORM a propósito: el contrato público de la API puede
evolucionar independientemente del esquema de la base de datos.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from cafe_common.events import OrderStatus

# customer_id: 1-64 caracteres alfanuméricos, guion o guion bajo.
CUSTOMER_ID_PATTERN = r"^[A-Za-z0-9_-]{1,64}$"


class OrderItemIn(BaseModel):
    """Línea de pedido recibida en ``POST /orders``."""

    # extra="forbid": rechaza campos desconocidos (errores de cliente visibles).
    # str_strip_whitespace: elimina espacios al inicio/fin de los textos.
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    name: str = Field(min_length=1, max_length=100, examples=["latte"])
    qty: int = Field(ge=1, le=100, examples=[1])  # 1..100 unidades por línea

    @field_validator("name")
    @classmethod
    def normalize_name(cls, value: str) -> str:
        """Normaliza el nombre del producto a minúsculas ("Latte" == "latte")."""
        return value.lower()


class CreateOrderRequest(BaseModel):
    """Cuerpo de ``POST /orders``."""

    model_config = ConfigDict(
        extra="forbid",
        str_strip_whitespace=True,
        json_schema_extra={
            "example": {
                "customer_id": "abc123",
                "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
            }
        },
    )

    customer_id: str = Field(pattern=CUSTOMER_ID_PATTERN)
    items: list[OrderItemIn] = Field(min_length=1, max_length=50)  # 1..50 líneas

    def fingerprint(self) -> str:
        """Huella SHA-256 canónica del cuerpo (para validar la idempotencia).

        Se serializa con claves ordenadas y sin espacios, de modo que dos
        cuerpos semánticamente iguales producen el mismo hash aunque el
        cliente cambie el orden de las claves JSON.

        Returns:
            64 caracteres hexadecimales.
        """
        canonical = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class CreateOrderResponse(BaseModel):
    """Respuesta de ``POST /orders`` (lo que exige la prueba)."""

    order_id: UUID
    status: OrderStatus
    created_at: datetime


class OrderItemOut(BaseModel):
    """Línea de pedido en las respuestas."""

    model_config = ConfigDict(from_attributes=True)  # se construye desde el ORM

    name: str
    qty: int


class OrderDetail(BaseModel):
    """Detalle de un pedido (``GET /orders/{order_id}``)."""

    order_id: UUID
    customer_id: str
    status: OrderStatus
    items: list[OrderItemOut]
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None
