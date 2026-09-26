"""Errores de dominio de orders-service.

Son independientes de HTTP; la capa ``api`` los traduce a códigos de estado.
Así la lógica de negocio puede reutilizarse desde otros adaptadores (CLI,
consumidores de eventos...) sin arrastrar FastAPI.
"""

from __future__ import annotations

from uuid import UUID


class OrdersError(Exception):
    """Clase base de los errores de dominio del servicio."""


class IdempotencyKeyReuseError(OrdersError):
    """Se reutilizó una ``Idempotency-Key`` con un cuerpo DISTINTO al original."""

    def __init__(self, key: str) -> None:
        """Guarda la clave conflictiva para el mensaje de error."""
        super().__init__(f"La Idempotency-Key '{key}' ya se usó con un cuerpo de petición distinto")
        self.key = key


class OrderNotFoundError(OrdersError):
    """No existe un pedido con el id solicitado."""

    def __init__(self, order_id: UUID) -> None:
        """Guarda el id buscado para el mensaje de error."""
        super().__init__(f"No existe el pedido '{order_id}'")
        self.order_id = order_id
