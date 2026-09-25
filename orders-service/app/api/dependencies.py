"""Dependencias FastAPI (inyección de dependencias).

Los endpoints no construyen sus colaboradores: los piden con ``Depends``. En
los tests basta con cambiar ``app.state.session_factory`` (o usar
``app.dependency_overrides``) para trabajar contra otra base de datos.
"""

from __future__ import annotations

from fastapi import Request

from app.services import OrderService


def get_order_service(request: Request) -> OrderService:
    """Construye el ``OrderService`` con la fábrica de sesiones de la app.

    Args:
        request: petición actual (da acceso a ``app.state``).

    Returns:
        Instancia del servicio para esta petición (objeto ligero y sin estado).
    """
    return OrderService(request.app.state.session_factory)
