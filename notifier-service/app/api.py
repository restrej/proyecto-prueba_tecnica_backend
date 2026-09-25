"""Rutas HTTP de notifier-service."""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, Path, Query, Request

from app.models import Notification
from app.repository import NotificationRepository
from cafe_common.security import verify_api_key

router = APIRouter(tags=["notifications"], dependencies=[Depends(verify_api_key)])


def get_repository(request: Request) -> NotificationRepository:
    """Dependencia: repositorio creado en el lifespan (o inyectado en tests)."""
    return request.app.state.notification_repository  # type: ignore[no-any-return]


@router.get("/notifications/{customer_id}", response_model=list[Notification])
async def list_notifications(
    customer_id: Annotated[str, Path(pattern=r"^[A-Za-z0-9_-]{1,64}$")],
    repository: Annotated[NotificationRepository, Depends(get_repository)],
    limit: Annotated[int, Query(ge=1, le=1000)] = 100,
) -> list[Notification]:
    """Devuelve las notificaciones de un cliente (más recientes primero).

    Una lista vacía (200) significa que el cliente aún no tiene
    notificaciones; no es un error.

    Args:
        customer_id: cliente a consultar.
        repository: repositorio inyectado.
        limit: máximo de resultados (paginación simple).
    """
    return await repository.list_by_customer(customer_id, limit=limit)
