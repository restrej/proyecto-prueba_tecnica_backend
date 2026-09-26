"""Rutas HTTP de pedidos y traducción de errores de dominio a HTTP."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, FastAPI, Header, HTTPException, Request, Response, status
from fastapi.responses import JSONResponse

from app.api.dependencies import get_order_service
from app.errors import IdempotencyKeyReuseError, OrderNotFoundError
from app.schemas import CreateOrderRequest, CreateOrderResponse, OrderDetail
from app.services import OrderService
from cafe_common.security import verify_api_key
from cafe_common.tracing import get_trace_id, new_trace_id

# Todas las rutas del router exigen API key (si está configurada).
router = APIRouter(prefix="/orders", tags=["orders"], dependencies=[Depends(verify_api_key)])


@router.post(
    "",
    status_code=status.HTTP_201_CREATED,
    response_model=CreateOrderResponse,
    responses={
        200: {"description": "Repetición idempotente: el pedido ya se creó con esta clave"},
        400: {"description": "Falta la cabecera Idempotency-Key"},
        422: {"description": "Error de validación o Idempotency-Key reutilizada con otro cuerpo"},
    },
)
async def create_order(
    payload: CreateOrderRequest,
    response: Response,
    service: Annotated[OrderService, Depends(get_order_service)],
    idempotency_key: Annotated[
        str | None, Header(alias="Idempotency-Key", min_length=1, max_length=255)
    ] = None,
) -> CreateOrderResponse:
    """Registra un pedido y lo encola para su preparación.

    * ``201 Created``: pedido nuevo.
    * ``200 OK`` + ``Idempotent-Replayed: true``: repetición con la misma clave.

    Args:
        payload: cuerpo JSON validado automáticamente por FastAPI/Pydantic.
        response: respuesta mutable (para cambiar status y cabeceras).
        service: caso de uso inyectado.
        idempotency_key: cabecera ``Idempotency-Key`` (obligatoria).

    Returns:
        ``order_id``, ``status`` y ``created_at``.
    """
    if idempotency_key is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="La cabecera Idempotency-Key es obligatoria",
        )
    result = await service.create_order(
        payload,
        idempotency_key=idempotency_key,
        trace_id=get_trace_id() or new_trace_id(),  # trace_id del middleware
    )
    if result.replayed:
        response.status_code = status.HTTP_200_OK
        response.headers["Idempotent-Replayed"] = "true"
    return result.response


@router.get("/{order_id}", response_model=OrderDetail)
async def get_order(
    order_id: UUID,
    service: Annotated[OrderService, Depends(get_order_service)],
) -> OrderDetail:
    """Consulta un pedido (útil para ver cómo pasa de PENDING a COMPLETED)."""
    return await service.get_order(order_id)


def register_exception_handlers(app: FastAPI) -> None:
    """Traduce las excepciones de dominio a respuestas HTTP coherentes.

    Args:
        app: aplicación donde se registran los manejadores.
    """

    @app.exception_handler(IdempotencyKeyReuseError)
    async def _idempotency_reuse(_: Request, exc: IdempotencyKeyReuseError) -> JSONResponse:
        """422: misma clave con cuerpo distinto (draft IETF Idempotency-Key)."""
        return JSONResponse(status_code=422, content={"detail": str(exc)})

    @app.exception_handler(OrderNotFoundError)
    async def _not_found(_: Request, exc: OrderNotFoundError) -> JSONResponse:
        """404: el pedido no existe."""
        return JSONResponse(status_code=404, content={"detail": str(exc)})
