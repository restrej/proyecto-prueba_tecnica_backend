"""Tests de la API de pedidos contra una base de datos SQLite real.

Verifican el contrato HTTP, la idempotencia y el Transactional Outbox.
"""

from __future__ import annotations

import asyncio
import uuid

from httpx import AsyncClient
from sqlalchemy import func, select

from cafe_common.db.models import IdempotencyKey, Order, OutboxEvent
from cafe_common.db.session import SessionFactory

BODY = {
    "customer_id": "abc123",
    "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
}


async def _count(session_factory: SessionFactory, model: type) -> int:
    """Cuenta las filas de la tabla de ``model``."""
    async with session_factory() as session:
        return int(await session.scalar(select(func.count()).select_from(model)) or 0)


async def test_create_order_persists_order_and_outbox_event(
    client: AsyncClient, session_factory: SessionFactory
) -> None:
    """201 + pedido PENDING + evento orders.created en el outbox (misma transacción)."""
    response = await client.post("/orders", json=BODY, headers={"Idempotency-Key": "k-1"})

    assert response.status_code == 201
    data = response.json()
    assert data["status"] == "PENDING"
    assert set(data) == {"order_id", "status", "created_at"}
    assert response.headers["X-Trace-Id"]  # el middleware asigna trace_id

    async with session_factory() as session:
        event = (await session.scalars(select(OutboxEvent))).one()
    assert event.stream == "orders.created"
    assert event.published_at is None  # pendiente de que lo publique el relay
    assert event.payload["order_id"] == data["order_id"]
    assert event.payload["items"] == BODY["items"]
    assert event.trace_id == response.headers["X-Trace-Id"]  # traza propagada


async def test_same_idempotency_key_returns_same_order(
    client: AsyncClient, session_factory: SessionFactory
) -> None:
    """Reintentar con la misma clave NO duplica: misma respuesta y 200."""
    headers = {"Idempotency-Key": "k-retry"}
    first = await client.post("/orders", json=BODY, headers=headers)
    second = await client.post("/orders", json=BODY, headers=headers)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.headers["Idempotent-Replayed"] == "true"
    assert second.json() == first.json()
    assert await _count(session_factory, Order) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_concurrent_requests_with_same_key_create_one_order(
    client: AsyncClient, session_factory: SessionFactory
) -> None:
    """Varias peticiones simultáneas con la misma clave -> un solo pedido."""
    headers = {"Idempotency-Key": "k-concurrent"}
    responses = await asyncio.gather(
        *(client.post("/orders", json=BODY, headers=headers) for _ in range(5))
    )

    assert {r.json()["order_id"] for r in responses} == {responses[0].json()["order_id"]}
    assert await _count(session_factory, Order) == 1
    assert await _count(session_factory, IdempotencyKey) == 1


async def test_reusing_key_with_different_body_is_rejected(client: AsyncClient) -> None:
    """Misma clave con otro cuerpo -> 422 (evita confundir dos pedidos distintos)."""
    headers = {"Idempotency-Key": "k-reuse"}
    await client.post("/orders", json=BODY, headers=headers)
    other = {**BODY, "items": [{"name": "latte", "qty": 3}]}

    response = await client.post("/orders", json=other, headers=headers)
    assert response.status_code == 422
    assert "cuerpo de petición distinto" in response.json()["detail"]


async def test_missing_idempotency_key_is_rejected(client: AsyncClient) -> None:
    """Sin cabecera Idempotency-Key -> 400."""
    response = await client.post("/orders", json=BODY)
    assert response.status_code == 400


async def test_invalid_body_is_rejected(client: AsyncClient) -> None:
    """Cuerpo inválido -> 422 con el detalle de validación de FastAPI."""
    response = await client.post(
        "/orders",
        json={"customer_id": "abc123", "items": [{"name": "latte", "qty": -1}]},
        headers={"Idempotency-Key": "k-invalid"},
    )
    assert response.status_code == 422


async def test_api_key_is_required(client: AsyncClient) -> None:
    """Sin API key válida -> 401."""
    response = await client.post(
        "/orders", json=BODY, headers={"Idempotency-Key": "k", "X-API-Key": "wrong"}
    )
    assert response.status_code == 401


async def test_get_order_returns_detail_and_404(client: AsyncClient) -> None:
    """GET /orders/{id} devuelve el detalle; un id inexistente devuelve 404."""
    created = (await client.post("/orders", json=BODY, headers={"Idempotency-Key": "k-g"})).json()

    detail = await client.get(f"/orders/{created['order_id']}")
    assert detail.status_code == 200
    assert detail.json()["items"] == BODY["items"]
    assert detail.json()["customer_id"] == "abc123"

    missing = await client.get(f"/orders/{uuid.uuid4()}")
    assert missing.status_code == 404


async def test_liveness_and_metrics(client: AsyncClient) -> None:
    """Los endpoints operativos responden sin API key."""
    assert (await client.get("/health/live")).json() == {"status": "alive"}
    metrics = await client.get("/metrics")
    assert metrics.status_code == 200
    assert "http_requests_total" in metrics.text
