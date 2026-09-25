"""Tests de notifier-service: handler idempotente y endpoint GET."""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.handlers import NotificationHandler
from app.main import create_app
from app.models import Notification
from cafe_common.events import EventEnvelope, Streams
from cafe_common.retry import PermanentError
from tests.fakes import InMemoryNotificationRepository

FIXED_NOW = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)


def _completed_event(customer_id: str = "abc123") -> EventEnvelope:
    """Evento orders.completed de ejemplo."""
    return EventEnvelope(
        event_type=Streams.ORDERS_COMPLETED,
        producer="processor-service",
        trace_id="trace-1",
        payload={
            "order_id": str(uuid.uuid4()),
            "customer_id": customer_id,
            "status": "COMPLETED",
            "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
            "completed_at": FIXED_NOW.isoformat(),
        },
    )


async def test_handler_creates_notification() -> None:
    """Un evento válido crea una notificación con mensaje y trace_id."""
    repo = InMemoryNotificationRepository()
    event = _completed_event()
    await NotificationHandler(repo, clock=lambda: FIXED_NOW).handle(event)

    [notification] = repo.items.values()
    assert notification.id == str(event.event_id)
    assert notification.customer_id == "abc123"
    assert "1x latte, 2x muffin" in notification.message
    assert notification.trace_id == "trace-1"
    assert notification.created_at == FIXED_NOW


async def test_duplicate_event_creates_single_notification() -> None:
    """Idempotent consumer: el mismo evento dos veces -> una notificación."""
    repo = InMemoryNotificationRepository()
    handler = NotificationHandler(repo)
    event = _completed_event()
    await handler.handle(event)
    await handler.handle(event)
    assert len(repo.items) == 1


async def test_invalid_payload_is_permanent_error() -> None:
    """Payload inválido -> PermanentError (irá a la DLQ)."""
    bad = EventEnvelope(event_type="orders.completed", producer="x", trace_id="t", payload={})
    with pytest.raises(PermanentError):
        await NotificationHandler(InMemoryNotificationRepository()).handle(bad)


def test_document_round_trip() -> None:
    """to_document/from_document conservan los datos (id <-> _id)."""
    original = Notification(
        id="e1",
        order_id="o1",
        customer_id="c1",
        message="m",
        items=[],
        trace_id="t",
        created_at=FIXED_NOW,
    )
    document = original.to_document()
    assert document["_id"] == "e1" and "id" not in document
    assert Notification.from_document(document) == original


@pytest.fixture
async def api() -> AsyncIterator[tuple[AsyncClient, InMemoryNotificationRepository]]:
    """Cliente HTTP en proceso con el repositorio en memoria inyectado."""
    settings = Settings(redis_url="redis://unused", mongo_url="mongodb://unused", api_key="k")  # type: ignore[arg-type]
    app = create_app(settings)
    repo = InMemoryNotificationRepository()
    app.state.notification_repository = repo  # sustituye a MongoDB
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test", headers={"X-API-Key": "k"}
    ) as client:
        yield client, repo


async def test_get_notifications_returns_customer_notifications(
    api: tuple[AsyncClient, InMemoryNotificationRepository],
) -> None:
    """GET devuelve solo las del cliente pedido."""
    client, repo = api
    handler = NotificationHandler(repo)
    await handler.handle(_completed_event("abc123"))
    await handler.handle(_completed_event("someone-else"))

    response = await client.get("/notifications/abc123")
    assert response.status_code == 200
    body = response.json()
    assert len(body) == 1
    assert body[0]["customer_id"] == "abc123"


async def test_get_notifications_empty_and_auth(
    api: tuple[AsyncClient, InMemoryNotificationRepository],
) -> None:
    """Cliente sin notificaciones -> lista vacía; sin API key -> 401."""
    client, _ = api
    assert (await client.get("/notifications/nobody")).json() == []
    unauthorized = await client.get("/notifications/nobody", headers={"X-API-Key": "bad"})
    assert unauthorized.status_code == 401
