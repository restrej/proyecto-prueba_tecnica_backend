"""Tests del procesador de pedidos (idempotencia, outbox, errores)."""

from __future__ import annotations

import random
import uuid

import pytest
from sqlalchemy import select

from app.handlers import OrderProcessor, TransientProcessingError
from cafe_common.clock import utcnow
from cafe_common.db.models import Order, OrderItem, OutboxEvent, ProcessedEvent
from cafe_common.db.session import SessionFactory
from cafe_common.events import EventEnvelope, OrderStatus, Streams
from cafe_common.retry import PermanentError


async def _no_sleep(_: float) -> None:
    """Sustituye asyncio.sleep: la preparación simulada no espera en tests."""


def _processor(session_factory: SessionFactory, failure_rate: float = 0.0) -> OrderProcessor:
    """Crea un procesador sin esperas y con aleatoriedad determinista."""
    return OrderProcessor(
        session_factory, failure_rate=failure_rate, sleep=_no_sleep, rng=random.Random(42)
    )


async def _create_pending_order(session_factory: SessionFactory) -> Order:
    """Inserta un pedido PENDING (como lo haría orders-service)."""
    order = Order(
        id=uuid.uuid4(),
        customer_id="abc123",
        status=OrderStatus.PENDING.value,
        items=[OrderItem(name="latte", qty=1), OrderItem(name="muffin", qty=2)],
    )
    async with session_factory() as session, session.begin():
        session.add(order)
    return order


def _created_event(order: Order) -> EventEnvelope:
    """Construye el evento orders.created correspondiente al pedido."""
    return EventEnvelope(
        event_type=Streams.ORDERS_CREATED,
        producer="orders-service",
        trace_id="trace-123",
        payload={
            "order_id": str(order.id),
            "customer_id": order.customer_id,
            "status": "PENDING",
            "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
            "created_at": utcnow().isoformat(),
        },
    )


async def test_order_is_completed_and_completed_event_is_enqueued(
    session_factory: SessionFactory,
) -> None:
    """El pedido pasa a COMPLETED y se encola orders.completed con el mismo trace_id."""
    order = await _create_pending_order(session_factory)
    await _processor(session_factory).handle(_created_event(order))

    async with session_factory() as session:
        stored = await session.get(Order, order.id)
        events = (await session.scalars(select(OutboxEvent))).all()
    assert stored is not None
    assert stored.status == "COMPLETED"
    assert stored.completed_at is not None
    assert len(events) == 1
    assert events[0].stream == Streams.ORDERS_COMPLETED
    assert events[0].trace_id == "trace-123"
    assert events[0].payload["customer_id"] == "abc123"
    assert events[0].payload["items"] == [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}]


async def test_duplicate_event_is_processed_only_once(session_factory: SessionFactory) -> None:
    """Idempotent consumer: el mismo evento dos veces -> un solo orders.completed."""
    order = await _create_pending_order(session_factory)
    event = _created_event(order)
    processor = _processor(session_factory)

    await processor.handle(event)
    await processor.handle(event)  # re-entrega (at-least-once)

    async with session_factory() as session:
        outbox = (await session.scalars(select(OutboxEvent))).all()
        inbox = (await session.scalars(select(ProcessedEvent))).all()
    assert len(outbox) == 1
    assert len(inbox) == 1


async def test_unknown_order_is_a_permanent_error(session_factory: SessionFactory) -> None:
    """Un pedido inexistente no se reintenta: PermanentError (-> DLQ)."""
    ghost = Order(id=uuid.uuid4(), customer_id="nobody", status="PENDING", items=[])
    with pytest.raises(PermanentError):
        await _processor(session_factory).handle(_created_event(ghost))


async def test_invalid_payload_is_a_permanent_error(session_factory: SessionFactory) -> None:
    """Un payload que no cumple el contrato -> PermanentError."""
    envelope = EventEnvelope(
        event_type=Streams.ORDERS_CREATED, producer="x", trace_id="t", payload={"foo": "bar"}
    )
    with pytest.raises(PermanentError):
        await _processor(session_factory).handle(envelope)


async def test_transient_failure_leaves_order_untouched(session_factory: SessionFactory) -> None:
    """Un fallo transitorio no deja cambios a medias (se podrá reintentar)."""
    order = await _create_pending_order(session_factory)
    with pytest.raises(TransientProcessingError):
        await _processor(session_factory, failure_rate=1.0).handle(_created_event(order))

    async with session_factory() as session:
        stored = await session.get(Order, order.id)
        assert stored is not None and stored.status == "PENDING"
        assert (await session.scalars(select(OutboxEvent))).all() == []


async def test_dead_letter_hook_marks_order_failed(session_factory: SessionFactory) -> None:
    """Si el mensaje acaba en la DLQ, el pedido queda FAILED."""
    order = await _create_pending_order(session_factory)
    await _processor(session_factory).mark_failed(_created_event(order), RuntimeError("boom"))

    async with session_factory() as session:
        stored = await session.get(Order, order.id)
    assert stored is not None and stored.status == "FAILED"
