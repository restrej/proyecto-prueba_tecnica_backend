"""Datos de ejemplo (seed) para pruebas locales.

Uso (tras ``alembic upgrade head``)::

    poetry run python -m app.seed

Inserta dos pedidos con ids FIJOS, por lo que es idempotente: ejecutarlo
varias veces no duplica datos.

* Un pedido ``COMPLETED`` histórico (no genera eventos).
* Un pedido ``PENDING`` con su evento en el outbox: al levantar el sistema,
  el relay lo publica y recorre todo el flujo, generando una notificación para
  el cliente ``seed-customer`` (demostración sin hacer ninguna petición).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from datetime import timedelta

from app.config import get_settings
from cafe_common.clock import utcnow
from cafe_common.db.models import Order, OrderItem
from cafe_common.db.outbox import build_outbox_event
from cafe_common.db.session import create_engine, create_session_factory
from cafe_common.events import OrderCreatedPayload, OrderItemPayload, OrderStatus, Streams
from cafe_common.logs import configure_logging
from cafe_common.tracing import new_trace_id

logger = logging.getLogger("app.seed")

# Ids fijos -> el seed es idempotente.
COMPLETED_ORDER_ID = uuid.UUID("00000000-0000-4000-8000-000000000001")
PENDING_ORDER_ID = uuid.UUID("00000000-0000-4000-8000-000000000002")
SEED_CUSTOMER = "seed-customer"


async def seed() -> int:
    """Inserta los pedidos de ejemplo que no existan todavía.

    Returns:
        Número de pedidos insertados en esta ejecución (0 si ya existían).
    """
    settings = get_settings()
    engine = create_engine(settings.database_url)
    session_factory = create_session_factory(engine)
    inserted = 0
    try:
        async with session_factory() as session, session.begin():
            now = utcnow()
            if await session.get(Order, COMPLETED_ORDER_ID) is None:
                session.add(
                    Order(
                        id=COMPLETED_ORDER_ID,
                        customer_id=SEED_CUSTOMER,
                        status=OrderStatus.COMPLETED.value,
                        created_at=now - timedelta(days=1),
                        updated_at=now - timedelta(days=1),
                        completed_at=now - timedelta(days=1),
                        items=[OrderItem(name="espresso", qty=1)],
                    )
                )
                inserted += 1
            if await session.get(Order, PENDING_ORDER_ID) is None:
                items = [OrderItem(name="latte", qty=1), OrderItem(name="muffin", qty=2)]
                session.add(
                    Order(
                        id=PENDING_ORDER_ID,
                        customer_id=SEED_CUSTOMER,
                        status=OrderStatus.PENDING.value,
                        created_at=now,
                        updated_at=now,
                        items=items,
                    )
                )
                await session.flush()  # el pedido debe existir antes que su evento
                session.add(
                    build_outbox_event(
                        stream=Streams.ORDERS_CREATED,
                        aggregate_id=PENDING_ORDER_ID,
                        payload=OrderCreatedPayload(
                            order_id=PENDING_ORDER_ID,
                            customer_id=SEED_CUSTOMER,
                            status=OrderStatus.PENDING,
                            items=[OrderItemPayload(name=i.name, qty=i.qty) for i in items],
                            created_at=now,
                        ),
                        trace_id=new_trace_id(),
                    )
                )
                inserted += 1
    finally:
        await engine.dispose()
    logger.info("seed_finished", extra={"inserted_orders": inserted})
    return inserted


def main() -> None:
    """Punto de entrada de línea de comandos."""
    configure_logging("orders-seed", get_settings().log_level)
    asyncio.run(seed())


if __name__ == "__main__":
    main()
