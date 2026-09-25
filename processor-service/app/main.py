"""Punto de entrada de processor-service.

Es un *worker* con una pequeña API HTTP operativa (``/health``, ``/metrics``).
En el ``lifespan`` arranca dos tareas en segundo plano:

* el consumidor de ``orders.created`` (Redis Streams, consumer group), y
* el relay del outbox que publica ``orders.completed``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI
from redis.asyncio import Redis

from app.config import Settings, get_settings
from app.handlers import OrderProcessor
from cafe_common.db.outbox import OutboxRelay
from cafe_common.db.session import create_engine, create_session_factory, ping_database
from cafe_common.events import Streams
from cafe_common.lifecycle import BackgroundWorkers
from cafe_common.logs import configure_logging
from cafe_common.messaging import ConsumerSettings, RedisStreamConsumer, RedisStreamPublisher
from cafe_common.observability import setup_observability


def create_app(settings: Settings | None = None) -> FastAPI:
    """Construye la aplicación (patrón Application Factory).

    Args:
        settings: configuración; por defecto se lee del entorno.

    Returns:
        Aplicación FastAPI con el consumidor y el relay ligados a su ciclo de vida.
    """
    settings = settings or get_settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Crea conexiones, arranca los workers y los detiene al apagar."""
        engine = create_engine(settings.database_url)
        session_factory = create_session_factory(engine)
        redis = Redis.from_url(settings.redis_url, decode_responses=True)
        app.state.engine = engine
        app.state.redis = redis

        processor = OrderProcessor(
            session_factory,
            min_seconds=settings.processing_min_seconds,
            max_seconds=settings.processing_max_seconds,
            failure_rate=settings.processing_failure_rate,
        )
        consumer = RedisStreamConsumer(
            redis,
            ConsumerSettings(
                stream=Streams.ORDERS_CREATED,
                group=settings.consumer_group,
                consumer_name=settings.consumer_name,
                batch_size=settings.consumer_batch_size,
                block_ms=settings.consumer_block_ms,
                claim_idle_ms=settings.consumer_claim_idle_ms,
                max_deliveries=settings.consumer_max_deliveries,
                retry_policy=settings.retry_policy(),
            ),
            processor.handle,  # handler de negocio
            on_dead_letter=processor.mark_failed,  # pedido -> FAILED si va a DLQ
        )

        workers = BackgroundWorkers()
        workers.start("orders-created-consumer", consumer.run)
        if settings.outbox_relay_enabled:
            relay = OutboxRelay(
                session_factory,
                RedisStreamPublisher(redis),
                streams=[Streams.ORDERS_COMPLETED],  # solo publica lo suyo
                producer=settings.service_name,
                batch_size=settings.outbox_batch_size,
                poll_interval=settings.outbox_poll_interval_seconds,
            )
            workers.start("outbox-relay", relay.run)
        try:
            yield
        finally:
            await workers.shutdown()
            await redis.aclose()
            await engine.dispose()

    app = FastAPI(
        title="Café Cloud — processor-service",
        version="1.0.0",
        description="Consume `orders.created`, prepara el pedido y emite `orders.completed`.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    setup_observability(
        app,
        checks={
            "database": lambda state: ping_database(state.engine),
            "redis": lambda state: state.redis.ping(),
        },
    )
    return app
