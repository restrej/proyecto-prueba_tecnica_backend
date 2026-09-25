"""Punto de entrada de notifier-service (Application Factory)."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from pymongo import AsyncMongoClient
from redis.asyncio import Redis

from app.api import router
from app.config import Settings, get_settings
from app.handlers import NotificationHandler
from app.repository import MongoNotificationRepository
from cafe_common.events import Streams
from cafe_common.lifecycle import BackgroundWorkers
from cafe_common.logs import configure_logging
from cafe_common.messaging import ConsumerSettings, RedisStreamConsumer
from cafe_common.observability import setup_observability


def create_app(settings: Settings | None = None) -> FastAPI:
    """Construye la aplicación.

    Args:
        settings: configuración; por defecto se lee del entorno.

    Returns:
        App FastAPI con la API y el consumidor de ``orders.completed``.
    """
    settings = settings or get_settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Conecta MongoDB/Redis, crea índices y arranca el consumidor."""
        # tz_aware=True: las fechas leídas vuelven con zona UTC (no naive).
        mongo: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
            settings.mongo_url, tz_aware=True
        )
        collection = mongo[settings.mongo_database][settings.mongo_collection]
        repository = MongoNotificationRepository(collection)
        await repository.ensure_indexes()
        redis = Redis.from_url(settings.redis_url, decode_responses=True)

        app.state.mongo = mongo
        app.state.redis = redis
        app.state.notification_repository = repository

        workers = BackgroundWorkers()
        if settings.consumer_enabled:
            consumer = RedisStreamConsumer(
                redis,
                ConsumerSettings(
                    stream=Streams.ORDERS_COMPLETED,
                    group=settings.consumer_group,
                    consumer_name=settings.consumer_name,
                    batch_size=settings.consumer_batch_size,
                    block_ms=settings.consumer_block_ms,
                    claim_idle_ms=settings.consumer_claim_idle_ms,
                    max_deliveries=settings.consumer_max_deliveries,
                    retry_policy=settings.retry_policy(),
                ),
                NotificationHandler(repository).handle,
            )
            workers.start("orders-completed-consumer", consumer.run)
        try:
            yield
        finally:
            await workers.shutdown()
            await redis.aclose()
            await mongo.close()

    app = FastAPI(
        title="Café Cloud — notifier-service",
        version="1.0.0",
        description="Guarda notificaciones de pedidos completados y las expone por cliente.",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.api_key = settings.api_key_value
    setup_observability(
        app,
        checks={
            "mongodb": lambda state: state.mongo.admin.command("ping"),
            "redis": lambda state: state.redis.ping(),
        },
    )
    app.include_router(router)
    return app
