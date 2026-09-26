"""Punto de entrada de orders-service (patrón *Application Factory*).

Se arranca con::

    uvicorn app.main:create_app --factory --host 0.0.0.0 --port 8000

Usar una fábrica (en lugar de una variable global ``app``) permite crear apps
con configuraciones distintas en los tests y evita leer el entorno al importar.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from app.api.routes import register_exception_handlers, router
from app.config import Settings, get_settings
from cafe_common.db.outbox import OutboxRelay
from cafe_common.db.session import create_engine, create_session_factory, ping_database
from cafe_common.events import Streams
from cafe_common.lifecycle import BackgroundWorkers
from cafe_common.logs import configure_logging
from cafe_common.messaging import RedisStreamPublisher, create_redis_client
from cafe_common.observability import setup_observability


def create_app(settings: Settings | None = None) -> FastAPI:
    """Construye y configura la aplicación FastAPI.

    Args:
        settings: configuración a usar; por defecto se lee del entorno.

    Returns:
        La aplicación lista para servir.
    """
    settings = settings or get_settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Ciclo de vida: crea recursos al arrancar y los libera al parar.

        Todo lo anterior al ``yield`` se ejecuta antes de aceptar peticiones;
        lo posterior, al recibir SIGTERM (``docker compose down``).
        """
        engine = create_engine(settings.database_url)  # pool de conexiones SQL
        redis = create_redis_client(settings.redis_url)
        app.state.engine = engine
        app.state.session_factory = create_session_factory(engine)
        app.state.redis = redis

        workers = BackgroundWorkers()
        if settings.outbox_relay_enabled:
            relay = OutboxRelay(
                app.state.session_factory,
                RedisStreamPublisher(redis),
                streams=[Streams.ORDERS_CREATED],  # este servicio solo publica este
                producer=settings.service_name,
                batch_size=settings.outbox_batch_size,
                poll_interval=settings.outbox_poll_interval_seconds,
            )
            workers.start("outbox-relay", relay.run)
        try:
            yield  # la aplicación atiende peticiones mientras estamos aquí
        finally:
            await workers.shutdown()  # 1) detener el relay
            await redis.aclose()  # 2) cerrar conexiones a Redis
            await engine.dispose()  # 3) cerrar el pool de PostgreSQL

    app = FastAPI(
        title="Café Cloud — orders-service",
        version="1.0.0",
        description="Registra pedidos y publica `orders.created` (Transactional Outbox).",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.api_key = settings.api_key_value  # la lee verify_api_key

    # /health comprueba PostgreSQL y Redis; /metrics expone Prometheus.
    setup_observability(
        app,
        checks={
            "database": lambda state: ping_database(state.engine),
            "redis": lambda state: state.redis.ping(),
        },
    )
    app.include_router(router)
    register_exception_handlers(app)
    return app
