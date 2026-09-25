"""Punto de entrada de cleanup-job: planificador + API operativa mínima.

* APScheduler ejecuta :meth:`CleanupService.run` cada ``INTERVAL_SECONDS``.
* ``POST /jobs/cleanup/run`` permite lanzarlo manualmente (útil en pruebas).
* ``GET /health`` y ``GET /metrics`` como el resto de servicios.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import APIRouter, Depends, FastAPI, Request
from pymongo import AsyncMongoClient

from app.config import Settings, get_settings
from app.repository import MongoCleanupRepository
from app.service import CleanupResult, CleanupService
from cafe_common.clock import utcnow
from cafe_common.logs import configure_logging
from cafe_common.observability import setup_observability
from cafe_common.security import verify_api_key

router = APIRouter(tags=["jobs"], dependencies=[Depends(verify_api_key)])


@router.post("/jobs/cleanup/run", response_model=CleanupResult)
async def run_cleanup(request: Request) -> CleanupResult:
    """Ejecuta la limpieza inmediatamente y devuelve el resultado."""
    service: CleanupService = request.app.state.cleanup_service
    return await service.run()


def create_app(settings: Settings | None = None) -> FastAPI:
    """Construye la aplicación del job.

    Args:
        settings: configuración; por defecto se lee del entorno.

    Returns:
        App FastAPI con el planificador ligado a su ciclo de vida.
    """
    settings = settings or get_settings()
    configure_logging(settings.service_name, settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        """Conecta MongoDB y arranca/detiene el planificador."""
        mongo: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(
            settings.mongo_url, tz_aware=True
        )
        collection = mongo[settings.mongo_database][settings.mongo_collection]
        service = CleanupService(
            MongoCleanupRepository(collection),
            retention=timedelta(hours=settings.retention_hours),
        )
        app.state.mongo = mongo
        app.state.cleanup_service = service

        scheduler = AsyncIOScheduler(timezone="UTC")
        if settings.scheduler_enabled:
            scheduler.add_job(
                service.run,
                trigger="interval",
                seconds=settings.interval_seconds,
                id="cleanup-notifications",
                max_instances=1,  # nunca dos limpiezas solapadas
                coalesce=True,  # si se acumulan ejecuciones perdidas, solo una
                next_run_time=utcnow() if settings.run_on_startup else None,
            )
            scheduler.start()
        try:
            yield
        finally:
            if scheduler.running:
                scheduler.shutdown(wait=False)
            await mongo.close()

    app = FastAPI(
        title="Café Cloud — cleanup-job",
        version="1.0.0",
        description="Borra notificaciones con más de 24 horas (APScheduler).",
        lifespan=lifespan,
    )
    app.state.settings = settings
    app.state.api_key = settings.api_key_value
    setup_observability(app, checks={"mongodb": lambda state: state.mongo.admin.command("ping")})
    app.include_router(router)
    return app
