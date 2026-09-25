"""Ejecución puntual (one-shot) de la limpieza desde línea de comandos.

El modo normal es el servicio con APScheduler (``app.main``). Este script
ejecuta UNA limpieza y termina, por si se prefiere planificar con cron del
sistema o con un CronJob de Kubernetes::

    python job.py                  # dentro del contenedor o con poetry run
    */1 * * * * python /srv/cleanup-job/job.py   # ejemplo de crontab
"""

from __future__ import annotations

import asyncio
from datetime import timedelta
from typing import Any

from pymongo import AsyncMongoClient

from app.config import get_settings
from app.repository import MongoCleanupRepository
from app.service import CleanupService
from cafe_common.logs import configure_logging


async def run_once() -> int:
    """Conecta a MongoDB, ejecuta una limpieza y devuelve cuántas borró."""
    settings = get_settings()
    mongo: AsyncMongoClient[dict[str, Any]] = AsyncMongoClient(settings.mongo_url, tz_aware=True)
    try:
        collection = mongo[settings.mongo_database][settings.mongo_collection]
        service = CleanupService(
            MongoCleanupRepository(collection),
            retention=timedelta(hours=settings.retention_hours),
        )
        return (await service.run()).deleted
    finally:
        await mongo.close()


def main() -> None:
    """Punto de entrada CLI."""
    settings = get_settings()
    configure_logging(settings.service_name, settings.log_level)
    asyncio.run(run_once())


if __name__ == "__main__":
    main()
