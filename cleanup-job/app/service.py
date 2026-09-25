"""Lógica de la limpieza: calcular el corte temporal y borrar lo anterior."""

from __future__ import annotations

import logging
import time
from datetime import datetime, timedelta

from prometheus_client import Counter
from pydantic import BaseModel

from app.repository import CleanupRepository
from cafe_common.clock import Clock, utcnow
from cafe_common.tracing import new_trace_id, trace_context

logger = logging.getLogger(__name__)

# Métricas específicas del job.
CLEANUP_RUNS_TOTAL = Counter("cleanup_runs_total", "Cleanup executions", ["result"])
CLEANUP_DELETED_TOTAL = Counter("cleanup_deleted_notifications_total", "Notifications deleted")


class CleanupResult(BaseModel):
    """Resultado de una ejecución de limpieza (también respuesta HTTP)."""

    deleted: int  # notificaciones borradas
    cutoff: datetime  # se borró todo lo creado antes de esta fecha
    ran_at: datetime  # cuándo se ejecutó
    duration_ms: float  # cuánto tardó
    trace_id: str  # traza de esta ejecución (para buscarla en logs)


class CleanupService:
    """Borra notificaciones más antiguas que la retención configurada."""

    def __init__(
        self,
        repository: CleanupRepository,
        *,
        retention: timedelta = timedelta(hours=24),
        clock: Clock = utcnow,
    ) -> None:
        """Crea el servicio.

        Args:
            repository: acceso a datos.
            retention: antigüedad máxima (24 h según la prueba).
            clock: reloj inyectable (tests deterministas).
        """
        self._repository = repository
        self._retention = retention
        self._clock = clock

    async def run(self) -> CleanupResult:
        """Ejecuta una limpieza y registra el resultado en logs y métricas.

        Returns:
            Resumen de la ejecución.
        """
        with trace_context(new_trace_id()) as trace_id:  # cada ejecución = 1 traza
            now = self._clock()
            cutoff = now - self._retention  # todo lo anterior a esto sobra
            start = time.perf_counter()
            try:
                deleted = await self._repository.delete_older_than(cutoff)
            except Exception:
                CLEANUP_RUNS_TOTAL.labels(result="error").inc()
                logger.exception("cleanup_failed", extra={"cutoff": cutoff.isoformat()})
                raise
            duration_ms = round((time.perf_counter() - start) * 1000, 2)
            CLEANUP_RUNS_TOTAL.labels(result="success").inc()
            CLEANUP_DELETED_TOTAL.inc(deleted)
            logger.info(
                "cleanup_finished",
                extra={
                    "deleted": deleted,
                    "cutoff": cutoff.isoformat(),
                    "duration_ms": duration_ms,
                },
            )
            return CleanupResult(
                deleted=deleted,
                cutoff=cutoff,
                ran_at=now,
                duration_ms=duration_ms,
                trace_id=str(trace_id),
            )
