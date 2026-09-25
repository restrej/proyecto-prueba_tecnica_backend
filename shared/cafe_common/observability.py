"""Observabilidad HTTP: middleware de trazas/métricas y endpoints operativos.

* :class:`TraceMiddleware` -> asigna un ``trace_id`` a cada petición, lo
  devuelve en la cabecera ``X-Trace-Id``, registra un log por petición y
  actualiza las métricas HTTP.
* :func:`build_ops_router` -> ``GET /health``, ``GET /health/live`` y
  ``GET /metrics``.
* :func:`setup_observability` -> instala ambos en una app FastAPI.
"""

from __future__ import annotations

import asyncio  # para ejecutar los health checks con timeout
import logging
import time  # medición de latencias con reloj monotónico
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

from fastapi import APIRouter, FastAPI, Request, Response
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest  # exposición
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from cafe_common.metrics import HTTP_REQUEST_DURATION_SECONDS, HTTP_REQUESTS_TOTAL
from cafe_common.tracing import new_trace_id, reset_trace_id, set_trace_id

logger = logging.getLogger(__name__)

# Cabecera HTTP en la que se recibe/devuelve el trace_id.
TRACE_HEADER = "X-Trace-Id"

# Un health check recibe el ``app.state`` y lanza excepción si algo va mal.
HealthCheck = Callable[[Any], Awaitable[Any]]


def _route_template(request: Request) -> str:
    """Devuelve la plantilla de la ruta que atendió la petición.

    Ejemplo: ``/notifications/abc123`` -> ``/notifications/{customer_id}``.
    Así las métricas tienen baja cardinalidad (una serie por endpoint, no por
    cliente). Debe llamarse DESPUÉS de ``call_next``: es el router de
    Starlette quien guarda la ruta elegida en ``scope["route"]``.

    Args:
        request: petición ya procesada.

    Returns:
        Plantilla de la ruta, o ``"unmatched"`` si ninguna coincidió (404).
    """
    route = request.scope.get("route")  # la fija el router al resolver la ruta
    return str(getattr(route, "path", None) or "unmatched")


class TraceMiddleware(BaseHTTPMiddleware):
    """Middleware que añade ``trace_id``, log de acceso y métricas por petición."""

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        """Procesa una petición HTTP envolviéndola con trazas y métricas.

        Args:
            request: petición entrante.
            call_next: siguiente elemento de la cadena (el endpoint).

        Returns:
            La respuesta del endpoint con la cabecera ``X-Trace-Id``.
        """
        # Reutiliza el trace_id del cliente si lo envía (p. ej. un API gateway),
        # de lo contrario se genera uno nuevo: aquí "nace" la traza.
        trace_id = request.headers.get(TRACE_HEADER) or new_trace_id()
        token = set_trace_id(trace_id)  # visible para todo el código del request
        start = time.perf_counter()  # reloj monotónico: inmune a cambios de hora
        status_code = 500  # valor por defecto si el endpoint lanza excepción
        try:
            response = await call_next(request)
            status_code = response.status_code
            response.headers[TRACE_HEADER] = trace_id  # el cliente puede citarlo
            return response
        finally:
            elapsed = time.perf_counter() - start
            path = _route_template(request)
            HTTP_REQUESTS_TOTAL.labels(request.method, path, str(status_code)).inc()
            HTTP_REQUEST_DURATION_SECONDS.labels(request.method, path).observe(elapsed)
            # Los health checks/metrics se loguean en DEBUG para no generar ruido.
            level = logging.DEBUG if path.startswith(("/health", "/metrics")) else logging.INFO
            logger.log(
                level,
                "http_request",
                extra={
                    "method": request.method,
                    "path": request.url.path,
                    "status_code": status_code,
                    "duration_ms": round(elapsed * 1000, 2),
                },
            )
            reset_trace_id(token)  # limpia el contexto al terminar


def build_ops_router(
    checks: Mapping[str, HealthCheck], *, timeout_seconds: float = 2.0
) -> APIRouter:
    """Construye el router con los endpoints operativos.

    Args:
        checks: mapa ``nombre -> función`` de comprobaciones de dependencias
            (base de datos, Redis, MongoDB...).
        timeout_seconds: tiempo máximo por comprobación.

    Returns:
        Un ``APIRouter`` con ``/health``, ``/health/live`` y ``/metrics``.
    """
    router = APIRouter(tags=["ops"])

    @router.get("/health/live")
    async def liveness() -> dict[str, str]:
        """Liveness: el proceso está vivo y responde (no mira dependencias)."""
        return {"status": "alive"}

    @router.get("/health")
    async def readiness(request: Request) -> JSONResponse:
        """Readiness: comprueba cada dependencia; 503 si alguna falla.

        Un orquestador (Docker/Kubernetes) solo debe enviar tráfico a la
        instancia cuando este endpoint devuelve 200.
        """
        results: dict[str, str] = {}
        healthy = True
        for name, check in checks.items():
            try:
                await asyncio.wait_for(check(request.app.state), timeout=timeout_seconds)
                results[name] = "ok"
            except Exception as exc:  # cualquier fallo -> dependencia caída
                healthy = False
                results[name] = f"error: {type(exc).__name__}"
        body = {"status": "ok" if healthy else "degraded", "checks": results}
        return JSONResponse(body, status_code=200 if healthy else 503)

    @router.get("/metrics")
    async def metrics() -> Response:
        """Expone todas las métricas en formato de texto de Prometheus."""
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    return router


def setup_observability(app: FastAPI, checks: Mapping[str, HealthCheck]) -> None:
    """Instala el middleware de trazas y los endpoints operativos en ``app``.

    Args:
        app: aplicación FastAPI.
        checks: health checks de las dependencias del servicio.
    """
    app.add_middleware(TraceMiddleware)
    app.include_router(build_ops_router(checks))
