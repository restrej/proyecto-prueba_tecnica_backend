"""Tests del middleware de observabilidad (trace_id y etiquetas de métricas)."""

from __future__ import annotations

from fastapi import APIRouter, FastAPI
from httpx import ASGITransport, AsyncClient
from prometheus_client import REGISTRY

from cafe_common.observability import setup_observability


def _app() -> FastAPI:
    """App mínima con una ruta parametrizada dentro de un router incluido."""
    app = FastAPI()
    router = APIRouter(prefix="/items")

    @router.get("/{item_id}")
    async def get_item(item_id: str) -> dict[str, str]:
        return {"id": item_id}

    app.include_router(router)
    setup_observability(app, checks={})
    return app


async def test_metrics_use_route_template_and_trace_header_is_returned() -> None:
    """La métrica usa /items/{item_id} (no la URL real) y se devuelve X-Trace-Id."""
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://t") as client:
        response = await client.get("/items/abc", headers={"X-Trace-Id": "trace-xyz"})

    assert response.headers["X-Trace-Id"] == "trace-xyz"  # traza reutilizada
    labels = {"method": "GET", "path": "/items/{item_id}", "status": "200"}
    assert REGISTRY.get_sample_value("http_requests_total", labels) >= 1
    raw = {"method": "GET", "path": "/items/abc", "status": "200"}
    assert REGISTRY.get_sample_value("http_requests_total", raw) is None


async def test_health_reports_ok_without_checks() -> None:
    """/health devuelve 200 cuando todas las comprobaciones pasan."""
    async with AsyncClient(transport=ASGITransport(app=_app()), base_url="http://t") as client:
        response = await client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
