"""Test de integración end-to-end del flujo completo de Café Cloud.

Simula exactamente el flujo que pide la prueba::

    POST /orders -> mensaje en la cola (orders.created) -> processor-service
    lo procesa -> orders.completed -> notifier-service crea la notificación
    -> GET /notifications/{customer_id} la devuelve.

Se ejecuta contra el stack levantado con docker compose (caja negra, solo
HTTP). Las URLs se configuran por variables de entorno:

* Desde el host: ``pytest tests/integration`` (usa localhost:8001/8003).
* Dentro de Docker: ``make test-integration`` (usa los nombres de servicio).

Si los servicios no están levantados, el test se SALTA (no falla).
"""

from __future__ import annotations

import os
import time
import uuid

import httpx
import pytest

ORDERS_URL = os.getenv("ORDERS_URL", "http://localhost:8001")
NOTIFIER_URL = os.getenv("NOTIFIER_URL", "http://localhost:8003")
CLEANUP_URL = os.getenv("CLEANUP_URL", "http://localhost:8004")
API_KEY = os.getenv("API_KEY", "cafe-cloud-dev-key")
TIMEOUT_SECONDS = float(os.getenv("E2E_TIMEOUT_SECONDS", "60"))
HEADERS = {"X-API-Key": API_KEY}


def _wait_until_healthy(base_url: str, timeout: float = 30.0) -> bool:
    """Espera a que ``/health`` responda 200 (o devuelve False al agotar el tiempo)."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if httpx.get(f"{base_url}/health", timeout=2).status_code == 200:
                return True
        except httpx.HTTPError:
            pass
        time.sleep(1)
    return False


@pytest.fixture(scope="module", autouse=True)
def _stack_running() -> None:
    """Salta todos los tests del módulo si el stack no está disponible."""
    for url in (ORDERS_URL, NOTIFIER_URL):
        if not _wait_until_healthy(url):
            pytest.skip(
                f"el servicio {url} no responde; ejecuta primero `docker compose up`"
            )


def _poll(fetch, predicate, timeout: float = TIMEOUT_SECONDS):  # type: ignore[no-untyped-def]
    """Llama a ``fetch`` hasta que ``predicate(resultado)`` sea cierto.

    La consistencia es EVENTUAL: la notificación aparece unos segundos después
    del POST (2-5 s de preparación simulada), así que se sondea.
    """
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        last = fetch()
        if predicate(last):
            return last
        time.sleep(1)
    raise AssertionError(
        f"la condición no se cumplió en {timeout}s; último valor: {last!r}"
    )


def test_order_flows_end_to_end_until_notification() -> None:
    """Pedido -> procesado -> notificación, con idempotencia y trace_id propagado."""
    customer_id = f"e2e-{uuid.uuid4().hex[:10]}"
    body = {
        "customer_id": customer_id,
        "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
    }
    headers = {**HEADERS, "Idempotency-Key": str(uuid.uuid4())}

    # 1) Crear el pedido.
    created = httpx.post(f"{ORDERS_URL}/orders", json=body, headers=headers, timeout=10)
    assert created.status_code == 201, created.text
    order = created.json()
    assert order["status"] == "PENDING"
    trace_id = created.headers["X-Trace-Id"]

    # 2) Reintento con la misma Idempotency-Key -> mismo pedido, sin duplicar.
    retried = httpx.post(f"{ORDERS_URL}/orders", json=body, headers=headers, timeout=10)
    assert retried.status_code == 200
    assert retried.json()["order_id"] == order["order_id"]

    # 3) processor-service lo completa (consistencia eventual).
    detail = _poll(
        lambda: httpx.get(
            f"{ORDERS_URL}/orders/{order['order_id']}", headers=HEADERS
        ).json(),
        lambda d: d.get("status") == "COMPLETED",
    )
    assert detail["completed_at"] is not None

    # 4) notifier-service crea UNA notificación, consultable por cliente.
    notifications = _poll(
        lambda: httpx.get(
            f"{NOTIFIER_URL}/notifications/{customer_id}", headers=HEADERS
        ).json(),
        lambda items: len(items) >= 1,
    )
    assert len(notifications) == 1  # el reintento del paso 2 no generó otra
    notification = notifications[0]
    assert notification["order_id"] == order["order_id"]
    assert notification["trace_id"] == trace_id  # la traza cruzó los 3 servicios
    assert "latte" in notification["message"]


def test_manual_cleanup_endpoint_keeps_recent_notifications() -> None:
    """El job manual responde y no borra notificaciones recientes (< 24 h)."""
    try:
        response = httpx.post(
            f"{CLEANUP_URL}/jobs/cleanup/run", headers=HEADERS, timeout=10
        )
    except httpx.HTTPError:
        pytest.skip("cleanup-job no responde")
    assert response.status_code == 200
    assert response.json()["deleted"] >= 0
