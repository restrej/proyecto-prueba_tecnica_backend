"""Tests del formateador de logs JSON."""

from __future__ import annotations

import json
import logging

from cafe_common.logs import JsonFormatter
from cafe_common.tracing import trace_context


def _record(msg: str, **extra: object) -> logging.LogRecord:
    """Crea un LogRecord como lo haría ``logger.info(msg, extra=extra)``."""
    record = logging.LogRecord("app.test", logging.INFO, __file__, 1, msg, (), None)
    record.__dict__.update(extra)
    return record


def test_log_contains_required_fields_and_context_trace_id() -> None:
    """Cada línea incluye timestamp, level, service y el trace_id del contexto."""
    formatter = JsonFormatter("orders-service")
    with trace_context("abc123"):
        line = json.loads(formatter.format(_record("order_created", order_id="o-1")))

    assert line["service"] == "orders-service"
    assert line["level"] == "INFO"
    assert line["trace_id"] == "abc123"
    assert line["message"] == "order_created"
    assert line["order_id"] == "o-1"  # campo extra
    assert line["timestamp"].endswith("+00:00")  # UTC explícito


def test_explicit_trace_id_overrides_context() -> None:
    """Un trace_id pasado en extra= tiene prioridad sobre el del contexto."""
    formatter = JsonFormatter("svc")
    with trace_context("from-context"):
        line = json.loads(formatter.format(_record("x", trace_id="explicit")))
    assert line["trace_id"] == "explicit"
