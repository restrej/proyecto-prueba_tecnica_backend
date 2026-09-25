"""Métricas Prometheus compartidas por todos los servicios.

Se definen a nivel de módulo (una sola vez por proceso) porque
``prometheus_client`` usa un registro global: registrar dos veces la misma
métrica lanza un error. Cada servicio las expone en ``GET /metrics``.
"""

from __future__ import annotations

from prometheus_client import Counter, Histogram  # tipos de métrica

# Total de peticiones HTTP, etiquetado por método, plantilla de ruta y status.
# Se usa la PLANTILLA de ruta (/notifications/{customer_id}) y no la ruta real
# para no crear una serie temporal por cada cliente (explosión de cardinalidad).
HTTP_REQUESTS_TOTAL = Counter(
    "http_requests_total",
    "Total HTTP requests handled",
    ["method", "path", "status"],
)

# Latencia de las peticiones HTTP (histograma -> permite percentiles p50/p95/p99).
HTTP_REQUEST_DURATION_SECONDS = Histogram(
    "http_request_duration_seconds",
    "HTTP request latency in seconds",
    ["method", "path"],
)

# Eventos publicados en el broker, por stream.
EVENTS_PUBLISHED_TOTAL = Counter(
    "events_published_total",
    "Events published to the message broker",
    ["stream"],
)

# Eventos consumidos por stream y resultado (success / duplicate / dead_letter).
EVENTS_CONSUMED_TOTAL = Counter(
    "events_consumed_total",
    "Events consumed from the message broker",
    ["stream", "result"],
)
