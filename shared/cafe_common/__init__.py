"""cafe_common: librería compartida de Café Cloud.

Módulos disponibles:

* ``tracing``       -> guarda/propaga el ``trace_id`` con ``contextvars``.
* ``logs``          -> configura logs estructurados en JSON.
* ``clock``         -> reloj UTC inyectable (facilita tests deterministas).
* ``events``        -> contrato (envelope + payloads) de los eventos de dominio.
* ``retry``         -> reintentos con backoff exponencial.
* ``metrics``       -> contadores/histogramas Prometheus compartidos.
* ``messaging``     -> publicador y consumidor sobre Redis Streams.
* ``observability`` -> middleware HTTP, ``/health`` y ``/metrics``.
* ``security``      -> dependencia FastAPI para validar la API key.
* ``lifecycle``     -> gestor de tareas en segundo plano (arranque/parada).
* ``db``            -> esquema SQL y Transactional Outbox (extra ``sql``).
"""

# Versión de la librería; útil para exponerla en logs o en /health si se desea.
__version__ = "1.0.0"
