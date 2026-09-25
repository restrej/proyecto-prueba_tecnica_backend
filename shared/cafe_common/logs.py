"""Logging estructurado en JSON.

Cada línea de log es un objeto JSON con, como mínimo, los campos que exige la
prueba: ``timestamp``, ``level``, ``service`` y ``trace_id``. Además se añaden
``logger``, ``message`` y cualquier campo extra pasado con ``extra={...}``.

Ejemplo de salida::

    {"timestamp": "2026-09-24T10:00:00.123+00:00", "level": "INFO",
     "service": "orders-service", "trace_id": "4bf9...", "logger": "app.services",
     "message": "order_created", "order_id": "7c1e..."}

Se implementa con la librería estándar (sin dependencias extra) para que el
formato sea explícito y fácil de revisar.
"""

from __future__ import annotations

import json  # serializa el diccionario del log a texto JSON
import logging  # sistema de logging estándar de Python
import sys  # para escribir en stdout (lo recoge Docker)
from datetime import UTC, datetime  # para formatear el timestamp en ISO-8601

from cafe_common.tracing import get_trace_id  # trace_id del contexto actual

# Conjunto de atributos que TODO LogRecord trae por defecto. Se calcula creando
# un LogRecord vacío y leyendo sus claves. Cualquier atributo que NO esté aquí
# ha sido añadido por el desarrollador vía ``extra=`` y se incluirá en el JSON.
_STANDARD_ATTRS: frozenset[str] = frozenset(
    vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()
) | {"message", "asctime"}


class JsonFormatter(logging.Formatter):
    """Formateador que convierte cada ``LogRecord`` en una línea JSON.

    Attributes:
        _service: nombre del microservicio que se incluye en cada log.
    """

    def __init__(self, service_name: str) -> None:
        """Crea el formateador.

        Args:
            service_name: valor del campo ``service`` de cada log.
        """
        super().__init__()  # inicializa la clase base logging.Formatter
        self._service = service_name  # se guarda para usarlo en format()

    def format(self, record: logging.LogRecord) -> str:
        """Transforma un registro de log en una cadena JSON.

        Args:
            record: registro creado por ``logger.info(...)`` y similares.

        Returns:
            La línea JSON que se escribirá en la salida.
        """
        payload: dict[str, object] = {
            # record.created es un epoch float; se convierte a ISO-8601 UTC.
            "timestamp": datetime.fromtimestamp(record.created, tz=UTC).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,  # DEBUG / INFO / WARNING / ERROR ...
            "service": self._service,  # qué microservicio emitió el log
            # Prioridad: trace_id explícito en extra=, si no el del contexto.
            "trace_id": getattr(record, "trace_id", None) or get_trace_id(),
            "logger": record.name,  # módulo que emitió el log
            "message": record.getMessage(),  # mensaje con los %s ya aplicados
        }
        # Añade los campos personalizados (extra=) que no sean estándar ni
        # colisionen con los que ya hemos puesto.
        for key, value in record.__dict__.items():
            if key not in _STANDARD_ATTRS and key not in payload:
                payload[key] = value
        # Si el log se hizo con logger.exception(...) se añade la traza.
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # default=str convierte UUID, datetime, etc. a texto; ensure_ascii=False
        # conserva tildes y emojis legibles.
        return json.dumps(payload, default=str, ensure_ascii=False)


def configure_logging(service_name: str, level: str = "INFO") -> None:
    """Configura el logging raíz del proceso para emitir JSON por stdout.

    Es idempotente: se puede llamar varias veces (p. ej. en tests) sin
    duplicar handlers.

    Args:
        service_name: nombre del servicio (campo ``service``).
        level: nivel mínimo de log (``DEBUG``, ``INFO``, ...).
    """
    handler = logging.StreamHandler(sys.stdout)  # escribe en la salida estándar
    handler.setFormatter(JsonFormatter(service_name))  # con nuestro formato JSON

    root = logging.getLogger()  # logger raíz: todos los demás propagan a él
    root.handlers[:] = [handler]  # reemplaza handlers previos (idempotencia)
    root.setLevel(level.upper())  # aplica el nivel configurado

    # Uvicorn y APScheduler instalan sus propios handlers de texto plano; se
    # eliminan y se hace que propaguen al raíz para que también salgan en JSON.
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "apscheduler"):
        third_party = logging.getLogger(name)
        third_party.handlers.clear()
        third_party.propagate = True

    # El access log de uvicorn se desactiva: nuestro middleware ya registra cada
    # petición HTTP con trace_id, método, ruta, status y duración.
    logging.getLogger("uvicorn.access").disabled = True
