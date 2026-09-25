"""Reloj de la aplicación.

Centralizar la obtención de "la hora actual" en una función permite:

1. Garantizar que TODAS las fechas son *timezone-aware* en UTC (nunca naive).
2. Inyectar un reloj falso en los tests (patrón *dependency injection*), de
   modo que se puedan probar reglas temporales como "borrar lo de hace >24h"
   sin esperar ni depender del reloj real.
"""

from __future__ import annotations  # permite usar anotaciones modernas en 3.11

from collections.abc import Callable  # tipo para "algo invocable"
from datetime import UTC, datetime  # UTC es la zona horaria estándar

# Alias de tipo: un "Clock" es cualquier función sin argumentos que devuelva un
# datetime. Los servicios reciben un Clock en su constructor.
Clock = Callable[[], datetime]


def utcnow() -> datetime:
    """Devuelve la fecha/hora actual en UTC con información de zona horaria.

    Returns:
        ``datetime`` aware en UTC, p. ej. ``2026-09-24T12:00:00+00:00``.
    """
    # datetime.now(UTC) crea un datetime "aware"; datetime.utcnow() (deprecado)
    # devolvería uno "naive", que es fuente habitual de bugs al comparar fechas.
    return datetime.now(UTC)
