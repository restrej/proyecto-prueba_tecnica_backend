"""Propagación del ``trace_id`` a través de peticiones, tareas y mensajes.

El ``trace_id`` se genera en ``orders-service`` al recibir ``POST /orders``,
se guarda en la fila del outbox, viaja dentro del evento ``orders.created``,
el ``processor-service`` lo reutiliza en ``orders.completed`` y el
``notifier-service`` lo guarda en la notificación. Así, filtrando los logs de
todos los servicios por un mismo ``trace_id`` se reconstruye el flujo completo.

Se usa ``contextvars.ContextVar`` porque es *async-safe*: cada petición HTTP o
cada mensaje procesado en su propia tarea asyncio ve su propio valor, sin
mezclarse con los de otras tareas concurrentes (a diferencia de una variable
global o de ``threading.local``).
"""

from __future__ import annotations

import uuid  # para generar identificadores aleatorios únicos
from collections.abc import Iterator  # tipo de retorno del context manager
from contextlib import contextmanager  # decorador para crear context managers
from contextvars import ContextVar, Token  # almacenamiento por contexto async

# Variable de contexto que contiene el trace_id actual (None si no hay ninguno).
_trace_id_var: ContextVar[str | None] = ContextVar("trace_id", default=None)


def new_trace_id() -> str:
    """Genera un nuevo ``trace_id``.

    Returns:
        32 caracteres hexadecimales (UUID4 sin guiones), compatible con el
        formato de *trace-id* de W3C Trace Context.
    """
    return uuid.uuid4().hex


def get_trace_id() -> str | None:
    """Devuelve el ``trace_id`` del contexto actual, o ``None`` si no existe."""
    return _trace_id_var.get()


def set_trace_id(trace_id: str | None) -> Token[str | None]:
    """Establece el ``trace_id`` del contexto actual.

    Args:
        trace_id: identificador a propagar (o ``None`` para limpiarlo).

    Returns:
        Un ``Token`` que permite restaurar el valor anterior con
        :func:`reset_trace_id` (importante para no "contaminar" otras tareas).
    """
    return _trace_id_var.set(trace_id)


def reset_trace_id(token: Token[str | None]) -> None:
    """Restaura el ``trace_id`` que había antes de llamar a :func:`set_trace_id`."""
    _trace_id_var.reset(token)


@contextmanager
def trace_context(trace_id: str | None) -> Iterator[str | None]:
    """Context manager que fija un ``trace_id`` durante un bloque ``with``.

    Ejemplo::

        with trace_context(envelope.trace_id):
            logger.info("procesando")   # este log incluirá el trace_id

    Args:
        trace_id: identificador a usar dentro del bloque.

    Yields:
        El mismo ``trace_id`` para comodidad del llamador.
    """
    token = set_trace_id(trace_id)  # fija el valor y guarda el anterior
    try:
        yield trace_id  # ejecuta el cuerpo del bloque with
    finally:
        reset_trace_id(token)  # siempre restaura, incluso si hubo excepción
