"""Gestión de tareas en segundo plano ligadas al ciclo de vida de la app.

Los servicios ejecutan bucles de larga duración (consumidor de eventos, relay
del outbox) junto al servidor HTTP. Esta clase los arranca en el ``lifespan``
de FastAPI y, al apagar, les pide que terminen de forma ordenada (graceful
shutdown) y, si no lo hacen a tiempo, los cancela.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)

# Un "worker" es una función async que recibe el evento de parada.
Worker = Callable[[asyncio.Event], Awaitable[None]]


class BackgroundWorkers:
    """Contenedor de tareas asyncio con parada cooperativa."""

    def __init__(self) -> None:
        """Inicializa el evento de parada compartido y la lista de tareas."""
        self._stop_event = asyncio.Event()  # se activa al apagar
        self._tasks: list[asyncio.Task[None]] = []

    def start(self, name: str, worker: Worker) -> None:
        """Lanza ``worker`` como tarea asyncio en segundo plano.

        Args:
            name: nombre de la tarea (aparece en logs y depuración).
            worker: bucle a ejecutar; debe salir cuando se active el evento.
        """
        task = asyncio.create_task(worker(self._stop_event), name=name)  # type: ignore[arg-type]
        self._tasks.append(task)
        logger.info("background_worker_started", extra={"worker": name})

    async def shutdown(self, grace_seconds: float = 10.0) -> None:
        """Detiene todas las tareas de forma ordenada.

        Args:
            grace_seconds: segundos que se espera a que terminen solas antes de
                cancelarlas a la fuerza.
        """
        self._stop_event.set()  # 1) pide a los bucles que terminen
        if not self._tasks:
            return
        _, pending = await asyncio.wait(self._tasks, timeout=grace_seconds)  # 2) espera
        for task in pending:  # 3) cancela las que no terminaron a tiempo
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        logger.info("background_workers_stopped", extra={"cancelled": len(pending)})
