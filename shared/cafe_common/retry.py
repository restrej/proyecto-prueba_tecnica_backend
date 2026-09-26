"""Reintentos con backoff exponencial (patrón *Retry* + estrategia configurable).

Fórmula de espera tras el intento fallido número ``n`` (empezando en 1)::

    espera = min(max_delay, base_delay * multiplier ** (n - 1))

Con los valores por defecto (0.5 s, x2): 0.5 s, 1 s, 2 s, 4 s, ... hasta 30 s.
Opcionalmente se aplica *jitter* (aleatoriedad) para que muchos consumidores
que fallan a la vez no reintenten todos en el mismo instante (*thundering herd*).
"""

from __future__ import annotations

import asyncio  # para asyncio.sleep (espera no bloqueante)
import logging  # para registrar cada reintento
import random  # para el jitter
from collections.abc import Awaitable, Callable  # tipos de funciones async
from dataclasses import dataclass  # para la política inmutable
from typing import TypeVar  # tipo genérico del resultado

logger = logging.getLogger(__name__)  # logger del módulo

T = TypeVar("T")  # tipo que devuelve la operación reintentada


class PermanentError(Exception):
    """Error NO recuperable: reintentar no sirve (p. ej. mensaje malformado).

    Cuando un handler lanza esta excepción, el consumidor deja de reintentar
    inmediatamente y envía el mensaje a la *dead letter queue*.
    """


class RetryExhaustedError(Exception):
    """Se agotaron todos los intentos permitidos por la política.

    Attributes:
        attempts: número de intentos realizados.
        last_error: la última excepción capturada.
    """

    def __init__(self, attempts: int, last_error: BaseException) -> None:
        """Crea el error con el contexto del último fallo."""
        super().__init__(f"la operación falló tras {attempts} intentos: {last_error!r}")
        self.attempts = attempts
        self.last_error = last_error


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    """Política de reintentos (objeto de valor inmutable).

    Attributes:
        max_attempts: intentos totales (el primero + reintentos).
        base_delay: espera en segundos tras el primer fallo.
        multiplier: factor de crecimiento exponencial.
        max_delay: tope superior de espera en segundos.
        jitter: si es True, la espera real es aleatoria en [espera/2, espera].
    """

    max_attempts: int = 5
    base_delay: float = 0.5
    multiplier: float = 2.0
    max_delay: float = 30.0
    jitter: bool = True

    def __post_init__(self) -> None:
        """Valida los parámetros al construir la política (fail fast)."""
        if self.max_attempts < 1:
            raise ValueError("max_attempts debe ser >= 1")
        if self.base_delay < 0 or self.max_delay < 0:
            raise ValueError("los tiempos de espera deben ser >= 0")
        if self.multiplier < 1:
            raise ValueError("multiplier debe ser >= 1")

    def backoff(self, attempt: int) -> float:
        """Calcula cuánto esperar después del intento fallido ``attempt``.

        Args:
            attempt: número (1-based) del intento que acaba de fallar.

        Returns:
            Segundos a esperar antes del siguiente intento.
        """
        # Crecimiento exponencial acotado por max_delay.
        delay = min(self.max_delay, self.base_delay * self.multiplier ** (attempt - 1))
        if self.jitter:
            # "Equal jitter": garantiza al menos la mitad de la espera y
            # reparte el resto aleatoriamente para desincronizar clientes.
            return random.uniform(delay / 2, delay)
        return delay


async def retry_async(
    operation: Callable[[], Awaitable[T]],
    policy: RetryPolicy,
    *,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    give_up_on: tuple[type[BaseException], ...] = (PermanentError,),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> T:
    """Ejecuta ``operation`` reintentando con backoff exponencial si falla.

    Args:
        operation: función async SIN argumentos a ejecutar (usar lambda o
            functools.partial para pasar argumentos).
        policy: parámetros de reintento.
        retry_on: excepciones que se consideran transitorias (se reintentan).
        give_up_on: excepciones que abortan inmediatamente (se relanzan tal cual).
        sleep: función de espera; inyectable para tests sin esperas reales.

    Returns:
        El resultado de ``operation`` en el primer intento exitoso.

    Raises:
        PermanentError: (o lo indicado en ``give_up_on``) sin reintentar.
        RetryExhaustedError: si fallan todos los intentos.
    """
    attempt = 0  # contador de intentos realizados
    while True:  # el bucle termina con return o raise
        attempt += 1
        try:
            return await operation()  # éxito -> devuelve el resultado
        except give_up_on:
            raise  # error permanente: no tiene sentido reintentar
        except retry_on as exc:
            if attempt >= policy.max_attempts:
                # Sin intentos restantes: se envuelve el último error.
                raise RetryExhaustedError(attempt, exc) from exc
            delay = policy.backoff(attempt)  # cuánto esperar
            logger.warning(
                "retrying_operation",
                extra={
                    "attempt": attempt,
                    "max_attempts": policy.max_attempts,
                    "retry_in_seconds": round(delay, 3),
                    "error": repr(exc),
                },
            )
            await sleep(delay)  # espera no bloqueante antes de reintentar
