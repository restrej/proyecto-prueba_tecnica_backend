"""Mensajería asíncrona sobre Redis Streams.

¿Por qué Redis Streams?

* Es un log persistente (con AOF) con *consumer groups*: cada grupo recibe
  todos los mensajes y, dentro de un grupo, cada mensaje se entrega a UNA sola
  réplica -> escalado horizontal del consumidor sin cambiar código.
* Cada mensaje entregado queda en la *Pending Entries List* (PEL) hasta que el
  consumidor hace ``XACK``. Si el proceso muere antes del ACK, otra réplica lo
  reclama con ``XAUTOCLAIM`` -> semántica **at-least-once** (nunca se pierde,
  pero puede llegar repetido; por eso los consumidores son idempotentes).
* Es ligero: un único contenedor, ideal para un entorno local autocontenido.

Este módulo ofrece:

* :class:`RedisStreamPublisher` -> ``XADD`` de un :class:`EventEnvelope`.
* :class:`RedisStreamConsumer`  -> bucle ``XREADGROUP`` + reintentos + ``XACK``
  + reclamación de mensajes huérfanos + *dead letter queue*.
"""

from __future__ import annotations

import asyncio  # concurrencia cooperativa (gather, sleep, Event)
import contextlib  # suppress(): ignorar una excepción esperada
import logging  # logs estructurados
from collections.abc import Awaitable, Callable, Mapping  # tipos
from dataclasses import dataclass, field  # configuración inmutable
from typing import Any, Protocol  # Protocol = interfaz estructural

from pydantic import ValidationError  # error al parsear un envelope inválido
from redis.asyncio import Redis  # cliente asíncrono de Redis
from redis.exceptions import ConnectionError as RedisConnectionError  # Redis caído
from redis.exceptions import ResponseError  # error devuelto por el servidor
from redis.exceptions import TimeoutError as RedisTimeoutError  # timeout de red

from cafe_common.clock import utcnow  # para marcar cuándo falló un mensaje
from cafe_common.events import EventEnvelope, dead_letter_stream  # contrato
from cafe_common.metrics import EVENTS_CONSUMED_TOTAL, EVENTS_PUBLISHED_TOTAL
from cafe_common.retry import (  # reintentos con backoff
    PermanentError,
    RetryExhaustedError,
    RetryPolicy,
    retry_async,
)
from cafe_common.tracing import trace_context  # propaga trace_id al procesar

logger = logging.getLogger(__name__)

# Firma de un handler de eventos: recibe el envelope y no devuelve nada.
EventHandler = Callable[[EventEnvelope], Awaitable[None]]
# Hook opcional que se invoca cuando un mensaje acaba en la DLQ.
DeadLetterHook = Callable[[EventEnvelope, BaseException], Awaitable[None]]
# Un mensaje de Redis Streams: (id, {campo: valor}).
StreamMessage = tuple[str, dict[str, str]]


def encode_envelope(envelope: EventEnvelope) -> dict[str, str]:
    """Convierte un envelope en los campos planos de un mensaje de stream.

    Se duplican ``event_id``, ``event_type`` y ``trace_id`` como campos propios
    para poder inspeccionarlos con ``redis-cli XRANGE`` sin parsear JSON; el
    campo ``envelope`` contiene el evento completo y es la fuente de verdad.

    Args:
        envelope: evento a serializar.

    Returns:
        Diccionario ``str -> str`` apto para ``XADD``.
    """
    return {
        "event_id": str(envelope.event_id),
        "event_type": envelope.event_type,
        "trace_id": envelope.trace_id,
        "envelope": envelope.model_dump_json(),  # JSON completo del evento
    }


def decode_envelope(fields: Mapping[str, str]) -> EventEnvelope:
    """Reconstruye y valida un envelope a partir de los campos del mensaje.

    Args:
        fields: campos leídos del stream.

    Returns:
        El :class:`EventEnvelope` validado.

    Raises:
        KeyError: si falta el campo ``envelope``.
        pydantic.ValidationError: si el JSON no cumple el contrato.
    """
    return EventEnvelope.model_validate_json(fields["envelope"])


class EventPublisher(Protocol):
    """Interfaz de un publicador de eventos (permite fakes en los tests)."""

    async def publish(self, stream: str, envelope: EventEnvelope) -> str:
        """Publica ``envelope`` en ``stream`` y devuelve el id del mensaje."""
        ...


class RedisStreamPublisher:
    """Publica eventos en Redis Streams mediante ``XADD``."""

    def __init__(self, redis: Redis, *, maxlen: int = 100_000) -> None:
        """Crea el publicador.

        Args:
            redis: cliente Redis asíncrono (con ``decode_responses=True``).
            maxlen: longitud máxima aproximada del stream; evita que crezca sin
                límite en memoria (los más antiguos se recortan).
        """
        self._redis = redis
        self._maxlen = maxlen

    async def publish(self, stream: str, envelope: EventEnvelope) -> str:
        """Añade el evento al final del stream.

        Args:
            stream: nombre del stream destino.
            envelope: evento a publicar.

        Returns:
            El id asignado por Redis (p. ej. ``1727170000000-0``).
        """
        message_id = await self._redis.xadd(
            stream,
            encode_envelope(envelope),  # type: ignore[arg-type]
            maxlen=self._maxlen,
            approximate=True,  # "~": recorte eficiente, no exacto
        )
        EVENTS_PUBLISHED_TOTAL.labels(stream=stream).inc()  # métrica
        logger.info(
            "event_published",
            extra={
                "stream": stream,
                "event_id": str(envelope.event_id),
                "message_id": message_id,
                "trace_id": envelope.trace_id,
            },
        )
        return str(message_id)


@dataclass(frozen=True, slots=True)
class ConsumerSettings:
    """Configuración de un consumidor de stream.

    Attributes:
        stream: stream del que se lee.
        group: consumer group (uno por servicio consumidor).
        consumer_name: nombre único de la réplica dentro del grupo.
        batch_size: máximo de mensajes leídos por iteración.
        block_ms: cuánto bloquea ``XREADGROUP`` esperando mensajes nuevos.
        claim_idle_ms: tras cuántos ms sin ACK se considera huérfano un mensaje
            (su consumidor murió) y se reclama con ``XAUTOCLAIM``.
        max_deliveries: entregas máximas de un mismo mensaje antes de mandarlo
            a la DLQ (protege de "poison messages" que tumban el proceso).
        retry_policy: reintentos en proceso para errores transitorios.
    """

    stream: str
    group: str
    consumer_name: str
    batch_size: int = 10
    block_ms: int = 5_000
    claim_idle_ms: int = 60_000
    max_deliveries: int = 5
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)


def _extract_messages(response: Any) -> list[StreamMessage]:
    """Normaliza la respuesta de ``XREADGROUP`` a una lista de mensajes.

    Según la versión del protocolo (RESP2 o RESP3) redis-py devuelve una lista
    ``[[stream, [msgs]]]`` o un diccionario ``{stream: [msgs]}``; esta función
    acepta ambas formas para no depender de ese detalle.

    Args:
        response: respuesta cruda de redis-py.

    Returns:
        Lista de tuplas ``(message_id, fields)``.
    """
    if not response:  # timeout de BLOCK sin mensajes -> None o vacío
        return []
    entries = response.values() if isinstance(response, dict) else (r[1] for r in response)
    messages: list[StreamMessage] = []
    for batch in entries:
        # En RESP3 redis-py envuelve la lista de mensajes una vez más: [[msgs]].
        if len(batch) == 1 and isinstance(batch[0], list):
            batch = batch[0]
        # fields vacío/None = entrada borrada del stream: se ignora.
        messages.extend((str(mid), dict(fields)) for mid, fields in batch if fields)
    return messages


class RedisStreamConsumer:
    """Consumidor robusto de un stream con semántica at-least-once.

    Flujo de cada iteración:

    1. Reclama mensajes huérfanos (``XAUTOCLAIM``) de réplicas caídas.
    2. Si no hay huérfanos, lee mensajes nuevos (``XREADGROUP ... >``).
    3. Procesa el lote concurrentemente; cada mensaje:
       a. se decodifica (si es inválido -> DLQ, error permanente);
       b. se ejecuta el handler con reintentos y backoff exponencial;
       c. si tiene éxito -> ``XACK``; si agota reintentos -> DLQ + ``XACK``.
    """

    def __init__(
        self,
        redis: Redis,
        settings: ConsumerSettings,
        handler: EventHandler,
        *,
        on_dead_letter: DeadLetterHook | None = None,
    ) -> None:
        """Crea el consumidor.

        Args:
            redis: cliente Redis asíncrono.
            settings: parámetros del consumidor.
            handler: función de negocio que procesa cada evento. DEBE ser
                idempotente, ya que un mensaje puede llegar más de una vez.
            on_dead_letter: callback opcional cuando un evento válido termina
                en la DLQ (p. ej. para marcar el pedido como FAILED).
        """
        self._redis = redis
        self._settings = settings
        self._handler = handler
        self._on_dead_letter = on_dead_letter

    @property
    def settings(self) -> ConsumerSettings:
        """Configuración del consumidor (solo lectura)."""
        return self._settings

    async def ensure_group(self) -> None:
        """Crea el consumer group (y el stream) si todavía no existen.

        ``id="0"`` hace que un grupo nuevo lea también los mensajes publicados
        ANTES de que el consumidor arrancara (no se pierde nada al desplegar).
        """
        try:
            await self._redis.xgroup_create(
                name=self._settings.stream,
                groupname=self._settings.group,
                id="0",
                mkstream=True,  # crea el stream vacío si no existe
            )
            logger.info("consumer_group_created", extra=self._log_ctx())
        except ResponseError as exc:
            # BUSYGROUP = el grupo ya existe (arranque normal tras reinicio).
            if "BUSYGROUP" not in str(exc):
                raise

    async def run(self, stop_event: asyncio.Event) -> None:
        """Bucle principal: consume hasta que se active ``stop_event``.

        Los errores de conexión con Redis no matan el bucle: se espera con
        backoff exponencial y se vuelve a intentar (auto-recuperación).

        Args:
            stop_event: evento que el ciclo de vida activa al apagar.
        """
        failures = 0  # fallos de conexión consecutivos
        group_ready = False  # si ya se creó/verificó el consumer group
        logger.info("consumer_started", extra=self._log_ctx())
        while not stop_event.is_set():
            try:
                if not group_ready:
                    await self.ensure_group()
                    group_ready = True
                await self.poll_once()  # procesa un lote (o espera block_ms)
                failures = 0  # éxito -> reinicia el contador de fallos
            except (RedisConnectionError, RedisTimeoutError, OSError) as exc:
                failures += 1
                delay = min(30.0, 0.5 * 2 ** min(failures, 6))  # backoff acotado
                logger.warning(
                    "broker_unavailable",
                    extra={**self._log_ctx(), "error": repr(exc), "retry_in_seconds": delay},
                )
                await _sleep_or_stop(stop_event, delay)
            except Exception:
                # Error inesperado: se registra y se sigue (no tumba el servicio).
                logger.exception("consumer_loop_error", extra=self._log_ctx())
                await _sleep_or_stop(stop_event, 1.0)
        logger.info("consumer_stopped", extra=self._log_ctx())

    async def poll_once(self) -> int:
        """Ejecuta UNA iteración de consumo.

        Returns:
            Número de mensajes procesados en esta iteración.
        """
        messages = await self._claim_stale_messages()  # 1) huérfanos primero
        if not messages:
            response = await self._redis.xreadgroup(  # 2) mensajes nuevos
                groupname=self._settings.group,
                consumername=self._settings.consumer_name,
                streams={self._settings.stream: ">"},  # ">" = nunca entregados
                count=self._settings.batch_size,
                block=self._settings.block_ms,
            )
            messages = _extract_messages(response)
        if messages:
            # 3) procesa el lote en paralelo (cada mensaje es independiente).
            await asyncio.gather(*(self._handle_message(mid, f) for mid, f in messages))
        return len(messages)

    async def _claim_stale_messages(self) -> list[StreamMessage]:
        """Reclama mensajes entregados a otra réplica que nunca hizo ACK.

        Los que superan ``max_deliveries`` se envían directamente a la DLQ.

        Returns:
            Mensajes reclamados que deben procesarse.
        """
        result = await self._redis.xautoclaim(
            name=self._settings.stream,
            groupname=self._settings.group,
            consumername=self._settings.consumer_name,
            min_idle_time=self._settings.claim_idle_ms,
            start_id="0-0",
            count=self._settings.batch_size,
        )
        # result = [siguiente_cursor, [(id, campos), ...], [ids_borrados]]
        claimed = [(str(mid), dict(f)) for mid, f in result[1] if f]
        to_process: list[StreamMessage] = []
        for message_id, fields in claimed:
            deliveries = await self._delivery_count(message_id)
            if deliveries > self._settings.max_deliveries:
                await self._dead_letter(
                    message_id, fields, f"max deliveries exceeded ({deliveries})"
                )
            else:
                logger.warning(
                    "message_reclaimed",
                    extra={**self._log_ctx(), "message_id": message_id, "deliveries": deliveries},
                )
                to_process.append((message_id, fields))
        return to_process

    async def _delivery_count(self, message_id: str) -> int:
        """Devuelve cuántas veces se ha entregado ``message_id`` (``XPENDING``)."""
        pending = await self._redis.xpending_range(
            name=self._settings.stream,
            groupname=self._settings.group,
            min=message_id,
            max=message_id,
            count=1,
        )
        return int(pending[0]["times_delivered"]) if pending else 1

    async def _handle_message(self, message_id: str, fields: dict[str, str]) -> None:
        """Procesa un único mensaje: decodificar, ejecutar handler, ACK o DLQ.

        Args:
            message_id: id del mensaje en el stream.
            fields: campos del mensaje.
        """
        stream = self._settings.stream
        try:
            envelope = decode_envelope(fields)
        except (KeyError, ValueError, ValidationError) as exc:
            # Mensaje corrupto: reintentar nunca funcionará -> DLQ directo.
            await self._dead_letter(message_id, fields, f"malformed message: {exc!r}")
            return

        # Todo lo que se loguee dentro del with lleva el trace_id del evento.
        with trace_context(envelope.trace_id):
            try:
                await retry_async(
                    lambda: self._handler(envelope),  # operación a reintentar
                    self._settings.retry_policy,
                )
            except (PermanentError, RetryExhaustedError) as exc:
                await self._dead_letter(message_id, fields, repr(exc))
                if self._on_dead_letter is not None:
                    await self._safe_dead_letter_hook(envelope, exc)
                return
            except Exception:
                # Error inesperado fuera de la política (p. ej. cancelación):
                # NO se hace ACK -> el mensaje sigue pendiente y se reclamará.
                logger.exception(
                    "message_left_pending", extra={**self._log_ctx(), "message_id": message_id}
                )
                return
            await self._redis.xack(stream, self._settings.group, message_id)  # confirmado
            EVENTS_CONSUMED_TOTAL.labels(stream=stream, result="success").inc()
            logger.info(
                "event_consumed",
                extra={
                    **self._log_ctx(),
                    "message_id": message_id,
                    "event_id": str(envelope.event_id),
                },
            )

    async def _dead_letter(self, message_id: str, fields: Mapping[str, str], reason: str) -> None:
        """Copia el mensaje a la DLQ con el motivo del fallo y hace ``XACK``.

        Se publica en la DLQ ANTES del ACK: si el proceso muere entre ambos
        pasos, el mensaje se reprocesa (quizá duplicado en la DLQ), pero nunca
        se pierde.

        Args:
            message_id: id original del mensaje.
            fields: campos originales.
            reason: descripción del error.
        """
        dlq = dead_letter_stream(self._settings.stream)
        await self._redis.xadd(
            dlq,
            {  # type: ignore[arg-type]
                **fields,
                "original_message_id": message_id,
                "original_stream": self._settings.stream,
                "error": reason[:1000],  # se acota para no guardar trazas enormes
                "failed_at": utcnow().isoformat(),
            },
        )
        await self._redis.xack(self._settings.stream, self._settings.group, message_id)
        EVENTS_CONSUMED_TOTAL.labels(stream=self._settings.stream, result="dead_letter").inc()
        logger.error(
            "event_dead_lettered",
            extra={
                **self._log_ctx(),
                "message_id": message_id,
                "dead_letter_stream": dlq,
                "reason": reason,
                "trace_id": fields.get("trace_id"),
            },
        )

    async def _safe_dead_letter_hook(self, envelope: EventEnvelope, exc: BaseException) -> None:
        """Ejecuta el hook de DLQ sin dejar que un fallo en él rompa el bucle."""
        assert self._on_dead_letter is not None  # garantizado por el llamador
        try:
            await self._on_dead_letter(envelope, exc)
        except Exception:
            logger.exception("dead_letter_hook_failed", extra=self._log_ctx())

    def _log_ctx(self) -> dict[str, str]:
        """Campos comunes que se añaden a los logs del consumidor."""
        return {
            "stream": self._settings.stream,
            "group": self._settings.group,
            "consumer": self._settings.consumer_name,
        }


async def _sleep_or_stop(stop_event: asyncio.Event, seconds: float) -> None:
    """Duerme ``seconds`` pero despierta antes si se solicita la parada.

    Args:
        stop_event: evento de parada.
        seconds: tiempo máximo a esperar.
    """
    # Si vence el plazo sin petición de parada, se ignora el TimeoutError y se sigue.
    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(stop_event.wait(), timeout=seconds)
