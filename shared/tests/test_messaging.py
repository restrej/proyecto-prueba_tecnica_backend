"""Tests del publicador/consumidor de Redis Streams usando ``fakeredis``.

``fakeredis`` implementa los comandos de Streams (XADD, XREADGROUP, XACK,
XAUTOCLAIM, XPENDING) en memoria, así que se prueba la lógica real del
consumidor sin necesitar un servidor Redis.
"""

from __future__ import annotations

import pytest
from fakeredis import FakeAsyncRedis

from cafe_common.events import EventEnvelope, dead_letter_stream
from cafe_common.messaging import ConsumerSettings, RedisStreamConsumer, RedisStreamPublisher
from cafe_common.retry import PermanentError, RetryPolicy

STREAM = "test.stream"


@pytest.fixture
async def redis() -> FakeAsyncRedis:
    """Cliente Redis en memoria, limpio en cada test."""
    client = FakeAsyncRedis(decode_responses=True)
    yield client
    await client.flushall()
    await client.aclose()


def _envelope(n: int = 1) -> EventEnvelope:
    """Crea un envelope de prueba."""
    return EventEnvelope(event_type=STREAM, producer="tests", trace_id=f"t{n}", payload={"n": n})


def _settings(**overrides: object) -> ConsumerSettings:
    """Configuración de consumidor rápida (sin esperas reales)."""
    base: dict[str, object] = {
        "stream": STREAM,
        "group": "g",
        "consumer_name": "c1",
        "block_ms": 10,
        "retry_policy": RetryPolicy(max_attempts=3, base_delay=0, jitter=False),
    }
    base.update(overrides)
    return ConsumerSettings(**base)  # type: ignore[arg-type]


async def test_published_event_is_consumed_and_acked(redis: FakeAsyncRedis) -> None:
    """Flujo feliz: publicar -> consumir -> handler recibe el envelope -> ACK."""
    received: list[EventEnvelope] = []

    async def handler(envelope: EventEnvelope) -> None:
        received.append(envelope)

    consumer = RedisStreamConsumer(redis, _settings(), handler)
    await consumer.ensure_group()
    sent = _envelope()
    await RedisStreamPublisher(redis).publish(STREAM, sent)

    assert await consumer.poll_once() == 1
    assert received == [sent]  # mismo event_id, trace_id y payload
    pending = await redis.xpending(STREAM, "g")
    assert pending["pending"] == 0  # se hizo XACK


async def test_transient_failures_are_retried(redis: FakeAsyncRedis) -> None:
    """Un handler que falla una vez se reintenta y termina con éxito."""
    attempts = 0

    async def flaky(_: EventEnvelope) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise ConnectionError("db hiccup")

    consumer = RedisStreamConsumer(redis, _settings(), flaky)
    await consumer.ensure_group()
    await RedisStreamPublisher(redis).publish(STREAM, _envelope())
    await consumer.poll_once()

    assert attempts == 2
    assert await redis.xlen(dead_letter_stream(STREAM)) == 0


async def test_permanent_failure_goes_to_dead_letter_and_hook_runs(
    redis: FakeAsyncRedis,
) -> None:
    """Un error permanente envía el mensaje a la DLQ, hace ACK y llama al hook."""
    hooked: list[str] = []

    async def broken(_: EventEnvelope) -> None:
        raise PermanentError("cannot process")

    async def on_dead_letter(envelope: EventEnvelope, _: BaseException) -> None:
        hooked.append(envelope.trace_id)

    consumer = RedisStreamConsumer(redis, _settings(), broken, on_dead_letter=on_dead_letter)
    await consumer.ensure_group()
    await RedisStreamPublisher(redis).publish(STREAM, _envelope(7))
    await consumer.poll_once()

    dlq = await redis.xrange(dead_letter_stream(STREAM))
    assert len(dlq) == 1
    assert "cannot process" in dlq[0][1]["error"]
    assert hooked == ["t7"]
    assert (await redis.xpending(STREAM, "g"))["pending"] == 0


async def test_malformed_message_goes_to_dead_letter(redis: FakeAsyncRedis) -> None:
    """Un mensaje sin envelope válido no llega al handler y va a la DLQ."""

    async def handler(_: EventEnvelope) -> None:  # pragma: no cover - no se llama
        raise AssertionError("handler must not be called")

    consumer = RedisStreamConsumer(redis, _settings(), handler)
    await consumer.ensure_group()
    await redis.xadd(STREAM, {"envelope": "{not json"})
    await consumer.poll_once()

    assert await redis.xlen(dead_letter_stream(STREAM)) == 1


async def test_orphan_message_is_reclaimed_by_another_consumer(redis: FakeAsyncRedis) -> None:
    """Un mensaje entregado a una réplica "muerta" (sin ACK) lo reclama otra."""
    received: list[EventEnvelope] = []

    async def handler(envelope: EventEnvelope) -> None:
        received.append(envelope)

    await RedisStreamPublisher(redis).publish(STREAM, _envelope())
    dead = RedisStreamConsumer(redis, _settings(consumer_name="dead"), handler)
    await dead.ensure_group()
    # La réplica "dead" lee el mensaje pero muere antes de procesarlo/ACK.
    await redis.xreadgroup("g", "dead", {STREAM: ">"}, count=1)

    alive = RedisStreamConsumer(redis, _settings(consumer_name="alive", claim_idle_ms=0), handler)
    assert await alive.poll_once() == 1
    assert len(received) == 1
    assert (await redis.xpending(STREAM, "g"))["pending"] == 0
