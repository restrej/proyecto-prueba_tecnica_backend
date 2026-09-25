"""Tests unitarios de la política de reintentos con backoff exponencial."""

from __future__ import annotations

import pytest

from cafe_common.retry import PermanentError, RetryExhaustedError, RetryPolicy, retry_async


class _FakeSleep:
    """Sustituto de ``asyncio.sleep`` que registra las esperas sin dormir."""

    def __init__(self) -> None:
        """Inicializa la lista de esperas solicitadas."""
        self.delays: list[float] = []

    async def __call__(self, seconds: float) -> None:
        """Guarda la espera pedida y vuelve inmediatamente."""
        self.delays.append(seconds)


def test_backoff_grows_exponentially_and_is_capped() -> None:
    """Sin jitter, las esperas son 0.5, 1, 2, 4 ... hasta el tope."""
    policy = RetryPolicy(base_delay=0.5, multiplier=2, max_delay=3, jitter=False)
    assert [policy.backoff(n) for n in range(1, 6)] == [0.5, 1, 2, 3, 3]


def test_backoff_with_jitter_stays_within_bounds() -> None:
    """Con jitter la espera queda en [espera/2, espera]."""
    policy = RetryPolicy(base_delay=1, multiplier=2, max_delay=100, jitter=True)
    for _ in range(50):
        assert 2 <= policy.backoff(3) <= 4  # espera nominal del 3er fallo = 4 s


async def test_retry_succeeds_after_transient_failures() -> None:
    """Falla dos veces y a la tercera funciona; se esperan 0.5 s y 1 s."""
    calls = 0
    sleep = _FakeSleep()

    async def flaky() -> str:
        """Operación que falla dos veces con un error transitorio y acierta a la tercera."""
        nonlocal calls
        calls += 1
        if calls < 3:
            raise ConnectionError("transient")
        return "ok"

    policy = RetryPolicy(max_attempts=5, base_delay=0.5, jitter=False)
    assert await retry_async(flaky, policy, sleep=sleep) == "ok"
    assert calls == 3
    assert sleep.delays == [0.5, 1.0]


async def test_retry_gives_up_after_max_attempts() -> None:
    """Si siempre falla, lanza RetryExhaustedError tras max_attempts."""
    sleep = _FakeSleep()

    async def always_fails() -> None:
        """Operación que siempre falla con un error transitorio."""
        raise ConnectionError("down")

    with pytest.raises(RetryExhaustedError) as info:
        await retry_async(always_fails, RetryPolicy(max_attempts=3, jitter=False), sleep=sleep)
    assert info.value.attempts == 3
    assert len(sleep.delays) == 2  # se espera entre intentos, no tras el último


async def test_permanent_error_is_not_retried() -> None:
    """Un PermanentError se propaga en el primer intento, sin esperas."""
    sleep = _FakeSleep()

    async def invalid() -> None:
        """Operación que falla con un error permanente (no debe reintentarse)."""
        raise PermanentError("bad payload")

    with pytest.raises(PermanentError):
        await retry_async(invalid, RetryPolicy(), sleep=sleep)
    assert sleep.delays == []


def test_invalid_policy_is_rejected() -> None:
    """La política valida sus parámetros al construirse."""
    with pytest.raises(ValueError):
        RetryPolicy(max_attempts=0)
