"""Configuración de processor-service (variables de entorno)."""

from __future__ import annotations

import socket
from functools import lru_cache

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from cafe_common.retry import RetryPolicy


class Settings(BaseSettings):
    """Parámetros de processor-service."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "processor-service"
    log_level: str = "INFO"

    database_url: str  # obligatorio (misma BD de pedidos que orders-service)
    redis_url: str  # obligatorio

    # --- Consumidor de orders.created -------------------------------------
    consumer_group: str = "processor-service"  # un grupo por servicio
    # Nombre de la réplica: el hostname del contenedor es único por réplica.
    consumer_name: str = Field(default_factory=socket.gethostname)
    consumer_batch_size: int = Field(default=10, gt=0)
    consumer_block_ms: int = Field(default=5_000, gt=0)
    consumer_claim_idle_ms: int = Field(default=60_000, ge=0)
    consumer_max_deliveries: int = Field(default=5, gt=0)

    # --- Reintentos con backoff exponencial -------------------------------
    retry_max_attempts: int = Field(default=5, gt=0)
    retry_base_delay_seconds: float = Field(default=0.5, ge=0)
    retry_max_delay_seconds: float = Field(default=10.0, ge=0)

    # --- Simulación de la preparación -------------------------------------
    processing_min_seconds: float = Field(default=2.0, ge=0)
    processing_max_seconds: float = Field(default=5.0, ge=0)
    # Probabilidad (0..1) de fallo transitorio simulado para demostrar reintentos.
    processing_failure_rate: float = Field(default=0.0, ge=0, le=1)

    # --- Outbox (publicación de orders.completed) --------------------------
    outbox_relay_enabled: bool = True
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    @model_validator(mode="after")
    def _check_processing_range(self) -> Settings:
        """Valida que el rango de simulación sea coherente (min <= max)."""
        if self.processing_min_seconds > self.processing_max_seconds:
            raise ValueError("PROCESSING_MIN_SECONDS must be <= PROCESSING_MAX_SECONDS")
        return self

    def retry_policy(self) -> RetryPolicy:
        """Construye la política de reintentos a partir de la configuración."""
        return RetryPolicy(
            max_attempts=self.retry_max_attempts,
            base_delay=self.retry_base_delay_seconds,
            max_delay=self.retry_max_delay_seconds,
        )


@lru_cache
def get_settings() -> Settings:
    """Devuelve la configuración (cacheada)."""
    return Settings()  # type: ignore[call-arg]
