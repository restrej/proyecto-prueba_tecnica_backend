"""Configuración de notifier-service (variables de entorno)."""

from __future__ import annotations

import socket
from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

from cafe_common.retry import RetryPolicy


class Settings(BaseSettings):
    """Parámetros de notifier-service."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "notifier-service"
    log_level: str = "INFO"

    redis_url: str  # obligatorio
    mongo_url: str  # obligatorio, p. ej. mongodb://user:pass@mongo:27017/?authSource=admin
    mongo_database: str = "cafe_notifications"
    mongo_collection: str = "notifications"

    api_key: SecretStr | None = None

    consumer_enabled: bool = True
    consumer_group: str = "notifier-service"
    consumer_name: str = Field(default_factory=socket.gethostname)
    consumer_batch_size: int = Field(default=10, gt=0)
    consumer_block_ms: int = Field(default=5_000, gt=0)
    consumer_claim_idle_ms: int = Field(default=60_000, ge=0)
    consumer_max_deliveries: int = Field(default=5, gt=0)

    retry_max_attempts: int = Field(default=5, gt=0)
    retry_base_delay_seconds: float = Field(default=0.5, ge=0)
    retry_max_delay_seconds: float = Field(default=10.0, ge=0)

    @property
    def api_key_value(self) -> str | None:
        """API key en claro, o ``None`` si no está configurada."""
        if self.api_key is None:
            return None
        return self.api_key.get_secret_value() or None

    def retry_policy(self) -> RetryPolicy:
        """Política de reintentos construida desde la configuración."""
        return RetryPolicy(
            max_attempts=self.retry_max_attempts,
            base_delay=self.retry_base_delay_seconds,
            max_delay=self.retry_max_delay_seconds,
        )


@lru_cache
def get_settings() -> Settings:
    """Devuelve la configuración (cacheada)."""
    return Settings()  # type: ignore[call-arg]
