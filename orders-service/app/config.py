"""Configuración del servicio leída de variables de entorno (12-factor app).

Las credenciales NO están en el código: ``DATABASE_URL`` y ``REDIS_URL`` son
obligatorias y las aporta docker-compose (a partir de ``.env`` o de valores
por defecto locales). Si faltan, el servicio falla al arrancar con un error
claro de validación (*fail fast*).
"""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Parámetros de configuración de orders-service.

    Cada atributo se rellena con la variable de entorno del mismo nombre en
    mayúsculas (p. ej. ``database_url`` <- ``DATABASE_URL``).
    """

    # extra="ignore": ignora otras variables del entorno que no nos interesan.
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "orders-service"  # campo "service" de los logs
    log_level: str = "INFO"

    database_url: str  # obligatorio, p. ej. postgresql+asyncpg://...
    redis_url: str  # obligatorio, p. ej. redis://redis:6379/0

    # SecretStr evita que la clave aparezca si se imprime la configuración.
    api_key: SecretStr | None = None

    outbox_relay_enabled: bool = True  # se desactiva en los tests
    outbox_poll_interval_seconds: float = Field(default=0.5, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    @property
    def api_key_value(self) -> str | None:
        """Devuelve la API key en claro, o ``None`` si está vacía/no definida."""
        if self.api_key is None:
            return None
        return self.api_key.get_secret_value() or None  # "" también desactiva


@lru_cache
def get_settings() -> Settings:
    """Devuelve la configuración (cacheada: se lee el entorno una sola vez)."""
    return Settings()  # type: ignore[call-arg]  # los valores vienen del entorno
