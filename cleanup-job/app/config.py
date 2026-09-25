"""Configuración de cleanup-job (variables de entorno)."""

from __future__ import annotations

from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Parámetros de cleanup-job."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    service_name: str = "cleanup-job"
    log_level: str = "INFO"

    mongo_url: str  # obligatorio
    mongo_database: str = "cafe_notifications"
    mongo_collection: str = "notifications"

    retention_hours: float = Field(default=24, gt=0)  # antigüedad máxima permitida
    interval_seconds: int = Field(default=60, gt=0)  # cada cuánto se ejecuta (1 min)
    scheduler_enabled: bool = True  # False en tests / si se usa cron externo
    run_on_startup: bool = True  # ejecutar una vez al arrancar

    api_key: SecretStr | None = None

    @property
    def api_key_value(self) -> str | None:
        """API key en claro, o ``None`` si no está configurada."""
        if self.api_key is None:
            return None
        return self.api_key.get_secret_value() or None


@lru_cache
def get_settings() -> Settings:
    """Devuelve la configuración (cacheada)."""
    return Settings()  # type: ignore[call-arg]
