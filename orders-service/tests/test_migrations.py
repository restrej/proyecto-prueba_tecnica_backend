"""Test de las migraciones Alembic.

Aplica ``upgrade head`` sobre una BD vacía, comprueba que el esquema resultante
coincide con los modelos ORM (``alembic check``) y que ``downgrade`` funciona.
"""

from __future__ import annotations

from pathlib import Path

from alembic import command
from alembic.config import Config

SERVICE_DIR = Path(__file__).resolve().parents[1]  # carpeta orders-service/


def _alembic_config(database_url: str) -> Config:
    """Crea la configuración de Alembic apuntando a ``database_url``."""
    config = Config(str(SERVICE_DIR / "alembic.ini"))
    config.set_main_option("script_location", str(SERVICE_DIR / "alembic"))
    config.set_main_option("sqlalchemy.url", database_url)
    return config


def test_migrations_upgrade_match_models_and_downgrade(tmp_path: Path) -> None:
    """upgrade -> sin diferencias con los modelos -> downgrade."""
    config = _alembic_config(f"sqlite+aiosqlite:///{tmp_path / 'migrations.db'}")

    command.upgrade(config, "head")
    command.check(config)  # lanza excepción si modelos y migraciones difieren
    command.downgrade(config, "base")
