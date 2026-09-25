"""Entorno de ejecución de Alembic (versión asíncrona).

Alembic ejecuta este archivo en cada comando (``upgrade``, ``downgrade``,
``revision --autogenerate``...). Aquí se indica:

* A qué base de datos conectarse (``DATABASE_URL`` vía ``Settings``).
* Qué metadatos representan el esquema deseado (``Base.metadata``), para que
  ``--autogenerate`` pueda comparar modelo vs. base de datos.
"""

from __future__ import annotations

import asyncio
from logging.config import fileConfig

from alembic import context
from sqlalchemy.engine import Connection

from app.config import get_settings
from cafe_common.db.models import Base
from cafe_common.db.session import create_engine

config = context.config  # objeto con los valores de alembic.ini

# Configura el logging de Alembic según alembic.ini (si se lanzó por CLI).
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# Metadatos objetivo: el esquema definido por los modelos ORM compartidos.
target_metadata = Base.metadata


def _database_url() -> str:
    """URL de la BD: la de ``-x``/config si se pasó, si no la del entorno."""
    return config.get_main_option("sqlalchemy.url") or get_settings().database_url


def run_migrations_offline() -> None:
    """Modo offline: genera el SQL por pantalla sin conectarse a la BD.

    Uso: ``alembic upgrade head --sql`` (útil para revisar el DDL).
    """
    context.configure(
        url=_database_url(),
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        compare_type=True,
    )
    with context.begin_transaction():
        context.run_migrations()


def _run_sync_migrations(connection: Connection) -> None:
    """Ejecuta las migraciones sobre una conexión síncrona (dentro de run_sync)."""
    context.configure(connection=connection, target_metadata=target_metadata, compare_type=True)
    with context.begin_transaction():  # cada upgrade se aplica en una transacción
        context.run_migrations()


async def run_migrations_online() -> None:
    """Modo online: se conecta con el driver asíncrono y aplica las migraciones."""
    engine = create_engine(_database_url())
    async with engine.connect() as connection:
        # Alembic es síncrono internamente; run_sync lo ejecuta sobre la
        # conexión asíncrona sin bloquear el event loop.
        await connection.run_sync(_run_sync_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(run_migrations_online())
