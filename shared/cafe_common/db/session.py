"""Fábricas de engine y sesiones asíncronas de SQLAlchemy (patrón *Factory*)."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

# Alias de tipo para la fábrica de sesiones que se inyecta en los servicios.
SessionFactory = async_sessionmaker[AsyncSession]


def create_engine(database_url: str, *, echo: bool = False) -> AsyncEngine:
    """Crea el engine asíncrono (pool de conexiones).

    Args:
        database_url: URL SQLAlchemy, p. ej.
            ``postgresql+asyncpg://user:pass@host:5432/db``.
        echo: si es True, imprime cada SQL ejecutado (depuración).

    Returns:
        ``AsyncEngine`` listo para usar.
    """
    # pool_pre_ping=True comprueba cada conexión antes de usarla: si PostgreSQL
    # se reinició, se descarta la conexión rota en vez de fallar la petición.
    return create_async_engine(database_url, echo=echo, pool_pre_ping=True)


def create_session_factory(engine: AsyncEngine) -> SessionFactory:
    """Crea la fábrica de sesiones ligada a ``engine``.

    ``expire_on_commit=False`` permite seguir leyendo atributos de los objetos
    tras el commit sin lanzar una nueva query (en async esa query implícita
    no está permitida).

    Args:
        engine: engine asíncrono.

    Returns:
        Fábrica que produce ``AsyncSession`` al invocarla.
    """
    return async_sessionmaker(engine, expire_on_commit=False)


async def ping_database(engine: AsyncEngine) -> None:
    """Health check: ejecuta ``SELECT 1`` (lanza excepción si la BD no responde)."""
    async with engine.connect() as connection:
        await connection.execute(text("SELECT 1"))
