"""Fixtures de pytest para orders-service.

Se usa SQLite (archivo temporal, driver ``aiosqlite``) como base de datos REAL
y efímera: las pruebas ejercitan SQL, transacciones y constraints de verdad
sin necesitar PostgreSQL. El relay del outbox se desactiva (no hay Redis) y
se verifica directamente el contenido de la tabla ``outbox_events``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import Settings
from app.main import create_app
from cafe_common.db.models import Base
from cafe_common.db.session import SessionFactory, create_engine, create_session_factory

API_KEY = "test-api-key"


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncIterator[AsyncEngine]:
    """Engine sobre una BD SQLite nueva por test, con el esquema creado."""
    engine = create_engine(f"sqlite+aiosqlite:///{tmp_path / 'orders.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)  # crea todas las tablas
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> SessionFactory:
    """Fábrica de sesiones ligada al engine de test."""
    return create_session_factory(engine)


@pytest.fixture
async def client(session_factory: SessionFactory) -> AsyncIterator[AsyncClient]:
    """Cliente HTTP en proceso contra la app (sin red ni servidor)."""
    settings = Settings(
        database_url="sqlite+aiosqlite://",  # no se usa: se inyecta session_factory
        redis_url="redis://unused:6379/0",
        api_key=API_KEY,  # type: ignore[arg-type]
        outbox_relay_enabled=False,
    )
    app = create_app(settings)
    # Sin lifespan: se inyecta directamente la dependencia que usarían las rutas.
    app.state.session_factory = session_factory
    transport = ASGITransport(app=app)
    async with AsyncClient(
        transport=transport, base_url="http://test", headers={"X-API-Key": API_KEY}
    ) as http:
        yield http
