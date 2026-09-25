"""Fixtures de processor-service: BD SQLite real por test."""

from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import pytest
from sqlalchemy.ext.asyncio import AsyncEngine

from cafe_common.db.models import Base
from cafe_common.db.session import SessionFactory, create_engine, create_session_factory


@pytest.fixture
async def engine(tmp_path: Path) -> AsyncIterator[AsyncEngine]:
    """Engine SQLite con el esquema creado."""
    engine = create_engine(f"sqlite+aiosqlite:///{tmp_path / 'processor.db'}")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> SessionFactory:
    """Fábrica de sesiones ligada al engine de test."""
    return create_session_factory(engine)
