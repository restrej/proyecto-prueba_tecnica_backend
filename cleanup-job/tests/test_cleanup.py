"""Tests de cleanup-job: regla de las 24 horas y endpoint manual."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from httpx import ASGITransport, AsyncClient

from app.config import Settings
from app.main import create_app
from app.service import CleanupService

NOW = datetime(2026, 9, 24, 12, 0, tzinfo=UTC)


class InMemoryCleanupRepository:
    """Fake que guarda fechas de creación y aplica el mismo filtro que Mongo."""

    def __init__(self, created_at: list[datetime]) -> None:
        """Inicializa con las fechas de las notificaciones existentes."""
        self.created_at = list(created_at)
        self.last_cutoff: datetime | None = None

    async def delete_older_than(self, cutoff: datetime) -> int:
        """Elimina las fechas < cutoff y devuelve cuántas eliminó."""
        self.last_cutoff = cutoff
        before = len(self.created_at)
        self.created_at = [d for d in self.created_at if d >= cutoff]
        return before - len(self.created_at)


async def test_deletes_only_notifications_older_than_24_hours() -> None:
    """Se borran las de hace 25 h y 48 h; se conservan las de 1 h y 23 h."""
    repo = InMemoryCleanupRepository([NOW - timedelta(hours=h) for h in (1, 23, 25, 48)])
    result = await CleanupService(repo, clock=lambda: NOW).run()

    assert result.deleted == 2
    assert result.cutoff == NOW - timedelta(hours=24)
    assert repo.created_at == [NOW - timedelta(hours=1), NOW - timedelta(hours=23)]


async def test_manual_endpoint_runs_cleanup() -> None:
    """POST /jobs/cleanup/run ejecuta la limpieza y devuelve el resumen."""
    settings = Settings(mongo_url="mongodb://unused", api_key="k")  # type: ignore[arg-type]
    app = create_app(settings)
    repo = InMemoryCleanupRepository([NOW - timedelta(days=2)])
    app.state.cleanup_service = CleanupService(repo, clock=lambda: NOW)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://t") as client:
        denied = await client.post("/jobs/cleanup/run")
        response = await client.post("/jobs/cleanup/run", headers={"X-API-Key": "k"})

    assert denied.status_code == 401
    assert response.status_code == 200
    assert response.json()["deleted"] == 1
