"""Autenticación opcional por API key (cabecera ``X-API-Key``).

Si la variable ``API_KEY`` del servicio está vacía, la autenticación queda
desactivada (útil en desarrollo); si tiene valor, las rutas protegidas exigen
la cabecera con ese mismo valor y responden ``401`` en caso contrario.
"""

from __future__ import annotations

import secrets  # compare_digest: comparación en tiempo constante

from fastapi import HTTPException, Request, Security, status
from fastapi.security import APIKeyHeader

# Esquema de seguridad: además de leer la cabecera, hace que Swagger (/docs)
# muestre el botón "Authorize" para introducir la clave.
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    request: Request,
    provided_key: str | None = Security(api_key_header),
) -> None:
    """Dependencia FastAPI que valida la API key de la petición.

    La clave esperada se lee de ``request.app.state.api_key`` (la fija cada
    servicio al crear la app), así la dependencia es reutilizable.

    Args:
        request: petición actual (para acceder a ``app.state``).
        provided_key: valor de la cabecera ``X-API-Key`` (o ``None``).

    Raises:
        HTTPException: 401 si la clave falta o no coincide.
    """
    expected_key: str | None = getattr(request.app.state, "api_key", None)
    if not expected_key:
        return  # autenticación desactivada
    # compare_digest evita ataques de temporización (timing attacks): tarda lo
    # mismo coincidan o no los primeros caracteres.
    if provided_key is None or not secrets.compare_digest(provided_key, expected_key):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API key no válida o ausente",
            headers={"WWW-Authenticate": "ApiKey"},
        )
