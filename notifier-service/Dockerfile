# syntax=docker/dockerfile:1
# -----------------------------------------------------------------------------
# Imagen de notifier-service. Contexto de build = raíz del repositorio (para poder
# copiar la librería compartida ../shared). Build multi-stage:
#   base    -> Python + Poetry + dependencias de producción
#   test    -> + dependencias de desarrollo + tests  (docker build --target test)
#   runtime -> imagen final, usuario sin privilegios (la usa docker-compose)
# -----------------------------------------------------------------------------
FROM python:3.12-slim AS base

# PYTHONDONTWRITEBYTECODE: no genera .pyc; PYTHONUNBUFFERED: logs al instante.
# POETRY_VIRTUALENVS_CREATE=false: instala en el Python del sistema del
# contenedor (el contenedor ya es el aislamiento; un venv sería redundante).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    POETRY_VERSION=2.5.1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

RUN pip install "poetry==${POETRY_VERSION}"

# Misma estructura relativa que en el repo: /srv/shared y /srv/<servicio>,
# así la ruta "../shared" del pyproject/lock funciona igual dentro de Docker.
COPY shared/ /srv/shared/
WORKDIR /srv/notifier-service

# Se copian primero SOLO los manifiestos: si el código cambia pero las
# dependencias no, Docker reutiliza esta capa cacheada (builds rápidos).
COPY notifier-service/pyproject.toml notifier-service/poetry.lock ./
RUN poetry install --only main

# ---------------------------------------------------------------- test stage
FROM base AS test
RUN poetry install --with dev
COPY notifier-service/ ./
CMD ["pytest", "-q"]

# ------------------------------------------------------------- runtime stage
FROM base AS runtime
COPY notifier-service/ ./
# Usuario sin privilegios: si alguien compromete la app, no es root.
RUN useradd --system --uid 1001 --no-create-home app
USER app
EXPOSE 8000
# --factory: uvicorn llama a create_app() para construir la app.
# --no-access-log: el middleware ya registra cada petición en JSON.
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
