# syntax=docker/dockerfile:1
# Imagen que ejecuta los tests de integración DENTRO de la red de docker
# compose (perfil "test"): no requiere Python ni Poetry en el host.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    POETRY_VERSION=2.5.1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

RUN pip install "poetry==${POETRY_VERSION}"
WORKDIR /srv/tests
COPY tests/pyproject.toml tests/poetry.lock ./
RUN poetry install
COPY tests/ ./
CMD ["pytest", "-v"]
