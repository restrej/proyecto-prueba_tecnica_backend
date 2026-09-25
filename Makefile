# =============================================================================
# Café Cloud — automatización local (build / test / deploy).
# Ejecuta `make help` para ver todos los comandos.
# En Windows sin make: usa scripts/tasks.ps1 (mismos comandos).
# =============================================================================

# Proyectos Python con tests unitarios.
PROJECTS := shared orders-service processor-service notifier-service cleanup-job
COMPOSE  := docker compose
API_KEY  ?= cafe-cloud-dev-key
ORDERS   ?= http://localhost:8001
NOTIFIER ?= http://localhost:8003
CLEANUP  ?= http://localhost:8004

.DEFAULT_GOAL := help
.PHONY: help env build up down restart logs ps migrate seed install test test-unit \
        test-unit-docker test-integration lint format demo clean

help: ## Muestra esta ayuda
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

env: ## Crea .env a partir de .env.example (si no existe)
	@test -f .env || cp .env.example .env

# ---------------------------------------------------------------- despliegue
build: ## Construye las imágenes Docker
	$(COMPOSE) build

up: ## Levanta todo el sistema (build incluido) en segundo plano
	$(COMPOSE) up --build -d
	@echo "orders:    $(ORDERS)/docs"
	@echo "processor: http://localhost:8002/health"
	@echo "notifier:  $(NOTIFIER)/docs"
	@echo "cleanup:   $(CLEANUP)/docs"

down: ## Detiene y elimina los contenedores (conserva los datos)
	$(COMPOSE) down

restart: down up ## Reinicia el sistema

logs: ## Sigue los logs JSON de los servicios de aplicación
	$(COMPOSE) logs -f orders-service processor-service notifier-service cleanup-job

ps: ## Estado de los contenedores
	$(COMPOSE) ps

migrate: ## Aplica las migraciones Alembic (alembic upgrade head)
	$(COMPOSE) run --rm migrate alembic upgrade head

seed: ## Inserta los datos de ejemplo (idempotente)
	$(COMPOSE) run --rm migrate python -m app.seed

# -------------------------------------------------------------------- tests
install: ## Instala dependencias locales con Poetry (necesita Python 3.11+ y Poetry)
	@for p in $(PROJECTS) tests; do echo "== $$p"; (cd $$p && poetry install) || exit 1; done

test: test-unit ## Alias de test-unit

test-unit: ## Tests unitarios de todos los proyectos (local, con Poetry)
	@for p in $(PROJECTS); do echo "== $$p"; (cd $$p && poetry run pytest -q) || exit 1; done

test-unit-docker: ## Tests unitarios dentro de Docker (no requiere Python local)
	@for s in orders-service processor-service notifier-service cleanup-job; do \
		echo "== $$s"; \
		docker build -q -f $$s/Dockerfile --target test -t cafe-cloud/$$s:test . && \
		docker run --rm cafe-cloud/$$s:test || exit 1; \
	done

test-integration: ## Test E2E contra el stack levantado (se ejecuta en Docker)
	$(COMPOSE) --profile test run --rm --build integration-tests

lint: ## Análisis estático con ruff
	@for p in $(PROJECTS); do echo "== $$p"; (cd $$p && poetry run ruff check .) || exit 1; done

format: ## Formatea el código con ruff
	@for p in $(PROJECTS); do (cd $$p && poetry run ruff format . && poetry run ruff check --fix .); done

# --------------------------------------------------------------------- demo
demo: ## Recorre el flujo completo con curl (requiere `make up`)
	@echo "1) POST /orders"
	@curl -s -X POST $(ORDERS)/orders \
		-H "Content-Type: application/json" -H "X-API-Key: $(API_KEY)" \
		-H "Idempotency-Key: demo-$$(date +%s)" \
		-d '{"customer_id":"abc123","items":[{"name":"latte","qty":1},{"name":"muffin","qty":2}]}'; echo
	@echo "2) Esperando la preparación (2-5 s)..."; sleep 7
	@echo "3) GET /notifications/abc123"
	@curl -s -H "X-API-Key: $(API_KEY)" $(NOTIFIER)/notifications/abc123; echo

clean: ## Detiene todo y BORRA volúmenes (datos) e imágenes locales
	$(COMPOSE) --profile test down -v --rmi local
