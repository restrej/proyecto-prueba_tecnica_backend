# ☕ Café Cloud — Sistema distribuido de pedidos

Sistema de microservicios en **Python 3.12 + FastAPI** que procesa pedidos online de una
cafetería mediante **comunicación asíncrona por eventos**, **PostgreSQL**, **Redis Streams**,
**MongoDB** y una **tarea programada**. Todo se levanta en local con un único comando.

```bash
git clone <repo> cafe-cloud && cd cafe-cloud
docker compose up --build
```

> No hace falta crear ningún archivo ni tener Python instalado: todas las credenciales y
> parámetros tienen valores **locales** por defecto en `docker-compose.yml` (puedes
> sobrescribirlos copiando `.env.example` a `.env`).

---

## Índice

1. [Arquitectura](#1-arquitectura)
2. [Puesta en marcha](#2-puesta-en-marcha)
3. [Probar el flujo completo con curl](#3-probar-el-flujo-completo-con-curl)
4. [Tests](#4-tests)
5. [Decisiones técnicas](#5-decisiones-técnicas)
6. [Contrato de eventos (payloads)](#6-contrato-de-eventos-payloads)
7. [Observabilidad](#7-observabilidad)
8. [Configuración](#8-configuración)
9. [Estructura del repositorio](#9-estructura-del-repositorio)
10. [Resolución de problemas](#10-resolución-de-problemas)
11. [Cumplimiento de requisitos de la prueba](#11-cumplimiento-de-requisitos-de-la-prueba)

---

## 1. Arquitectura

### Flujo de eventos

```
                 POST /orders  (Idempotency-Key, X-API-Key)
 Cliente ───────────────────────────────►┌────────────────────┐
    │                                    │   orders-service   │ :8001
    │                                    └─────────┬──────────┘
    │               UNA transacción SQL:           │
    │         orders + order_items + outbox_events │
    │                   + idempotency_keys         ▼
    │                                    ┌────────────────────┐
    │                                    │     PostgreSQL     │◄──────────────┐
    │                                    │  (orders, outbox,  │               │
    │                                    │  processed_events) │               │
    │                                    └─────────┬──────────┘               │
    │                       Outbox relay (polling, │ FOR UPDATE SKIP LOCKED)  │
    │                                              ▼                          │
    │                         ┌─────────────────────────────────────┐        │
    │                         │  Redis Streams  ·  orders.created    │        │
    │                         └──────────────────┬──────────────────┘        │
    │                   XREADGROUP (consumer group "processor-service")      │
    │                                            ▼                            │
    │                                 ┌────────────────────┐   UPDATE status  │
    │                                 │ processor-service  │───COMPLETED + ───┘
    │                                 │  sleep 2-5 s,      │   outbox + inbox
    │                                 │  retry + backoff   │   (1 transacción)
    │                                 └─────────┬──────────┘
    │                                           │ Outbox relay
    │                                           ▼
    │                         ┌─────────────────────────────────────┐
    │                         │ Redis Streams  ·  orders.completed   │
    │                         └──────────────────┬──────────────────┘
    │                   XREADGROUP (consumer group "notifier-service")
    │                                            ▼
    │                                 ┌────────────────────┐  insert _id=event_id
    │                                 │  notifier-service  │───────────────┐
    │                                 └─────────┬──────────┘               ▼
    │   GET /notifications/{customer_id}        │               ┌────────────────┐
    └──────────────────────────────────────────►│               │    MongoDB     │
                                     :8003                     │ notifications  │
                                                               └───────▲────────┘
                               ┌────────────────────┐  deleteMany        │
                               │    cleanup-job     │  created_at < now-24h
                               │ APScheduler (1 min)│─────────────────────┘
                               └────────────────────┘  :8004 POST /jobs/cleanup/run

  Mensajes fallidos tras reintentos / malformados ──► orders.created.dlq / orders.completed.dlq
```

### Servicios

| Servicio | Puerto host | Responsabilidad | Persistencia |
|---|---|---|---|
| `orders-service` | 8001 | `POST /orders` idempotente, `GET /orders/{id}`, publica `orders.created` | PostgreSQL |
| `processor-service` | 8002 | Consume `orders.created`, simula la preparación, marca `COMPLETED`, publica `orders.completed` | PostgreSQL |
| `notifier-service` | 8003 | Consume `orders.completed`, guarda la notificación, `GET /notifications/{customer_id}` | MongoDB |
| `cleanup-job` | 8004 | Cada minuto borra notificaciones con más de 24 h; `POST /jobs/cleanup/run` | MongoDB |
| `migrate` | — | Contenedor de un solo uso: `alembic upgrade head` + datos semilla | PostgreSQL |
| `postgres` / `redis` / `mongo` | 5432 / 6379 / 27017 | Infraestructura | volúmenes Docker |

Todos los servicios exponen `GET /health`, `GET /health/live`, `GET /metrics` y la
documentación interactiva **Swagger** en `/docs`.

### Estados de un pedido

```
PENDING ──(processor OK)──► COMPLETED
   │
   └──(reintentos agotados → DLQ)──► FAILED
```

---

## 2. Puesta en marcha

### Requisitos

* Docker con Docker Compose v2 (`docker compose version`).
* *(Opcional, solo para tests unitarios locales)* Python 3.11+ y Poetry 2.x.

### Levantar el entorno

```bash
docker compose up --build        # en primer plano, con logs
# o bien
make up                          # en segundo plano (Windows: .\scripts\tasks.ps1 up)
```

Orden de arranque (lo gestiona Compose con `depends_on` + `healthcheck`):

1. `postgres`, `redis` y `mongo` pasan a *healthy*.
2. `migrate` aplica `alembic upgrade head` e inserta los **datos semilla**, y termina.
3. Arrancan `orders-service`, `processor-service`, `notifier-service` y `cleanup-job`.

Comprueba el estado con `docker compose ps` (todos deben aparecer como `healthy`).

### Migraciones y datos semilla

Las migraciones están en `orders-service/alembic/versions/`. Se aplican automáticamente, pero
también se pueden ejecutar a mano:

```bash
make migrate                                   # docker compose run --rm migrate alembic upgrade head
make seed                                      # docker compose run --rm migrate python -m app.seed

# o en local (con PostgreSQL accesible y DATABASE_URL/REDIS_URL definidas):
cd orders-service && poetry run alembic upgrade head && poetry run python -m app.seed
```

El *seed* es **idempotente** (ids fijos) e inserta, para el cliente `seed-customer`:

* un pedido `COMPLETED` histórico, y
* un pedido `PENDING` con su evento en el outbox, que **recorre todo el flujo al arrancar**:
  a los pocos segundos `GET /notifications/seed-customer` ya devuelve una notificación.

### Parar

```bash
docker compose down        # conserva los datos (volúmenes)
make clean                 # borra también volúmenes e imágenes locales
```

---

## 3. Probar el flujo completo con curl

La API key por defecto es `cafe-cloud-dev-key` (cabecera `X-API-Key`).

**1. Crear un pedido**

```bash
curl -i -X POST http://localhost:8001/orders \
  -H "Content-Type: application/json" \
  -H "X-API-Key: cafe-cloud-dev-key" \
  -H "Idempotency-Key: 7f1d3c0e-0b6a-4a51-9d7e-1b2c3d4e5f60" \
  -d '{"customer_id":"abc123","items":[{"name":"latte","qty":1},{"name":"muffin","qty":2}]}'
```

```http
HTTP/1.1 201 Created
x-trace-id: 07f6e728903e4f46a0441713b336e84b

{"order_id":"ba7549ca-c576-4912-9def-c78894cef9e3","status":"PENDING","created_at":"2026-09-24T10:15:31.473986Z"}
```

**2. Repetir la MISMA petición (reintento del cliente)**: misma respuesta, `200 OK`,
cabecera `Idempotent-Replayed: true` y **ningún pedido nuevo**. Si se reutiliza la clave con
un cuerpo distinto, se devuelve `422`.

**3. Ver el pedido pasar de `PENDING` a `COMPLETED`** (2-5 s después):

```bash
curl -s -H "X-API-Key: cafe-cloud-dev-key" http://localhost:8001/orders/<order_id>
```

**4. Consultar las notificaciones del cliente**

```bash
curl -s -H "X-API-Key: cafe-cloud-dev-key" http://localhost:8003/notifications/abc123
```

```json
[
  {
    "id": "6a0e2f7c-4b1d-4c55-9a55-2f0f8a3b9d11",
    "order_id": "ba7549ca-c576-4912-9def-c78894cef9e3",
    "customer_id": "abc123",
    "message": "Tu pedido ba7549ca-c576-4912-9def-c78894cef9e3 está listo: 1x latte, 2x muffin. ¡Que lo disfrutes! ☕",
    "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
    "trace_id": "07f6e728903e4f46a0441713b336e84b",
    "created_at": "2026-09-24T10:15:34.512000Z"
  }
]
```

Fíjate en que el `trace_id` es el mismo que devolvió `POST /orders`: la traza ha cruzado los
tres servicios.

**5. Ejecutar la limpieza manualmente**

```bash
curl -s -X POST -H "X-API-Key: cafe-cloud-dev-key" http://localhost:8004/jobs/cleanup/run
# {"deleted":0,"cutoff":"2026-09-23T10:16:00Z","ran_at":"...","duration_ms":1.8,"trace_id":"..."}
```

**Atajo:** `make demo` (o `.\scripts\tasks.ps1 demo`) ejecuta los pasos 1 y 4.

**Otros endpoints útiles**

```bash
curl -s http://localhost:8001/health      # readiness: {"status":"ok","checks":{"database":"ok","redis":"ok"}}
curl -s http://localhost:8002/metrics     # métricas Prometheus
open http://localhost:8001/docs           # Swagger UI
```

**Ver los reintentos con backoff en acción**: arranca con un 30 % de fallos simulados y
observa los logs `retrying_operation` del procesador:

```bash
PROCESSING_FAILURE_RATE=0.3 docker compose up -d processor-service
docker compose logs -f processor-service
```

**Inspeccionar el broker**:

```bash
docker compose exec redis redis-cli XRANGE orders.created - +
docker compose exec redis redis-cli XINFO GROUPS orders.created
docker compose exec redis redis-cli XRANGE orders.created.dlq - +     # dead letters
```

---

## 4. Tests

| Tipo | Dónde | Qué cubre |
|---|---|---|
| Unitarios | `shared/tests` | backoff exponencial, reintentos, logs JSON, métricas, consumidor de Redis Streams (ACK, DLQ, reclamación de huérfanos) con `fakeredis` |
| Unitarios + API | `orders-service/tests` | validación, idempotencia (repetición, concurrencia, reutilización de clave), outbox, API key, 404, **migraciones Alembic** (`upgrade` → `alembic check` → `downgrade`) |
| Unitarios | `processor-service/tests` | COMPLETED + evento en outbox, consumidor idempotente, errores permanentes/transitorios, hook de DLQ → `FAILED` |
| Unitarios + API | `notifier-service/tests` | creación de la notificación, duplicados, `GET /notifications/{customer_id}`, API key |
| Unitarios + API | `cleanup-job/tests` | regla de las 24 h, `POST /jobs/cleanup/run` |
| **Integración E2E** | `tests/integration` | `POST /orders` → cola → processor → notifier → `GET /notifications/{customer_id}` + idempotencia + propagación del `trace_id` |

Los tests de servicio usan **bases de datos reales y efímeras** (SQLite en un archivo temporal)
y dobles en memoria solo donde hace falta (MongoDB, Redis).

### Ejecutar

```bash
# Unitarios, en local (Python 3.11+ y Poetry):
make install
make test-unit                  # = cd <proyecto> && poetry run pytest -q, para cada proyecto

# Unitarios, sin Python local (dentro de Docker, stage "test" de cada Dockerfile):
make test-unit-docker

# Integración end-to-end (con el sistema levantado):
docker compose up --build -d
make test-integration           # = docker compose --profile test run --rm integration-tests

# o desde el host:
cd tests && poetry install && poetry run pytest -v
```

Un proyecto concreto: `cd orders-service && poetry run pytest -q`.

---

## 5. Decisiones técnicas

### Broker: Redis Streams

* **Consumer groups**: cada servicio consumidor es un grupo; dentro del grupo cada mensaje se
  entrega a **una** réplica → escalado horizontal sin cambiar código
  (`docker compose up --scale processor-service=3`, quitando antes su mapeo de puerto
  `8002:8000`, ya que varias réplicas no pueden publicar el mismo puerto del host).
* **At-least-once**: un mensaje queda en la *Pending Entries List* hasta el `XACK`. Si una
  réplica muere sin confirmar, otra lo reclama con `XAUTOCLAIM` pasados `CONSUMER_CLAIM_IDLE_MS`.
* **Persistencia** con AOF (`--appendonly yes`).
* Frente a RabbitMQ/Kafka: un único contenedor ligero, arranque en segundos y suficiente para
  este caso. La abstracción `EventPublisher` / `RedisStreamConsumer` permite cambiarlo.

### Base de datos SQL: PostgreSQL (+ Alembic)

Transacciones ACID (imprescindibles para el outbox), `SELECT … FOR UPDATE SKIP LOCKED`
para el relay, `JSONB`, índices parciales y constraints `CHECK`. El esquema lo gestiona
Alembic (`orders-service/alembic`), y un test verifica que migraciones y modelos no divergen.

Tablas: `orders (id, customer_id, status, created_at, updated_at, completed_at)`,
`order_items`, `idempotency_keys`, `outbox_events` y `processed_events`.

> **Base de datos compartida.** La prueba pide que `processor-service` actualice el pedido en la
> BD. Por eso ambos servicios comparten la BD de pedidos: el **esquema** vive en la librería
> compartida `cafe_common.db.models` (una sola fuente de verdad) y **solo `orders-service` ejecuta
> las migraciones**. En un sistema más grande, `processor-service` tendría su propia BD y el
> estado del pedido se actualizaría reaccionando a `orders.completed` (*database per service*).

### NoSQL: MongoDB

Documentos de notificación sin esquema rígido, driver asíncrono oficial (`pymongo.AsyncMongoClient`),
índice `(customer_id, created_at desc)` para el `GET` e índice `created_at` para la limpieza.

### Idempotencia de `POST /orders` (cabecera `Idempotency-Key`)

1. Se calcula un **SHA-256 canónico** del cuerpo (claves ordenadas).
2. En la **misma transacción** que crea el pedido se inserta `idempotency_keys(key PK,
   request_hash, order_id, response_body)`.
3. Repetición con la misma clave y el mismo cuerpo → se devuelve la respuesta guardada
   (`200` + `Idempotent-Replayed: true`).
4. Misma clave, cuerpo distinto → `422` (siguiendo el borrador IETF *Idempotency-Key*).
5. **Concurrencia**: si llegan dos peticiones simultáneas con la misma clave, la PK hace que la
   segunda falle con `IntegrityError`; se captura y se devuelve el resultado de la primera. Hay
   un test que lanza 5 peticiones en paralelo y verifica que se crea un solo pedido.
6. Sin cabecera → `400`.

### Consistencia BD ↔ mensajería: Transactional Outbox

Escribir en la BD **y** publicar en el broker son dos sistemas distintos (*dual write*): si el
proceso cae entre ambos, habría pedidos sin evento. Solución:

1. El evento se inserta en `outbox_events` **en la misma transacción** que el cambio de negocio.
2. Un *relay* en segundo plano (`OutboxRelay`) lee las filas pendientes con
   `FOR UPDATE SKIP LOCKED` (seguro con varias réplicas), las publica y marca `published_at`.
3. Si el relay publica y cae antes de marcar la fila, la vuelve a publicar **con el mismo
   `event_id`**, y los consumidores lo deduplican.

Cada servicio solo publica los streams que le pertenecen (orders → `orders.created`,
processor → `orders.completed`). El procesador aplica el mismo patrón: `UPDATE` del pedido,
inserción en `processed_events` y evento `orders.completed` en **una** transacción.

### Consumidores idempotentes (at-least-once)

* **processor-service**: tabla *inbox* `processed_events (event_id, consumer)` + **bloqueo de
  fila** (`SELECT … FOR UPDATE`) + comprobación de estado (`COMPLETED` no se re-completa).
  Hay una comprobación previa sin bloqueo para no repetir la "preparación" de un duplicado.
* **notifier-service**: la notificación usa `_id = event_id`; un duplicado choca con el
  índice único de `_id` (`DuplicateKeyError`) y se ignora.

### Reintentos con backoff exponencial y DLQ

* `retry_async` + `RetryPolicy`: espera `min(max_delay, base · 2^(n-1))` con **jitter**
  (0.5 s, 1 s, 2 s, 4 s…) para errores transitorios; `PermanentError` no se reintenta.
* Agotados los reintentos (o si el mensaje está malformado) → copia a `<stream>.dlq` con el
  motivo y `XACK`. En el procesador, un *hook* marca el pedido como `FAILED`.
* Protección contra *poison messages*: si un mensaje se ha entregado más de
  `CONSUMER_MAX_DELIVERIES` veces (el proceso muere al tratarlo), va directo a la DLQ.
* El bucle del consumidor sobrevive a caídas de Redis (backoff y reconexión).
* El timeout del socket de Redis se fija **por encima** del bloqueo de `XREADGROUP`
  (`create_redis_client`). Si fueran iguales, cada espera sin mensajes acabaría en
  `TimeoutError`, se confundiría con "Redis caído" y el backoff retrasaría los pedidos nuevos.

### Consistencia eventual

El cliente recibe `201` con `status: PENDING` en milisegundos; el pedido pasa a `COMPLETED`
y aparece la notificación unos segundos después. El test E2E sondea hasta que se cumple.

### Tarea programada: APScheduler

`AsyncIOScheduler` dentro del proceso de `cleanup-job` (intervalo de 60 s, `max_instances=1`,
`coalesce=True`), junto a una pequeña API con `/health`, `/metrics` y
`POST /jobs/cleanup/run`. `cleanup-job/job.py` permite ejecutarlo una sola vez, por si se
prefiere cron del sistema o un CronJob de Kubernetes.

### Patrones de código

| Patrón | Dónde |
|---|---|
| Application Factory | `create_app()` en cada `app/main.py` (uvicorn `--factory`) |
| Repository | `app/repositories.py`, `app/repository.py` (Protocol + implementación Mongo / fake en memoria) |
| Unit of Work | `async with session.begin()` en la capa de servicio |
| Service Layer | `OrderService`, `OrderProcessor`, `NotificationHandler`, `CleanupService` |
| Dependency Injection | `Depends(...)` de FastAPI; reloj, `sleep` y aleatoriedad inyectables en los tests |
| Strategy / Value Object | `RetryPolicy` inmutable |
| Transactional Outbox / Inbox | `cafe_common/db/outbox.py`, `processed_events` |
| Envelope | `EventEnvelope` (metadatos comunes + payload versionado) |

### Librería compartida `shared/` (`cafe_common`)

Contiene solo aspectos transversales: logs JSON, `trace_id`, contrato de eventos, reintentos,
mensajería, métricas/health, API key y esquema SQL. Se instala como dependencia de ruta de
Poetry, así se evita duplicar código entre servicios sin mezclar su lógica de negocio.

### Desviaciones respecto a la estructura sugerida

* Se añadió `shared/` (la librería común) y `tests/` en la raíz (tests E2E).
* `cleanup-job` usa `pyproject.toml` + Poetry en lugar de `requirements.txt`, por coherencia con
  el requisito *"Dependencies managed with Poetry"*. Mantiene `job.py` como punto de entrada.

---

## 6. Contrato de eventos (payloads)

Todos los eventos comparten un *envelope*. En Redis se guardan los campos `event_id`,
`event_type` y `trace_id` (para inspección rápida) y `envelope` (JSON completo).

### `orders.created` (orders-service → processor-service)

```json
{
  "event_id": "5b0f6a53-2c0e-4c8e-9f0d-2a8e3c1b7d44",
  "event_type": "orders.created",
  "event_version": 1,
  "occurred_at": "2026-09-24T10:15:31.473986Z",
  "producer": "orders-service",
  "trace_id": "07f6e728903e4f46a0441713b336e84b",
  "payload": {
    "order_id": "ba7549ca-c576-4912-9def-c78894cef9e3",
    "customer_id": "abc123",
    "status": "PENDING",
    "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
    "created_at": "2026-09-24T10:15:31.473986Z"
  }
}
```

### `orders.completed` (processor-service → notifier-service)

```json
{
  "event_id": "6a0e2f7c-4b1d-4c55-9a55-2f0f8a3b9d11",
  "event_type": "orders.completed",
  "event_version": 1,
  "occurred_at": "2026-09-24T10:15:34.476527Z",
  "producer": "processor-service",
  "trace_id": "07f6e728903e4f46a0441713b336e84b",
  "payload": {
    "order_id": "ba7549ca-c576-4912-9def-c78894cef9e3",
    "customer_id": "abc123",
    "status": "COMPLETED",
    "items": [{"name": "latte", "qty": 1}, {"name": "muffin", "qty": 2}],
    "completed_at": "2026-09-24T10:15:34.476527Z"
  }
}
```

* `event_id` es **estable**: es el id de la fila del outbox, así que una re-publicación conserva
  el mismo valor y permite deduplicar.
* `orders.completed` incluye `customer_id` e `items` (*event-carried state transfer*): el
  notificador no necesita consultar la BD de pedidos.

---

## 7. Observabilidad

* **Logs JSON estructurados** en stdout con `timestamp`, `level`, `service`, `trace_id`,
  `logger`, `message` y campos extra:

  ```json
  {"timestamp":"2026-09-24T10:15:34.476+00:00","level":"INFO","service":"processor-service","trace_id":"07f6e728903e4f46a0441713b336e84b","logger":"app.handlers","message":"order_completed","order_id":"ba7549ca-..."}
  ```

* **Trazabilidad**: el `trace_id` nace en `orders-service` (o se toma de la cabecera
  `X-Trace-Id` si el cliente la envía), se devuelve en la respuesta, viaja en el outbox y en los
  eventos, y se restaura en cada consumidor con `contextvars`. Para seguir un pedido:
  `docker compose logs | grep <trace_id>`.
* **`GET /health`** (readiness: comprueba PostgreSQL/Redis/MongoDB y devuelve `503` si alguno
  falla) y **`GET /health/live`** (liveness). Docker Compose usa `/health` como `healthcheck`.
* **`GET /metrics`** en formato Prometheus: `http_requests_total`,
  `http_request_duration_seconds`, `events_published_total`, `events_consumed_total{result}`
  (`success`/`duplicate`/`dead_letter`), `cleanup_runs_total`, `cleanup_deleted_notifications_total`.
* **Visores web de datos** (perfil opcional `tools`, solo desarrollo): `make tools` levanta
  Adminer (PostgreSQL, http://localhost:8080), mongo-express (MongoDB, http://localhost:8081) y
  redis-commander (Redis Streams, http://localhost:8082). No arrancan con `docker compose up`.

---

## 8. Configuración

Toda la configuración se hace con **variables de entorno** (`pydantic-settings`). Las URLs con
credenciales (`DATABASE_URL`, `REDIS_URL`, `MONGO_URL`) son **obligatorias** y no tienen valor
por defecto en el código: si faltan, el servicio no arranca (*fail fast*).

| Variable | Servicio | Defecto | Descripción |
|---|---|---|---|
| `API_KEY` | orders, notifier, cleanup | `cafe-cloud-dev-key` (compose) | Vacía = autenticación desactivada |
| `LOG_LEVEL` | todos | `INFO` | |
| `DATABASE_URL` | orders, processor | — | `postgresql+asyncpg://…` |
| `REDIS_URL` | orders, processor, notifier | — | `redis://redis:6379/0` |
| `MONGO_URL` | notifier, cleanup | — | `mongodb://…?authSource=admin` |
| `PROCESSING_MIN_SECONDS` / `_MAX_SECONDS` | processor | 2 / 5 | Duración simulada de la preparación |
| `PROCESSING_FAILURE_RATE` | processor | 0 | Probabilidad de fallo transitorio simulado |
| `RETRY_MAX_ATTEMPTS` / `RETRY_BASE_DELAY_SECONDS` / `RETRY_MAX_DELAY_SECONDS` | processor, notifier | 5 / 0.5 / 10 | Backoff exponencial |
| `CONSUMER_CLAIM_IDLE_MS` / `CONSUMER_MAX_DELIVERIES` | processor, notifier | 60000 / 5 | Reclamación de huérfanos / poison messages |
| `OUTBOX_POLL_INTERVAL_SECONDS` | orders, processor | 0.5 | Frecuencia del relay |
| `RETENTION_HOURS` / `INTERVAL_SECONDS` | cleanup | 24 / 60 | Retención y periodicidad |

Consulta `.env.example` para las variables de Docker Compose (usuarios, contraseñas, puertos).

---

## 9. Estructura del repositorio

```
cafe-cloud/
├── shared/                     # librería común cafe_common (instalada por ruta con Poetry)
│   ├── cafe_common/
│   │   ├── db/                 # models.py (esquema), session.py, outbox.py (Transactional Outbox)
│   │   ├── events.py           # EventEnvelope + payloads + nombres de streams
│   │   ├── messaging.py        # RedisStreamPublisher / RedisStreamConsumer (ACK, DLQ, XAUTOCLAIM)
│   │   ├── retry.py            # RetryPolicy + retry_async (backoff exponencial)
│   │   ├── observability.py    # middleware trace_id + /health + /metrics
│   │   ├── logs.py · tracing.py · metrics.py · security.py · lifecycle.py · clock.py
│   └── tests/
├── orders-service/
│   ├── app/                    # main.py, config.py, schemas.py, services.py, repositories.py, api/, seed.py
│   ├── alembic/ + alembic.ini  # migraciones
│   ├── tests/  ·  Dockerfile  ·  pyproject.toml  ·  poetry.lock
├── processor-service/          # app/handlers.py (OrderProcessor), repositories.py, main.py
├── notifier-service/           # app/handlers.py, repository.py (Mongo), api.py, models.py
├── cleanup-job/                # app/service.py, repository.py, main.py (APScheduler) + job.py
├── tests/integration/          # test E2E + Dockerfile (perfil "test" de compose)
├── docs/EXPLICACION_DETALLADA.md
├── scripts/tasks.ps1           # equivalente del Makefile para Windows
├── docker-compose.yml  ·  Makefile  ·  .env.example  ·  README.md
```

---

## 10. Resolución de problemas

| Síntoma | Solución |
|---|---|
| `port is already allocated` (5432/6379/27017) | Tienes ese motor instalado en local: cambia `POSTGRES_PORT`, `REDIS_PORT` o `MONGO_PORT` en `.env` |
| Un servicio no llega a `healthy` | `docker compose logs <servicio>`; `curl localhost:800X/health` muestra qué dependencia falla |
| Quiero empezar de cero | `make clean && make up` (borra volúmenes) |
| El test E2E se salta (`skipped`) | El sistema no está levantado: `docker compose up -d` y repite |
| `make` no existe (Windows) | Usa `.\scripts\tasks.ps1 <tarea>` |
| `make` no existe (Ubuntu/Debian) | `sudo apt install -y make` |
| `permission denied ... docker.sock` (Linux) | `sudo usermod -aG docker $USER` y cierra sesión (o `newgrp docker`) |

---

## 11. Cumplimiento de requisitos de la prueba

Correspondencia entre cada requisito del enunciado y dónde está implementado.
La explicación detallada, archivo por archivo, está en
[`docs/EXPLICACION_DETALLADA.md`](docs/EXPLICACION_DETALLADA.md).

| Requisito del enunciado | Implementación |
|---|---|
| `POST /orders` valida, guarda en SQL y devuelve `order_id`, `status`, `created_at` | `orders-service/app/schemas.py`, `app/services.py`, `app/api/routes.py` |
| Tabla `orders (id, customer_id, status, created_at)` | `shared/cafe_common/db/models.py` + migración Alembic `orders-service/alembic/versions/` |
| Publicar `orders.created` en un broker | Transactional Outbox (`shared/cafe_common/db/outbox.py`) → Redis Streams (`shared/cafe_common/messaging.py`) |
| Idempotencia con `Idempotency-Key` | Tabla `idempotency_keys` + `OrderService.create_order` |
| processor: escucha, simula 2–5 s, marca `COMPLETED`, publica `orders.completed` | `processor-service/app/handlers.py` |
| Reintentos con backoff exponencial | `shared/cafe_common/retry.py` (+ DLQ en `messaging.py`) |
| Consumidor idempotente | Tabla inbox `processed_events` (processor) y `_id = event_id` en MongoDB (notifier) |
| Logs estructurados con `trace_id` propagado | `shared/cafe_common/logs.py`, `tracing.py`; el `trace_id` viaja en el envelope del evento |
| notifier: guarda en NoSQL y expone `GET /notifications/{customer_id}` | `notifier-service/app/repository.py`, `app/api.py` (MongoDB) |
| cleanup-job periódico que borra > 24 h y lo registra en logs | `cleanup-job/app/service.py` + APScheduler en `app/main.py`; `POST /jobs/cleanup/run` |
| Python 3.11+, Poetry, Dockerfile por servicio | `pyproject.toml` + `poetry.lock` y `Dockerfile` en cada servicio (imágenes Python 3.12) |
| Docker Compose autocontenido (`docker compose up --build`) | `docker-compose.yml` con valores locales por defecto (`${VAR:-default}`) |
| Migraciones (Alembic) y datos semilla | `orders-service/alembic/`, `orders-service/app/seed.py` (servicio `migrate`) |
| Tests: ≥1 unitario por servicio y 1 de integración | `*/tests/` y `tests/integration/test_full_flow.py` |
| `GET /health` y `GET /metrics` | `shared/cafe_common/observability.py` (en los 4 servicios) |
| API key (plus) y credenciales no *hardcodeadas* | `shared/cafe_common/security.py` (`X-API-Key`); credenciales por variables de entorno |
| Automatización build/test/deploy | `Makefile` (Linux/macOS) y `scripts/tasks.ps1` (Windows) |
