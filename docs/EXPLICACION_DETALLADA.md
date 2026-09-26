# Explicación detallada del código — Café Cloud

Este documento recorre **cada archivo** del proyecto y explica qué hace cada bloque y cada
línea importante, y **por qué** está escrito así. Complementa los comentarios del propio código:
cada clase y cada función tienen un *docstring*, y casi todas las líneas relevantes llevan un
comentario al lado.

Se sigue el orden en que "viaja" un pedido por el sistema:

1. [Conceptos previos](#1-conceptos-previos)
2. [Librería compartida `shared/cafe_common`](#2-librería-compartida-sharedcafe_common)
3. [orders-service](#3-orders-service)
4. [processor-service](#4-processor-service)
5. [notifier-service](#5-notifier-service)
6. [cleanup-job](#6-cleanup-job)
7. [Infraestructura: Dockerfiles, docker-compose, Makefile](#7-infraestructura)
8. [Tests](#8-tests)
9. [Recorrido completo de un pedido paso a paso](#9-recorrido-completo-de-un-pedido-paso-a-paso)

---

## 1. Conceptos previos

| Concepto | Qué significa aquí |
|---|---|
| **async / await** | Todos los servicios son asíncronos: mientras una petición espera a la BD o a Redis, el mismo proceso atiende otras. `await` marca los puntos donde se cede el control. |
| **FastAPI** | Framework web. Valida automáticamente los cuerpos JSON con Pydantic y genera `/docs`. |
| **Pydantic** | Define modelos con tipos; valida y convierte datos (JSON ↔ objetos Python). |
| **SQLAlchemy 2.0 (async)** | ORM: las tablas se definen como clases Python y se consultan con `select(...)`. |
| **Alembic** | Versiona el esquema de la BD (migraciones). |
| **Redis Streams** | Estructura de Redis que funciona como una cola/log de mensajes con *consumer groups*. |
| **At-least-once** | Garantía de entrega: un mensaje nunca se pierde, pero **puede llegar repetido**. Por eso los consumidores deben ser **idempotentes** (procesar dos veces = procesar una). |
| **Idempotencia** | Repetir una operación produce el mismo resultado que hacerla una vez. |
| **Transactional Outbox** | Guardar el evento en la BD en la misma transacción que el dato, y publicarlo después. |
| **Consistencia eventual** | Los servicios no se actualizan todos a la vez, pero acaban coherentes en segundos. |
| `from __future__ import annotations` | Aparece en todos los archivos: hace que las anotaciones de tipo se evalúen de forma diferida, lo que permite referencias adelantadas (`Order` usado antes de definirse) y sintaxis moderna de tipos. |

---

## 2. Librería compartida `shared/cafe_common`

Es un paquete Python que instalan los cuatro servicios (dependencia de ruta en Poetry:
`cafe-common = { path = "../shared", develop = true }`). Solo contiene aspectos **transversales**.

### 2.1 `shared/pyproject.toml`

* `[project]`: nombre, versión y `requires-python = ">=3.11,<4.0"` (la prueba exige 3.11+).
* `dependencies`: las librerías mínimas (`pydantic`, `redis`, `prometheus-client`, `fastapi`).
  Los rangos `>=X,<Y` permiten parches y evitan saltos de versión mayor; el `poetry.lock` fija
  las versiones exactas, lo que hace el build **reproducible**.
* `[project.optional-dependencies] sql = [...]`: **extra** `sql`. SQLAlchemy solo lo instalan
  los servicios que usan PostgreSQL (`cafe-common[sql]`), así notifier y cleanup no cargan con él.
* `[tool.poetry] packages`: le indica a Poetry que el código del paquete está en `cafe_common/`.
* `[tool.poetry.group.dev.dependencies]`: herramientas solo de desarrollo (pytest, fakeredis, ruff).
* `[tool.pytest.ini_options] asyncio_mode = "auto"`: los tests `async def` se ejecutan sin
  decoradores adicionales.
* `[tool.ruff]`: reglas del linter (E/F errores, I orden de imports, UP modernización,
  B bugs comunes, SIM simplificaciones, ASYNC malas prácticas async).

### 2.2 `clock.py`

```python
Clock = Callable[[], datetime]
```
Alias de tipo: un "reloj" es cualquier función sin argumentos que devuelve un `datetime`.

```python
def utcnow() -> datetime:
    return datetime.now(UTC)
```
Devuelve la hora actual **con zona horaria UTC** (*aware*). Se evita `datetime.utcnow()`
(deprecado), que devuelve fechas *naive* (sin zona) y provoca errores al compararlas. Los
servicios reciben `clock=utcnow` en su constructor; en los tests se pasa `clock=lambda: FECHA_FIJA`
para probar la regla de 24 h de forma determinista.

### 2.3 `tracing.py`

```python
_trace_id_var: ContextVar[str | None] = ContextVar("trace_id", default=None)
```
Una `ContextVar` es como una variable global, pero **cada tarea asyncio ve su propio valor**.
Si llegan 100 peticiones a la vez, cada una tiene su `trace_id` sin mezclarse.

* `new_trace_id()` → `uuid.uuid4().hex`: 32 caracteres hexadecimales aleatorios.
* `get_trace_id()` → lee el valor del contexto actual.
* `set_trace_id(x)` → fija el valor y devuelve un `Token`, que sirve para **restaurar** el
  valor anterior con `reset_trace_id(token)`.
* `trace_context(trace_id)` → *context manager* (`with trace_context(...)`): fija el valor al
  entrar y lo restaura en el `finally`, incluso si hay una excepción.

### 2.4 `logs.py`

```python
_STANDARD_ATTRS = frozenset(vars(logging.LogRecord("", 0, "", 0, "", (), None)).keys()) | {...}
```
Crea un `LogRecord` vacío y guarda los nombres de sus atributos estándar (`msg`, `levelname`,
`lineno`…). Todo lo que **no** esté en ese conjunto lo ha añadido el programador con
`extra={...}` y se incluirá en el JSON.

`class JsonFormatter(logging.Formatter)`:
* `__init__` guarda el nombre del servicio.
* `format(record)` construye un diccionario:
  * `timestamp`: `record.created` (segundos epoch) → ISO-8601 UTC con milisegundos.
  * `level`, `service`, `logger`, `message` (`record.getMessage()` aplica los `%s`).
  * `trace_id`: el pasado explícitamente en `extra=` o, si no, el del contexto (`get_trace_id()`).
  * El bucle `for key, value in record.__dict__.items()` añade los campos extra.
  * Si hay excepción (`logger.exception`), añade la traza en `exception`.
  * `json.dumps(..., default=str, ensure_ascii=False)`: `default=str` convierte tipos no JSON
    (UUID, datetime) a texto; `ensure_ascii=False` conserva tildes y emojis.

`configure_logging(service_name, level)`:
* Crea un `StreamHandler(sys.stdout)` con el formateador JSON (Docker recoge stdout).
* `root.handlers[:] = [handler]` **reemplaza** los handlers (si se llama dos veces no duplica logs).
* Para `uvicorn` y `apscheduler` borra sus handlers y activa `propagate`, para que sus logs
  también salgan en JSON.
* Desactiva `uvicorn.access` porque nuestro middleware ya registra cada petición con `trace_id`.

### 2.5 `events.py`

* `class Streams`: constantes `ORDERS_CREATED = "orders.created"` y
  `ORDERS_COMPLETED = "orders.completed"`. Centralizarlas evita erratas entre productor y consumidor.
* `dead_letter_stream(stream)` → `"orders.created.dlq"`.
* `class OrderStatus(StrEnum)`: `PENDING`, `COMPLETED`, `FAILED`. `StrEnum` hace que cada valor
  **sea** un string (se serializa a JSON directamente).
* `class EventEnvelope(BaseModel)`: el "sobre" de todo evento.
  * `model_config = ConfigDict(frozen=True)`: inmutable (no se puede modificar tras crearse).
  * `event_id: UUID = Field(default_factory=uuid4)`: id único; `default_factory` genera uno
    nuevo en cada instancia (un valor por defecto fijo sería el mismo para todas).
  * `event_version`: permite evolucionar el payload sin romper consumidores.
  * `occurred_at`, `producer`, `trace_id`, `payload: dict[str, Any]`.
* `OrderItemPayload`, `OrderCreatedPayload`, `OrderCompletedPayload`: el contenido tipado de
  cada evento. Los consumidores validan `envelope.payload` con estos modelos.

### 2.6 `retry.py`

* `class PermanentError(Exception)`: señala un error **no recuperable**. El consumidor no
  reintenta y manda el mensaje a la DLQ.
* `class RetryExhaustedError`: se lanza al agotar los intentos; guarda `attempts` y `last_error`.
* `@dataclass(frozen=True, slots=True) class RetryPolicy`:
  * `frozen=True` → inmutable; `slots=True` → menos memoria y sin atributos accidentales.
  * `__post_init__` valida los parámetros (*fail fast*).
  * `backoff(attempt)`:
    ```python
    delay = min(self.max_delay, self.base_delay * self.multiplier ** (attempt - 1))
    ```
    Con `base=0.5, multiplier=2`: intento 1 → 0.5 s, 2 → 1 s, 3 → 2 s, 4 → 4 s… con tope en `max_delay`.
    ```python
    return random.uniform(delay / 2, delay) if self.jitter else delay
    ```
    *Equal jitter*: se espera entre la mitad y el total, para que muchos clientes que fallan a
    la vez no reintenten en el mismo instante.
* `async def retry_async(operation, policy, *, retry_on, give_up_on, sleep)`:
  * `while True` con un contador `attempt`.
  * `return await operation()` → si funciona, sale.
  * `except give_up_on: raise` → los errores permanentes se propagan sin reintentar.
  * `except retry_on as exc:` → si quedan intentos, calcula la espera, lo registra en el log
    (`retrying_operation`) y hace `await sleep(delay)`. Si no quedan, lanza `RetryExhaustedError`.
  * `sleep` es inyectable: en los tests se pasa una función que solo apunta las esperas.

### 2.7 `metrics.py`

Define las métricas Prometheus **una sola vez por proceso**, porque registrarlas dos veces
lanza un error:
* `Counter` = contador que solo sube; `Histogram` = distribución de valores (latencias).
* Las etiquetas (`["method", "path", "status"]`) permiten filtrar. Se usa la **plantilla** de
  ruta para no crear una serie por cliente (explosión de cardinalidad).

### 2.8 `messaging.py` (el corazón de la mensajería)

* `create_redis_client(url, block_ms=None)` (*factory*): crea el cliente Redis que usan los
  servicios. Fija `socket_timeout = max(5, block_ms/1000 + 5)` segundos. Motivo: el consumidor
  hace `XREADGROUP BLOCK 5000` y el servidor tarda hasta 5 s en responder si no hay mensajes; con
  el timeout por defecto de redis-py 8 (también 5 s) el cliente cortaba la espera con
  `TimeoutError`, el bucle lo trataba como "Redis caído" y aplicaba backoff de hasta 30 s,
  retrasando los pedidos nuevos. `decode_responses=True` devuelve `str` en lugar de `bytes`.
* `encode_envelope(envelope)`: convierte el evento en los campos planos de un mensaje de
  Redis (`event_id`, `event_type`, `trace_id` y `envelope` con el JSON completo).
* `decode_envelope(fields)`: `EventEnvelope.model_validate_json(fields["envelope"])` parsea y
  **valida** el JSON. Si falta el campo lanza `KeyError`; si no cumple el contrato, `ValidationError`.
* `class EventPublisher(Protocol)`: interfaz estructural. Cualquier objeto con un método
  `publish(stream, envelope)` sirve (permite *fakes*).
* `class RedisStreamPublisher`:
  * `publish` ejecuta `XADD stream MAXLEN ~ 100000 * campos...`. `maxlen` + `approximate=True`
    recorta el stream de forma eficiente para que no crezca sin límite.
  * Incrementa `events_published_total` y registra `event_published`.
* `@dataclass ConsumerSettings`: stream, grupo, nombre de réplica, tamaño de lote,
  `block_ms` (cuánto espera `XREADGROUP`), `claim_idle_ms` (cuándo un mensaje se considera
  huérfano), `max_deliveries` y la política de reintentos.
* `_extract_messages(response)`: redis-py devuelve formas distintas según el protocolo (RESP2:
  lista; RESP3: diccionario). La función acepta ambas. Se verificó con tests bajo los dos protocolos.
* `class RedisStreamConsumer`:
  * `ensure_group()`: `XGROUP CREATE stream group 0 MKSTREAM`. `id="0"` hace que un grupo nuevo
    lea también los mensajes antiguos; `mkstream` crea el stream si no existe. Si el grupo ya
    existe, Redis responde `BUSYGROUP` y se ignora.
  * `run(stop_event)`: bucle principal hasta que se pida la parada.
    * Si Redis no responde (`ConnectionError`, `TimeoutError`, `OSError`), espera con backoff
      (`min(30, 0.5·2^fallos)`) y reintenta **sin morir**.
    * Cualquier otra excepción se registra y el bucle continúa.
  * `poll_once()`:
    1. `_claim_stale_messages()` → `XAUTOCLAIM`: se apropia de mensajes entregados a otra
       réplica que lleva más de `claim_idle_ms` sin hacer ACK (probablemente murió).
    2. Si no hay huérfanos: `XREADGROUP GROUP g c COUNT 10 BLOCK 5000 STREAMS s >`. `>`
       significa "mensajes nunca entregados a este grupo".
    3. `asyncio.gather(...)` procesa el lote **en paralelo**.
  * `_claim_stale_messages()` consulta con `XPENDING` cuántas veces se ha entregado cada mensaje
    (`_delivery_count`). Si supera `max_deliveries` → DLQ (*poison message*: un mensaje que
    tumba el proceso una y otra vez).
  * `_handle_message(message_id, fields)`:
    1. Decodifica; si falla → `_dead_letter(...)` y termina.
    2. `with trace_context(envelope.trace_id)`: a partir de aquí, **todos** los logs llevan el
       `trace_id` del evento.
    3. `await retry_async(lambda: self._handler(envelope), policy)`: ejecuta la lógica de
       negocio con reintentos.
    4. Si fue bien → `XACK` (confirmación: se quita de la lista de pendientes) y la métrica `success`.
    5. `PermanentError` / `RetryExhaustedError` → DLQ + hook `on_dead_letter` (en el procesador,
       marca el pedido `FAILED`).
    6. Otra excepción inesperada → **no** se hace ACK; el mensaje queda pendiente y se reclamará.
  * `_dead_letter(...)`: `XADD <stream>.dlq` con los campos originales + `error`,
    `original_message_id`, `failed_at`, y **después** `XACK`. El orden importa: si el proceso
    muere entre ambos, el mensaje se reprocesa pero nunca se pierde.
* `_sleep_or_stop(stop_event, seconds)`: espera `seconds` **o** hasta que se pida la parada
  (`asyncio.wait_for(stop_event.wait(), timeout)`), así el apagado es inmediato.

### 2.9 `observability.py`

* `_route_template(request)`: lee `request.scope["route"]`, que **el router de Starlette rellena
  al resolver la ruta**, y devuelve su plantilla (`/notifications/{customer_id}`). Se llama
  después de procesar la petición. *(Nota: en FastAPI 0.141 los routers incluidos quedan
  envueltos y ya no aparecen como rutas planas en `app.routes`; por eso se usa el scope.)*
* `class TraceMiddleware(BaseHTTPMiddleware)` → `dispatch(request, call_next)`:
  1. `trace_id = request.headers.get("X-Trace-Id") or new_trace_id()` → aquí **nace** la traza.
  2. `set_trace_id(trace_id)` → visible para todo el código de la petición.
  3. `time.perf_counter()` → reloj monotónico, que no le afectan los cambios de hora.
  4. `response = await call_next(request)` → ejecuta el endpoint.
  5. Añade la cabecera `X-Trace-Id` a la respuesta.
  6. En el `finally`: actualiza `http_requests_total` y `http_request_duration_seconds`, escribe
     el log `http_request` (en DEBUG para `/health` y `/metrics`, para no generar ruido) y
     restaura el contexto.
* `build_ops_router(checks)`:
  * `GET /health/live` → `{"status":"alive"}` (el proceso responde).
  * `GET /health` → ejecuta cada comprobación (`SELECT 1`, `PING`…) con un timeout de 2 s.
    Devuelve 200 si todo está bien o **503** con el detalle de lo que falla.
  * `GET /metrics` → `generate_latest()` en formato texto de Prometheus.
* `setup_observability(app, checks)` instala el middleware y el router.

### 2.10 `security.py`

* `api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)`: lee la cabecera y hace
  que Swagger muestre el botón **Authorize**. `auto_error=False` para devolver nuestro propio 401.
* `verify_api_key(request, provided_key)`:
  * Lee la clave esperada de `request.app.state.api_key` (la fija cada servicio en `create_app`).
  * Si está vacía → autenticación desactivada.
  * `secrets.compare_digest` compara en **tiempo constante** (evita ataques de temporización).
  * Si no coincide → `HTTPException(401)` con la cabecera `WWW-Authenticate: ApiKey`.

### 2.11 `lifecycle.py`

`class BackgroundWorkers`: gestiona las tareas de fondo (consumidor, relay).
* `start(name, worker)` → `asyncio.create_task(worker(self._stop_event))`.
* `shutdown(grace_seconds)`:
  1. `self._stop_event.set()` → los bucles ven la señal y terminan su iteración.
  2. `asyncio.wait(tasks, timeout=grace_seconds)` → espera a que acaben solas.
  3. Las que no acaban a tiempo se cancelan (`task.cancel()`).

### 2.12 `db/models.py` (esquema de la BD)

* `NAMING_CONVENTION`: nombres deterministas para constraints e índices (`pk_orders`,
  `fk_order_items_order_id_orders`), lo que hace las migraciones reproducibles.
* `JsonType = JSON().with_variant(JSONB(), "postgresql")`: JSONB en PostgreSQL, JSON en SQLite.
* `BigIntPk = BigInteger().with_variant(Integer(), "sqlite")`: en SQLite solo `INTEGER`
  autoincrementa.
* `class Base(DeclarativeBase)`: base de los modelos, con la convención de nombres y el mapa
  `type_annotation_map` (por ejemplo, todo `datetime` → `TIMESTAMP WITH TIME ZONE`).
* `class Order`:
  * `__table_args__ = (CheckConstraint("status IN (...)"),)`: la **BD** garantiza que el estado
    es válido, aunque un bug en el código intentara otro valor.
  * `id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)`.
  * `customer_id` con `index=True` (búsquedas por cliente).
  * `created_at` con `default=utcnow` (Python) y `server_default=func.now()` (BD).
  * `updated_at` con `onupdate=utcnow` (se actualiza en cada `UPDATE`).
  * `items = relationship(..., cascade="all, delete-orphan", lazy="selectin")`:
    las líneas se guardan y borran con el pedido. `selectin` las carga con una segunda consulta
    `IN (...)`; en modo async la carga perezosa implícita no está permitida.
* `class OrderItem`: `CheckConstraint("qty > 0")`, FK a `orders.id` con `ondelete="CASCADE"`.
* `class IdempotencyKey`: la **PK es la propia clave**, así la BD impide duplicados incluso con
  concurrencia. Guarda `request_hash` (SHA-256 del cuerpo), `order_id`, `status_code` y
  `response_body`.
* `class OutboxEvent`: `id` (será el `event_id`), `aggregate_id` (pedido), `stream`, `payload`,
  `trace_id`, `created_at`, `published_at` (NULL = pendiente). **Índice parcial**
  `WHERE published_at IS NULL`: solo indexa las filas pendientes, así es pequeño y rápido.
* `class ProcessedEvent`: PK compuesta `(event_id, consumer)`, la tabla *inbox* del consumidor
  idempotente.

### 2.13 `db/session.py`

* `create_engine(url)` → `create_async_engine(url, pool_pre_ping=True)`: un pool de conexiones.
  `pool_pre_ping` descarta conexiones rotas (por ejemplo, si PostgreSQL se reinició).
* `create_session_factory(engine)` → `async_sessionmaker(engine, expire_on_commit=False)`:
  tras el commit se pueden seguir leyendo los atributos sin otra consulta.
* `ping_database(engine)` → `SELECT 1` (health check).

### 2.14 `db/outbox.py` (Transactional Outbox)

* `build_outbox_event(stream, aggregate_id, payload, trace_id, created_at)`: crea (sin guardar)
  la fila. `payload.model_dump(mode="json")` convierte UUID y fechas a texto JSON.
* `class OutboxRelay`:
  * `publish_pending()`:
    ```python
    select(OutboxEvent)
      .where(OutboxEvent.published_at.is_(None), OutboxEvent.stream.in_(self._streams))
      .order_by(OutboxEvent.created_at).limit(self._batch_size)
      .with_for_update(skip_locked=True)
    ```
    * `published_at IS NULL` → solo las pendientes; `stream IN (...)` → solo los streams de
      este servicio.
    * `FOR UPDATE SKIP LOCKED` → bloquea las filas leídas y **salta** las que tiene bloqueadas
      otra réplica, así dos relays nunca publican la misma fila a la vez.
    * Para cada fila se construye el `EventEnvelope` con `event_id=row.id` (estable) y se publica.
    * Si el broker falla, `break`: las filas ya publicadas se marcan (commit al salir del `with`)
      y el resto se reintenta en la siguiente vuelta.
  * `run(stop_event)`: repite `publish_pending`. Si el lote vino lleno, repite enseguida (hay
    más trabajo); si no, espera `poll_interval` (0.5 s).

---

## 3. orders-service

### 3.1 `app/config.py`

`class Settings(BaseSettings)`: cada atributo se lee de la variable de entorno homónima en
mayúsculas.
* `database_url: str` y `redis_url: str` **sin valor por defecto** → obligatorias. Si faltan,
  Pydantic lanza un error claro al arrancar. Así no hay credenciales en el código.
* `api_key: SecretStr | None` → `SecretStr` oculta el valor si se imprime la configuración.
* `api_key_value` (propiedad) → la clave en claro, o `None` si está vacía.
* `@lru_cache get_settings()` → lee el entorno una sola vez.

### 3.2 `app/schemas.py` (validación de la API)

* `OrderItemIn`: `name` (1-100 caracteres) y `qty` (1-100). `extra="forbid"` rechaza campos
  desconocidos; `str_strip_whitespace` quita espacios; el `field_validator` pasa el nombre a
  minúsculas (`"Latte"` y `"latte"` son el mismo producto).
* `CreateOrderRequest`: `customer_id` con patrón `^[A-Za-z0-9_-]{1,64}$`; `items` con 1-50 líneas.
  * `fingerprint()`: `json.dumps(..., sort_keys=True, separators=(",", ":"))` produce un JSON
    **canónico** (el mismo texto aunque el cliente cambie el orden de las claves) y se le aplica
    SHA-256.
* `CreateOrderResponse`: `order_id`, `status`, `created_at` (lo que pide la prueba).
* `OrderDetail` / `OrderItemOut`: respuesta de `GET /orders/{id}`. `from_attributes=True`
  permite construirlos desde objetos ORM.

### 3.3 `app/errors.py`

Errores de dominio **independientes de HTTP** (`IdempotencyKeyReuseError`,
`OrderNotFoundError`). La capa API los traduce a 422 y 404. Así la lógica de negocio no depende
de FastAPI.

### 3.4 `app/repositories.py`

Patrón **Repository**: `OrderRepository.add/get`, `IdempotencyRepository.get/add` y
`OutboxRepository.add`. **Nunca hacen commit**: la transacción la controla el servicio
(Unit of Work), de modo que varias escrituras se confirman o se deshacen juntas.

### 3.5 `app/services.py` → `OrderService.create_order`

```python
request_hash = request.fingerprint()
try:
    return await self._create_or_replay(...)
except IntegrityError:
    replay = await self._replay_existing(idempotency_key, request_hash)
    if replay is None:
        raise
    return replay
```
* Camino normal: `_create_or_replay`.
* Si dos peticiones con la misma clave llegan a la vez, la segunda choca con la PK de
  `idempotency_keys` al hacer commit (`IntegrityError`). Su transacción ya se deshizo, así que se
  relee la clave en una sesión nueva y se devuelve la respuesta de la primera.

`_create_or_replay`:
1. `async with self._session_factory() as session, session.begin():` abre sesión y transacción
   (commit automático al salir bien, rollback si hay excepción).
2. `existing = await idempotency.get(key)`: si existe → `_to_replay` (compara hashes; si
   difieren → `IdempotencyKeyReuseError` → 422).
3. Crea el `Order` con sus `OrderItem` (estado `PENDING`).
4. `await session.flush()`: envía el `INSERT` del pedido **sin** commit, para que las filas que lo
   referencian por FK se inserten después.
5. Añade el evento `orders.created` al outbox con `build_outbox_event(...)`.
6. Añade el registro `IdempotencyKey` con la respuesta serializada.
7. Al salir del `with` → **COMMIT atómico** de pedido + líneas + outbox + clave.

`_to_replay` → `CreateOrderResponse.model_validate(record.response_body)`: reconstruye la
respuesta original guardada.

`get_order(order_id)` → detalle o `OrderNotFoundError`.

### 3.6 `app/api/dependencies.py` y `app/api/routes.py`

* `get_order_service(request)` → `OrderService(request.app.state.session_factory)`. Los
  endpoints lo piden con `Depends`, lo que facilita sustituirlo en los tests.
* `router = APIRouter(prefix="/orders", dependencies=[Depends(verify_api_key)])`: **todas** las
  rutas exigen la API key.
* `create_order`:
  * `payload: CreateOrderRequest` → FastAPI valida el JSON; si es inválido, 422 automático.
  * `idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key", ...)]` → lee la cabecera.
  * Sin cabecera → 400.
  * `trace_id=get_trace_id()` → el que puso el middleware.
  * Si `result.replayed` → cambia el status a 200 y añade `Idempotent-Replayed: true`.
* `get_order` → `GET /orders/{order_id}` (FastAPI valida que sea un UUID).
* `register_exception_handlers` → traduce los errores de dominio a 422 y 404.

### 3.7 `app/main.py` → `create_app(settings)`

1. `configure_logging(...)`.
2. `lifespan(app)`: el código **antes** del `yield` se ejecuta al arrancar y el de **después**, al parar.
   * Crea el engine SQL, la fábrica de sesiones y el cliente Redis (`decode_responses=True`
     → strings en lugar de bytes) y los guarda en `app.state`.
   * Arranca el `OutboxRelay` para `orders.created` como tarea de fondo.
   * Al parar: detiene los workers, cierra Redis y libera el pool de PostgreSQL.
3. `FastAPI(...)`, `app.state.api_key`, `setup_observability(...)` con los checks
   `database` y `redis`, el router y los manejadores de errores.

Se arranca con `uvicorn app.main:create_app --factory`: uvicorn llama a la función, así no hay
una variable global que lea el entorno al importar.

### 3.8 `app/seed.py`

Inserta dos pedidos con **ids fijos** (idempotente: `if await session.get(...) is None`). El
`PENDING` lleva su evento en el outbox, así que al arrancar recorre todo el flujo.

### 3.9 Alembic: `alembic.ini`, `alembic/env.py`, `alembic/versions/..._0001_initial_schema.py`

* `alembic.ini`: `script_location`, plantilla de nombres y `prepend_sys_path = .` (para poder
  importar `app`). **Sin URL**: se lee de `DATABASE_URL`.
* `env.py`:
  * `target_metadata = Base.metadata` → el esquema deseado (para `--autogenerate` y `check`).
  * `run_migrations_online()` crea un engine async y usa `connection.run_sync(...)`, porque
    Alembic es síncrono por dentro.
  * `run_migrations_offline()` → `alembic upgrade head --sql` genera el SQL sin conectarse.
* `0001_initial_schema.py`: `upgrade()` crea las 5 tablas con sus PK, FK, CHECK e índices
  (incluido el índice parcial del outbox). `downgrade()` las elimina en orden inverso por las FK.
  Un test verifica que `upgrade` + `alembic check` no detecta diferencias con los modelos.

---

## 4. processor-service

### 4.1 `app/config.py`

Además de las URLs: parámetros del consumidor (`consumer_group`, `consumer_name` = hostname
del contenedor, que es único por réplica), de los reintentos, de la simulación
(`processing_min/max_seconds`, `processing_failure_rate`) y del relay. El `model_validator`
comprueba que `min <= max`. `retry_policy()` construye la `RetryPolicy`.

### 4.2 `app/repositories.py`

* `OrderRepository.get_for_update(order_id)` → `SELECT ... FOR UPDATE`: **bloquea** la fila del
  pedido hasta el fin de la transacción. Si dos réplicas reciben el mismo pedido a la vez, la
  segunda espera y luego lo ve ya `COMPLETED`.
* `ProcessedEventRepository.exists/add` → tabla inbox `processed_events`.

### 4.3 `app/handlers.py` → `OrderProcessor.handle(envelope)`

1. Valida el payload con `OrderCreatedPayload`; si es inválido → `PermanentError` (DLQ).
2. `_already_processed(event_id)`: consulta rápida sin bloqueo. Si ya se procesó, lo ignora
   (no "prepara" dos veces).
3. `_prepare(payload)`: `await self._sleep(uniform(2, 5))`, que simula la preparación. Se usa
   `asyncio.sleep` (equivalente **no bloqueante** de `time.sleep`) para que el proceso siga
   atendiendo `/health` y otros mensajes. Con `failure_rate` lanza `TransientProcessingError`
   (reintento con backoff). Está **fuera** de la transacción para no retener bloqueos 5 s.
4. `_complete(envelope, payload)` en **una transacción**:
   * `get_for_update` (bloqueo); si no existe → `PermanentError`.
   * Si el evento ya está en el inbox → ignorar (comprobación definitiva, ya con el bloqueo).
   * `inbox.add(event_id)`.
   * Si ya estaba `COMPLETED` → no emite otro evento.
   * `status = COMPLETED`, `completed_at`, `updated_at` y añade `orders.completed` al outbox
     con **el mismo `trace_id`**.
   * COMMIT: los tres cambios se confirman juntos.

`mark_failed(envelope, error)` es el hook de DLQ: si el pedido sigue `PENDING`, lo marca `FAILED`.

### 4.4 `app/main.py`

En el `lifespan` crea el `OrderProcessor`, el `RedisStreamConsumer` (stream `orders.created`,
handler `processor.handle`, `on_dead_letter=processor.mark_failed`) y el `OutboxRelay` para
`orders.completed`, y los arranca como workers. Expone `/health` (BD + Redis) y `/metrics`.

---

## 5. notifier-service

### 5.1 `app/models.py` → `Notification`

* `id` = `event_id` de `orders.completed`. Guardado como `_id` en MongoDB, **garantiza la
  idempotencia**: un evento duplicado no puede crear otra notificación.
* `to_document()` (`id` → `_id`) y `from_document()` (`_id` → `id`) adaptan el modelo a Mongo.

### 5.2 `app/repository.py`

* `NotificationRepository(Protocol)`: la interfaz (`add_if_absent`, `list_by_customer`).
* `MongoNotificationRepository`:
  * `ensure_indexes()`: índice compuesto `(customer_id, created_at desc)` para el GET e índice
    `created_at` para la limpieza. `create_index` es idempotente.
  * `add_if_absent()`: `insert_one`; si salta `DuplicateKeyError`, devuelve `False` (duplicado).
  * `list_by_customer()`: `find({"customer_id": ...}).sort("created_at", -1).limit(n)` y se
    recorre el cursor async con `async for`.

### 5.3 `app/handlers.py` → `NotificationHandler.handle`

Valida `OrderCompletedPayload`, construye el mensaje (`"1x latte, 2x muffin"`), crea la
`Notification` con `created_at=self._clock()` y llama a `add_if_absent`. Registra en el log
`notification_created` o `duplicate_event_ignored`.

### 5.4 `app/api.py`

`GET /notifications/{customer_id}`: valida el patrón del id y `limit` (1-1000, 100 por defecto)
y devuelve `list[Notification]`. Una lista vacía es un 200 válido (el cliente aún no tiene
notificaciones).

### 5.5 `app/main.py`

`AsyncMongoClient(url, tz_aware=True)` (las fechas vuelven con zona UTC), `ensure_indexes()`,
consumidor de `orders.completed` y health checks `mongodb` (`admin.command("ping")`) y `redis`.

---

## 6. cleanup-job

* `app/config.py`: `retention_hours=24`, `interval_seconds=60`, `run_on_startup`.
* `app/repository.py`: `delete_many({"created_at": {"$lt": cutoff}})`, que devuelve cuántos borró.
* `app/service.py` → `CleanupService.run()`:
  1. `with trace_context(new_trace_id())` → cada ejecución tiene su propio `trace_id` en los logs.
  2. `cutoff = now - 24h`.
  3. Borra, mide la duración, actualiza las métricas (`cleanup_runs_total`,
     `cleanup_deleted_notifications_total`) y escribe el log `cleanup_finished`.
  4. Devuelve `CleanupResult` (también es la respuesta HTTP del endpoint manual).
* `app/main.py`:
  * `AsyncIOScheduler(timezone="UTC").add_job(service.run, "interval", seconds=60,
    max_instances=1, coalesce=True, next_run_time=ahora)`:
    * `max_instances=1` → nunca dos limpiezas a la vez.
    * `coalesce=True` → si se acumulan ejecuciones perdidas, solo se ejecuta una.
    * `next_run_time=ahora` → la primera ejecución es inmediata.
  * `POST /jobs/cleanup/run` → ejecución manual (protegida con API key).
* `job.py`: ejecuta **una** limpieza y termina (para cron del sistema o un CronJob de Kubernetes).

---

## 7. Infraestructura

### 7.1 Dockerfiles (multi-stage)

* `FROM python:3.12-slim AS base`: imagen ligera.
* `ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 ... POETRY_VIRTUALENVS_CREATE=false`:
  sin `.pyc`, logs sin búfer y dependencias instaladas en el Python del contenedor (el
  contenedor ya aísla).
* `COPY shared/ /srv/shared/` + `WORKDIR /srv/<servicio>`: se reproduce la estructura del repo
  para que la ruta `../shared` del lock funcione.
* Se copian primero `pyproject.toml` y `poetry.lock` y se ejecuta `poetry install --only main`:
  Docker **cachea** esta capa y, si solo cambia el código, no reinstala dependencias.
* Stage `test`: añade las dependencias de desarrollo y ejecuta `pytest` (`make test-unit-docker`).
* Stage `runtime`: copia el código, crea el usuario sin privilegios `app` (`USER app`) y arranca
  `uvicorn app.main:create_app --factory --no-access-log`.

### 7.2 `docker-compose.yml`

* `x-app-env`, `x-database-url`, `x-mongo-url`, `x-http-healthcheck`: **anclas YAML**
  reutilizadas con `<<: [*a, *b]` para no repetir configuración.
* `${VAR:-defecto}`: toma la variable de `.env` o del entorno, o el valor local por defecto.
  Así `docker compose up` funciona sin configurar nada.
* `postgres`, `redis` (`--appendonly yes` = persistencia AOF) y `mongo`, con `healthcheck` y
  volúmenes con nombre (los datos sobreviven a `down`).
* `migrate`: usa la imagen de orders-service con el comando
  `alembic upgrade head && python -m app.seed`, y `restart: "no"`.
* `depends_on` con `condition: service_healthy` / `service_completed_successfully` garantiza el
  orden de arranque.
* `healthcheck` de las apps: `python -c "urllib.request.urlopen('http://localhost:8000/health')"`
  (las imágenes slim no traen curl).
* `integration-tests` con `profiles: ["test"]`: no arranca con `up`, solo con `--profile test`.

### 7.3 `Makefile` y `scripts/tasks.ps1`

Automatizan build, arranque, migraciones, seed, tests (locales, en Docker y E2E), lint, demo y
limpieza. `tasks.ps1` ofrece los mismos comandos en PowerShell para Windows sin `make`.

### 7.4 `.env.example`, `.gitignore`, `.dockerignore`

* `.env.example`: plantilla de variables (el `.env` real está en `.gitignore`).
* `.dockerignore`: evita enviar al build los `.venv` locales (pesados y de otro sistema
  operativo), las cachés y `.git`.

---

## 8. Tests

| Archivo | Qué demuestra |
|---|---|
| `shared/tests/test_retry.py` | Esperas 0.5/1/2 s, tope, jitter acotado, `PermanentError` sin reintentos, `RetryExhaustedError` |
| `shared/tests/test_messaging.py` | Publicar → consumir → ACK; reintento transitorio; DLQ + hook; mensaje malformado; **reclamación de huérfanos** con `XAUTOCLAIM` (con `fakeredis`); timeout del cliente mayor que el bloqueo |
| `shared/tests/test_logs.py` | Campos obligatorios del JSON y prioridad del `trace_id` |
| `shared/tests/test_observability.py` | La métrica usa la plantilla de ruta; `X-Trace-Id` reutilizado; `/health` |
| `orders-service/tests/test_schemas.py` | Validaciones y hash canónico |
| `orders-service/tests/test_orders_api.py` | 201 + outbox, repetición 200, **5 peticiones concurrentes → 1 pedido**, 422 por reutilizar la clave, 400, 401, 404, `/metrics` |
| `orders-service/tests/test_migrations.py` | `upgrade` → `alembic check` → `downgrade` |
| `processor-service/tests/test_order_processor.py` | COMPLETED + `orders.completed` con el mismo trace, duplicado → un solo evento, errores permanentes, fallo transitorio sin cambios a medias, `FAILED` por DLQ |
| `notifier-service/tests/test_notifications.py` | Creación, duplicado, payload inválido, ida y vuelta del documento, GET y 401 |
| `cleanup-job/tests/test_cleanup.py` | Borra las de 25 h y 48 h y conserva las de 1 h y 23 h; endpoint manual |
| `tests/integration/test_full_flow.py` | **E2E**: POST → cola → processor → notifier → GET, idempotencia y `trace_id` de extremo a extremo; se salta si el stack no está levantado |

Las técnicas usadas son: **SQLite real y efímero** (`tmp_path`) en lugar de *mocks* de SQL,
**fakes en memoria** que implementan el mismo `Protocol` que el repositorio real, y **reloj,
`sleep` y aleatoriedad inyectados** para que los tests sean deterministas e instantáneos.

---

## 9. Recorrido completo de un pedido paso a paso

1. El cliente envía `POST /orders` con `Idempotency-Key` y `X-API-Key`.
2. `TraceMiddleware` genera `trace_id = 07f6…` y lo fija en el contexto.
3. `verify_api_key` valida la clave; FastAPI valida el cuerpo con `CreateOrderRequest`.
4. `OrderService.create_order` abre una transacción: no existe la clave → inserta el pedido
   `PENDING`, las líneas, la fila del outbox (`orders.created`, `trace_id=07f6…`) y la clave de
   idempotencia → **COMMIT**. Responde `201` con `X-Trace-Id: 07f6…`.
5. En menos de 0.5 s, el `OutboxRelay` de orders-service lee la fila (`FOR UPDATE SKIP LOCKED`),
   ejecuta `XADD orders.created` y marca `published_at`.
6. El `RedisStreamConsumer` de processor-service la recibe con `XREADGROUP`, fija
   `trace_id=07f6…` y llama a `OrderProcessor.handle` con reintentos.
7. El procesador "prepara" el pedido (2-5 s) y en una transacción: bloquea la fila, registra el
   inbox, pasa el pedido a `COMPLETED` y añade `orders.completed` al outbox → **COMMIT** → `XACK`.
8. El relay de processor-service publica `orders.completed`.
9. notifier-service lo consume e inserta en MongoDB la notificación con `_id = event_id` y
   `trace_id=07f6…` → `XACK`.
10. `GET /notifications/abc123` la devuelve.
11. Pasadas 24 h, cleanup-job (cada minuto) la borra.

Si en el paso 7 falla algo transitorio, se reintenta con backoff (0.5 s, 1 s, 2 s…). Si falla
siempre, el mensaje va a `orders.created.dlq` y el pedido queda `FAILED`. Si un proceso muere a
mitad, el mensaje sigue pendiente y otra réplica lo reclama con `XAUTOCLAIM`; gracias a la
idempotencia, el resultado final es el mismo.
