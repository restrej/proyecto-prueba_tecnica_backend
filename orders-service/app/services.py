"""Casos de uso de orders-service (capa de aplicación / *Service Layer*).

El caso de uso principal, :meth:`OrderService.create_order`, combina dos
patrones de integración:

**Idempotency-Key** (``POST /orders`` idempotente)
    La primera petición con una clave crea el pedido y guarda la respuesta en
    ``idempotency_keys``. Las repeticiones con la misma clave y el mismo cuerpo
    reciben la respuesta original SIN crear nada. Si el cuerpo difiere, error
    422 (la clave se está reutilizando mal). La tabla tiene la clave como PK,
    así que dos peticiones concurrentes con la misma clave no pueden crear dos
    pedidos: la segunda choca con la PK (IntegrityError) y devuelve el
    resultado de la primera.

**Transactional Outbox**
    El pedido, sus líneas, el evento ``orders.created`` (tabla outbox) y la
    clave de idempotencia se insertan en UNA sola transacción. El relay publica
    el evento después. Nunca hay pedido sin evento ni evento sin pedido.
"""

from __future__ import annotations

import logging
import uuid
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from app.errors import IdempotencyKeyReuseError, OrderNotFoundError
from app.repositories import IdempotencyRepository, OrderRepository, OutboxRepository
from app.schemas import CreateOrderRequest, CreateOrderResponse, OrderDetail, OrderItemOut
from cafe_common.clock import Clock, utcnow
from cafe_common.db.models import IdempotencyKey, Order, OrderItem
from cafe_common.db.outbox import build_outbox_event
from cafe_common.db.session import SessionFactory
from cafe_common.events import (
    OrderCreatedPayload,
    OrderItemPayload,
    OrderStatus,
    Streams,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class CreateOrderResult:
    """Resultado del caso de uso "crear pedido".

    Attributes:
        response: cuerpo que se devolverá al cliente.
        replayed: True si es una repetición idempotente (no se creó nada).
    """

    response: CreateOrderResponse
    replayed: bool


class OrderService:
    """Casos de uso sobre pedidos."""

    def __init__(self, session_factory: SessionFactory, *, clock: Clock = utcnow) -> None:
        """Crea el servicio.

        Args:
            session_factory: fábrica de sesiones SQL (una por caso de uso).
            clock: reloj inyectable (tests deterministas).
        """
        self._session_factory = session_factory
        self._clock = clock

    async def create_order(
        self, request: CreateOrderRequest, *, idempotency_key: str, trace_id: str
    ) -> CreateOrderResult:
        """Crea un pedido de forma idempotente y encola ``orders.created``.

        Args:
            request: cuerpo validado de la petición.
            idempotency_key: valor de la cabecera ``Idempotency-Key``.
            trace_id: traza de la petición (se propaga en el evento).

        Returns:
            El resultado, indicando si fue una repetición.

        Raises:
            IdempotencyKeyReuseError: la clave ya se usó con otro cuerpo.
        """
        request_hash = request.fingerprint()
        try:
            return await self._create_or_replay(request, idempotency_key, request_hash, trace_id)
        except IntegrityError:
            # Carrera: otra petición concurrente con la MISMA clave hizo commit
            # primero. Nuestra transacción ya se deshizo; devolvemos la suya.
            logger.info("idempotency_race_detected", extra={"idempotency_key": idempotency_key})
            replay = await self._replay_existing(idempotency_key, request_hash)
            if replay is None:
                raise  # el IntegrityError no era por la clave: se propaga
            return replay

    async def _create_or_replay(
        self,
        request: CreateOrderRequest,
        idempotency_key: str,
        request_hash: str,
        trace_id: str,
    ) -> CreateOrderResult:
        """Transacción principal: repite si la clave existe o crea el pedido.

        Args:
            request: cuerpo validado.
            idempotency_key: clave de idempotencia.
            request_hash: huella del cuerpo.
            trace_id: traza a propagar.

        Returns:
            Resultado nuevo o repetido.
        """
        # session.begin(): commit automático al salir del bloque sin errores,
        # rollback automático si se lanza cualquier excepción.
        async with self._session_factory() as session, session.begin():
            idempotency = IdempotencyRepository(session)
            existing = await idempotency.get(idempotency_key)
            if existing is not None:
                return self._to_replay(existing, request_hash)

            now = self._clock()
            order = Order(
                id=uuid.uuid4(),
                customer_id=request.customer_id,
                status=OrderStatus.PENDING.value,
                created_at=now,
                updated_at=now,
                items=[OrderItem(name=item.name, qty=item.qty) for item in request.items],
            )
            OrderRepository(session).add(order)
            # flush: envía el INSERT del pedido ya (sin commit) para que las
            # filas que lo referencian por FK se inserten después.
            await session.flush()

            payload = OrderCreatedPayload(
                order_id=order.id,
                customer_id=order.customer_id,
                status=OrderStatus.PENDING,
                items=[OrderItemPayload(name=i.name, qty=i.qty) for i in request.items],
                created_at=now,
            )
            OutboxRepository(session).add(
                build_outbox_event(
                    stream=Streams.ORDERS_CREATED,
                    aggregate_id=order.id,
                    payload=payload,
                    trace_id=trace_id,
                    created_at=now,
                )
            )

            response = CreateOrderResponse(
                order_id=order.id, status=OrderStatus.PENDING, created_at=now
            )
            idempotency.add(
                IdempotencyKey(
                    key=idempotency_key,
                    request_hash=request_hash,
                    order_id=order.id,
                    status_code=201,
                    response_body=response.model_dump(mode="json"),
                    created_at=now,
                )
            )
        # Aquí ya se hizo COMMIT: pedido + outbox + clave son persistentes.
        logger.info(
            "order_created",
            extra={"order_id": str(order.id), "customer_id": order.customer_id},
        )
        return CreateOrderResult(response=response, replayed=False)

    async def _replay_existing(self, key: str, request_hash: str) -> CreateOrderResult | None:
        """Relee la clave en una transacción nueva (tras una carrera).

        Returns:
            El resultado repetido, o ``None`` si la clave no existe.
        """
        async with self._session_factory() as session:
            existing = await IdempotencyRepository(session).get(key)
            return None if existing is None else self._to_replay(existing, request_hash)

    @staticmethod
    def _to_replay(record: IdempotencyKey, request_hash: str) -> CreateOrderResult:
        """Convierte un registro de idempotencia en la respuesta original.

        Raises:
            IdempotencyKeyReuseError: si el cuerpo actual no coincide.
        """
        if record.request_hash != request_hash:
            raise IdempotencyKeyReuseError(record.key)
        logger.info("idempotent_replay", extra={"order_id": str(record.order_id)})
        return CreateOrderResult(
            response=CreateOrderResponse.model_validate(record.response_body), replayed=True
        )

    async def get_order(self, order_id: UUID) -> OrderDetail:
        """Devuelve el detalle de un pedido.

        Raises:
            OrderNotFoundError: si no existe.
        """
        async with self._session_factory() as session:
            order = await OrderRepository(session).get(order_id)
            if order is None:
                raise OrderNotFoundError(order_id)
            return OrderDetail(
                order_id=order.id,
                customer_id=order.customer_id,
                status=OrderStatus(order.status),
                items=[OrderItemOut.model_validate(item) for item in order.items],
                created_at=order.created_at,
                updated_at=order.updated_at,
                completed_at=order.completed_at,
            )
