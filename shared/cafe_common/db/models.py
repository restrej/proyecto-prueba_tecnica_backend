"""Esquema ORM (SQLAlchemy 2.0) de la base de datos de pedidos.

Decisión de diseño: ``orders-service`` es el *dueño* de este esquema (ejecuta
las migraciones Alembic), pero ``processor-service`` necesita actualizar el
estado del pedido, tal y como pide la prueba. Para tener UNA sola fuente de
verdad del esquema, los modelos viven en esta librería compartida y ambos
servicios los importan.

Tablas:

* ``orders``            -> pedido (id, customer_id, status, created_at, ...).
* ``order_items``       -> líneas del pedido (normalización 1:N).
* ``idempotency_keys``  -> claves Idempotency-Key ya procesadas + respuesta.
* ``outbox_events``     -> eventos pendientes de publicar (Transactional Outbox).
* ``processed_events``  -> eventos ya consumidos (Idempotent Consumer / Inbox).
"""

from __future__ import annotations

import uuid
from datetime import datetime
from typing import Any

from sqlalchemy import (
    JSON,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    MetaData,
    String,
    Uuid,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB  # JSON binario indexable en PG
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

from cafe_common.clock import utcnow
from cafe_common.events import OrderStatus

# Convención de nombres para constraints/índices: Alembic genera nombres
# deterministas (p. ej. "pk_orders", "fk_order_items_order_id_orders"), lo que
# hace que las migraciones sean reproducibles entre entornos.
NAMING_CONVENTION = {
    "ix": "ix_%(table_name)s_%(column_0_name)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}

# JSON portable: JSONB en PostgreSQL y JSON genérico en SQLite (tests).
JsonType = JSON().with_variant(JSONB(), "postgresql")
# Entero autoincremental: BIGINT en PostgreSQL; en SQLite solo INTEGER
# autoincrementa, por eso se usa la variante.
BigIntPk = BigInteger().with_variant(Integer(), "sqlite")
# Lista de estados válidos para el CHECK constraint.
_STATUS_VALUES = ", ".join(f"'{s.value}'" for s in OrderStatus)


class Base(DeclarativeBase):
    """Clase base declarativa de todos los modelos ORM."""

    metadata = MetaData(naming_convention=NAMING_CONVENTION)
    # Mapea anotaciones Python a tipos SQL por defecto.
    type_annotation_map = {
        dict[str, Any]: JsonType,
        datetime: DateTime(timezone=True),  # siempre TIMESTAMPTZ (con zona)
    }


class Order(Base):
    """Pedido de un cliente (raíz del agregado)."""

    __tablename__ = "orders"
    __table_args__ = (
        # Integridad a nivel de BD: el estado solo puede ser uno de los válidos.
        CheckConstraint(f"status IN ({_STATUS_VALUES})", name="status_valid"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    customer_id: Mapped[str] = mapped_column(String(64), index=True)  # búsquedas por cliente
    status: Mapped[str] = mapped_column(String(20), default=OrderStatus.PENDING.value)
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    updated_at: Mapped[datetime] = mapped_column(
        default=utcnow, server_default=func.now(), onupdate=utcnow
    )
    completed_at: Mapped[datetime | None] = mapped_column(default=None)

    # Relación 1:N con las líneas. cascade="all, delete-orphan": las líneas se
    # guardan/borran junto con el pedido. lazy="selectin": se cargan con una
    # segunda query "IN (...)" (obligatorio en async, donde el lazy-load
    # implícito no está permitido).
    items: Mapped[list[OrderItem]] = relationship(
        back_populates="order",
        cascade="all, delete-orphan",
        lazy="selectin",
        order_by="OrderItem.id",
    )


class OrderItem(Base):
    """Línea de pedido: producto y cantidad."""

    __tablename__ = "order_items"
    __table_args__ = (CheckConstraint("qty > 0", name="qty_positive"),)

    id: Mapped[int] = mapped_column(BigIntPk, primary_key=True, autoincrement=True)
    order_id: Mapped[uuid.UUID] = mapped_column(
        ForeignKey("orders.id", ondelete="CASCADE"), index=True
    )
    name: Mapped[str] = mapped_column(String(100))
    qty: Mapped[int] = mapped_column(Integer)

    order: Mapped[Order] = relationship(back_populates="items")


class IdempotencyKey(Base):
    """Registro de una petición ``POST /orders`` ya procesada.

    La clave primaria es la propia ``Idempotency-Key``: la unicidad la
    garantiza la base de datos incluso con peticiones concurrentes.
    """

    __tablename__ = "idempotency_keys"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    # SHA-256 del cuerpo: detecta reutilización de la clave con OTRO cuerpo.
    request_hash: Mapped[str] = mapped_column(String(64))
    order_id: Mapped[uuid.UUID] = mapped_column(ForeignKey("orders.id", ondelete="CASCADE"))
    status_code: Mapped[int] = mapped_column(Integer)  # status original (201)
    response_body: Mapped[dict[str, Any]] = mapped_column()  # respuesta original
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())


class OutboxEvent(Base):
    """Evento pendiente de publicar (patrón Transactional Outbox).

    Se inserta en la MISMA transacción que el cambio de negocio; un proceso
    aparte (el *relay*) lo publica en el broker y rellena ``published_at``.
    """

    __tablename__ = "outbox_events"
    __table_args__ = (
        # Índice parcial: solo indexa las filas sin publicar, que son las que
        # consulta el relay constantemente -> muy pequeño y rápido.
        Index(
            "ix_outbox_events_unpublished",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
            sqlite_where=text("published_at IS NULL"),
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(Uuid, index=True)  # id del pedido
    event_type: Mapped[str] = mapped_column(String(100))
    stream: Mapped[str] = mapped_column(String(100))  # destino en el broker
    payload: Mapped[dict[str, Any]] = mapped_column()
    trace_id: Mapped[str] = mapped_column(String(64))
    created_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
    published_at: Mapped[datetime | None] = mapped_column(default=None)


class ProcessedEvent(Base):
    """Evento ya procesado por un consumidor (Idempotent Consumer / Inbox).

    Clave primaria compuesta (event_id, consumer): el mismo evento puede ser
    procesado una vez por CADA consumidor distinto, pero nunca dos veces por
    el mismo.
    """

    __tablename__ = "processed_events"

    event_id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    consumer: Mapped[str] = mapped_column(String(100), primary_key=True)
    processed_at: Mapped[datetime] = mapped_column(default=utcnow, server_default=func.now())
