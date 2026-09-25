"""orders-service: registro de pedidos de Café Cloud.

Capas (de fuera hacia dentro):

* ``api``          -> HTTP: rutas, cabeceras, códigos de estado (FastAPI).
* ``services``     -> casos de uso / lógica de aplicación (idempotencia, outbox).
* ``repositories`` -> acceso a datos (patrón Repository sobre SQLAlchemy).
* ``cafe_common.db.models`` -> esquema persistente.
"""
