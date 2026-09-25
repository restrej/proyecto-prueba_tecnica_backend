"""Capa SQL compartida (requiere el extra ``cafe-common[sql]``).

* ``models``  -> esquema ORM de la base de datos de pedidos.
* ``session`` -> fábrica de engine/sesiones asíncronas.
* ``outbox``  -> patrón Transactional Outbox (creación + relay de eventos).
"""
