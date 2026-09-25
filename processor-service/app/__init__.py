"""processor-service: "prepara" los pedidos de Café Cloud.

Consume ``orders.created``, simula la preparación, marca el pedido como
``COMPLETED`` y publica ``orders.completed`` (vía Transactional Outbox).
"""
