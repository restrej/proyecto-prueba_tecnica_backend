"""notifier-service: notifica al cliente cuando su pedido está listo.

Consume ``orders.completed``, guarda una notificación en MongoDB (NoSQL) y
expone ``GET /notifications/{customer_id}``.
"""
