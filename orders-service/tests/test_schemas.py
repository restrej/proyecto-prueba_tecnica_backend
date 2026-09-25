"""Tests unitarios de validación del cuerpo de ``POST /orders``."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from app.schemas import CreateOrderRequest

VALID = {"customer_id": "abc123", "items": [{"name": "Latte ", "qty": 1}]}


def test_valid_request_is_normalized() -> None:
    """Los nombres se recortan y pasan a minúsculas."""
    request = CreateOrderRequest.model_validate(VALID)
    assert request.items[0].name == "latte"


@pytest.mark.parametrize(
    "body",
    [
        {"customer_id": "abc123", "items": []},  # sin líneas
        {"customer_id": "abc123", "items": [{"name": "latte", "qty": 0}]},  # qty 0
        {"customer_id": "abc123", "items": [{"name": "latte", "qty": 101}]},  # qty > 100
        {"customer_id": "abc123", "items": [{"name": "", "qty": 1}]},  # nombre vacío
        {"customer_id": "has spaces", "items": [{"name": "latte", "qty": 1}]},  # id inválido
        {"customer_id": "abc123", "items": [{"name": "latte", "qty": 1, "x": 1}]},  # extra
        {"items": [{"name": "latte", "qty": 1}]},  # falta customer_id
    ],
)
def test_invalid_requests_are_rejected(body: dict[str, object]) -> None:
    """Cada cuerpo inválido produce un ValidationError."""
    with pytest.raises(ValidationError):
        CreateOrderRequest.model_validate(body)


def test_fingerprint_is_independent_of_key_order() -> None:
    """La huella no depende del orden de las claves del JSON recibido."""
    a = CreateOrderRequest.model_validate({"customer_id": "c1", "items": [{"name": "a", "qty": 1}]})
    b = CreateOrderRequest.model_validate({"items": [{"qty": 1, "name": "a"}], "customer_id": "c1"})
    assert a.fingerprint() == b.fingerprint()


def test_fingerprint_changes_with_content() -> None:
    """Cambiar la cantidad cambia la huella."""
    a = CreateOrderRequest.model_validate({"customer_id": "c1", "items": [{"name": "a", "qty": 1}]})
    b = CreateOrderRequest.model_validate({"customer_id": "c1", "items": [{"name": "a", "qty": 2}]})
    assert a.fingerprint() != b.fingerprint()
