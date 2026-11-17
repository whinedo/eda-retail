"""Qué mensajes son inválidos para un consumidor (spec resilience, «Dead letter queue»).

Sin Kafka: `decode_checked` decide si un mensaje va a la dead letter (InvalidMessage) o se procesa.
"""

from __future__ import annotations

import json

import pytest

from eda_retail.events.catalog import CATALOG
from eda_retail.events.envelope import new_event
from eda_retail.infra.consumer import InvalidMessage, decode_checked


class FakeMessage:
    def __init__(self, value: bytes):
        self._value = value

    def value(self):
        return self._value


def _delayed(**overrides) -> bytes:
    event = new_event(
        event_type="ShipmentDelayed", correlation_id="I-1", causation_id=None, producer="interpreter",
        payload={"incident_id": "I-1", "order_ids": ["P-1"], "cause": "delay", "estimated_delay_hours": 24,
                 "severity": "low"},
    )
    return json.dumps({**json.loads(event.model_dump_json()), **overrides}).encode()


def test_unknown_event_version_is_invalid():
    """Evento de una versión desconocida: una versión posterior a la del catálogo no tiene upcaster y es inválida"""
    future = CATALOG["ShipmentDelayed"].version + 1
    with pytest.raises(InvalidMessage) as bad:
        decode_checked(FakeMessage(_delayed(event_version=future)))  # type: ignore[arg-type]
    assert (bad.value.event_type, bad.value.correlation_id) == ("ShipmentDelayed", "I-1")


def test_current_version_is_valid():
    event = decode_checked(FakeMessage(_delayed()))  # type: ignore[arg-type]
    assert event.event_version == CATALOG["ShipmentDelayed"].version


def test_unknown_event_type_with_a_valid_envelope_is_not_invalid():
    """Evento de un tipo desconocido: la estructura común es válida, así que no es inválido (tolerant reader)"""
    event = decode_checked(FakeMessage(_delayed(event_type="CarrierRenamed", event_version=7)))  # type: ignore[arg-type]
    assert event.event_type == "CarrierRenamed"


def test_message_without_readable_event_type_has_no_type():
    with pytest.raises(InvalidMessage) as bad:
        decode_checked(FakeMessage(b"not json"))  # type: ignore[arg-type]
    assert bad.value.event_type is None
