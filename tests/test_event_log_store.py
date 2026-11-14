"""Componente de consulta `event_log` (spec cqrs-views, "Vista del log de eventos").

Sin PostgreSQL ni Kafka: qué guarda de cada mensaje. La vista contra la base real está en
tests/integration/test_event_log_view.py.
"""

from __future__ import annotations

import json
import uuid

from eda_retail.event_log import store
from eda_retail.events.envelope import new_event


class FakeMessage:
    def __init__(self, value: bytes, headers=None):
        self._value, self._headers = value, headers

    def value(self):
        return self._value

    def headers(self):
        return self._headers

    def topic(self):
        return "eda.events"

    def partition(self):
        return 0

    def offset(self):
        return 7


def _body(**overrides) -> dict:
    event = new_event(
        event_type="ShipmentDelayed", correlation_id="I-1", causation_id=None, producer="interpreter",
        payload={"incident_id": "I-1", "order_ids": ["P-1"], "cause": "delay", "estimated_delay_hours": 24,
                 "severity": "low"},
    )
    return {**json.loads(event.model_dump_json()), **overrides}


def test_valid_message_is_stored_as_published():
    """El log guarda el evento tal como se publicó, sin el seq del productor"""
    body = _body(seq=99)
    envelope = store.envelope_of(FakeMessage(json.dumps(body).encode()))
    assert envelope is not None
    assert envelope.event_id == uuid.UUID(body["event_id"])
    assert envelope.event_version == body["event_version"]
    assert envelope.payload["order_ids"] == ["P-1"]


def test_message_that_is_not_json_is_skipped():
    assert store.envelope_of(FakeMessage(b"not json")) is None


def test_message_with_personal_data_is_not_stored():
    """Sin datos personales en el log: un mensaje con un correo no se guarda"""
    body = _body(payload={"incident_id": "I-1", "order_ids": ["ana@example.com"], "cause": "delay",
                          "estimated_delay_hours": 24, "severity": "low"})
    assert store.envelope_of(FakeMessage(json.dumps(body).encode())) is None
