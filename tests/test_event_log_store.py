"""Componente de consulta `event_log` (spec cqrs-views, "Vista del log de eventos").

Sin PostgreSQL ni Kafka: qué guarda de cada mensaje, en el log o en su dead letter. La vista contra la base real está en
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


class FakeConn:
    def __init__(self):
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))
        return self

    rowcount = 1

    def fetchone(self):
        return {"seq": 1}

    def inserts(self) -> list[str]:
        return [s.split("(")[0].removeprefix("INSERT INTO ").strip() for s, _ in self.statements
                if s.startswith("INSERT INTO")]


def test_invalid_message_goes_to_the_event_log_dead_letter():
    """Evento inválido en el lado de lectura: event_log lo guarda en su dead letter, no en el log"""
    conn = FakeConn()
    store.apply_message(conn, FakeMessage(b"not json"))  # type: ignore[arg-type]
    assert conn.inserts() == ["event_log.dead_letters"]
    [(sql, params)] = conn.statements
    assert "ON CONFLICT (consumer, kafka_topic, kafka_partition, kafka_offset) DO NOTHING" in sql
    assert params[0] == "event_log" and params[3] == "invalid_schema"
    assert params[-3:] == ("eda.events", 0, 7)


def test_personal_data_goes_to_the_dead_letter_and_not_to_the_log():
    """Evento con datos personales: va a la dead letter y no se escribe en el log"""
    body = _body(payload={"incident_id": "I-1", "order_ids": ["ana@example.com"], "cause": "delay",
                          "estimated_delay_hours": 24, "severity": "low"})
    conn = FakeConn()
    store.apply_message(conn, FakeMessage(json.dumps(body).encode()))  # type: ignore[arg-type]
    assert conn.inserts() == ["event_log.dead_letters"]
    assert conn.statements[0][1][2] == "I-1"  # correlation_id


def test_unknown_event_type_is_stored_in_the_log():
    """Evento de un tipo desconocido: event_log lo guarda en event_log.events, sin dead letter"""
    conn = FakeConn()
    store.apply_message(conn, FakeMessage(json.dumps(_body(event_type="CarrierRenamed")).encode()))  # type: ignore[arg-type]
    assert conn.inserts() == ["event_log.events"]
