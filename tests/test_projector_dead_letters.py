"""Dead letters de los proyectores (spec resilience, «Dead letter queue»).

Sin PostgreSQL ni Kafka: qué escribe un proyector ante un mensaje inválido. La DLQ contra la base
y el broker reales está en tests/integration/test_incident_status.py.
"""

from __future__ import annotations

import json

from eda_retail.events.envelope import new_event
from eda_retail.projectors.base import Projector


class FakeMessage:
    def __init__(self, value: bytes):
        self._value = value

    def value(self):
        return self._value

    def headers(self):
        return None

    def topic(self):
        return "eda.events"

    def partition(self):
        return 2

    def offset(self):
        return 11


class FakeConn:
    rowcount = 1

    def __init__(self):
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))
        return self

    def tables(self) -> list[str]:
        return [s.split("(")[0].removeprefix("INSERT INTO ").strip() for s, _ in self.statements
                if s.startswith("INSERT INTO")]


def _projector() -> tuple[Projector, list]:
    applied: list = []
    return Projector("plans", {"PlanCreated": lambda conn, pos, event: applied.append(event)}), applied


def _message(event_type: str, payload: dict) -> FakeMessage:
    event = new_event(event_type=event_type, correlation_id="I-1", causation_id=None, producer="planner",
                      payload=payload)
    return FakeMessage(event.model_dump_json().encode())


def test_invalid_event_of_a_projected_type_goes_to_the_views_dead_letter():
    """Evento inválido en el lado de lectura: el proyector lo guarda en su dead letter y no lo aplica"""
    projector, applied = _projector()
    conn = FakeConn()
    projector.apply_message(conn, _message("PlanCreated", {"incident_id": "I-1"}))  # type: ignore[arg-type]
    assert applied == []
    assert conn.tables() == ["views.dead_letters"]
    params = conn.statements[0][1]
    assert (params[0], params[2], params[3]) == ("plans", "I-1", "invalid_schema")
    assert params[-3:] == ("eda.events", 2, 11)


def test_invalid_event_of_a_type_the_projector_does_not_attend_is_ignored():
    """Evento inválido de un tipo que el consumidor no atiende: lo ignora sin dead letter"""
    projector, applied = _projector()
    conn = FakeConn()
    projector.apply_message(conn, _message("StepCompleted", {"incident_id": "I-1"}))  # type: ignore[arg-type]
    assert applied == [] and conn.statements == []


def test_message_without_event_type_goes_to_every_projector_dead_letter():
    """Un mensaje del que no se puede leer el event_type lo atienden todos los proyectores"""
    projector, _ = _projector()
    conn = FakeConn()
    projector.apply_message(conn, FakeMessage(json.dumps({"hola": 1}).encode()))  # type: ignore[arg-type]
    assert conn.tables() == ["views.dead_letters"]


def test_unknown_event_type_is_ignored_without_dead_letter():
    """Evento de un tipo desconocido: el proyector lo ignora sin dead letter"""
    projector, applied = _projector()
    conn = FakeConn()
    projector.apply_message(conn, _message("CarrierRenamed", {"carrier": "X"}))  # type: ignore[arg-type]
    assert applied == [] and conn.statements == []
