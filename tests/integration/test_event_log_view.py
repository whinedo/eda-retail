"""Vista del log de eventos de `event_log` contra PostgreSQL y Kafka reales (spec cqrs-views)."""

from __future__ import annotations

import uuid

import psycopg
import pytest

from eda_retail.event_log.cli import build
from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.outbox import ComponentRelay, encode_event

TRACEPARENT = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"


def _incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


def _notice(incident: str) -> EventEnvelope:
    return build_event(
        "CarrierNoticeReceived",
        {"carrier_notice_id": f"N-{incident}", "carrier": "acme", "raw_text": "retraso"},
        correlation_id=incident, causation_id=None, producer="intake",
    )


def _delayed(incident: str, cause: EventEnvelope) -> EventEnvelope:
    return build_event(
        "ShipmentDelayed",
        {"incident_id": incident, "order_ids": ["P-1"], "cause": "delay", "estimated_delay_hours": 24,
         "severity": "low"},
        correlation_id=incident, causation_id=cause.event_id, producer="interpreter",
    )


def _plan(incident: str, cause: EventEnvelope) -> EventEnvelope:
    return build_event(
        "PlanCreated",
        {"incident_id": incident, "plan_version": 1,
         "steps": [{"step_id": "s1", "action": "notify_customer", "params": {}, "depends_on": []}]},
        correlation_id=incident, causation_id=cause.event_id, producer="planner",
    )


def _publish(settings, *events: EventEnvelope, raw: bytes | None = None, key: str = "I-x") -> None:
    producer = make_producer(settings.kafka_bootstrap_servers)
    for event in events:
        headers = [("event_type", event.event_type.encode()), ("traceparent", TRACEPARENT.encode())]
        producer.produce(settings.events_topic, key=event.correlation_id.encode(), value=encode_event(event),
                         headers=headers)
    if raw is not None:
        producer.produce(settings.events_topic, key=key.encode(), value=raw)
    assert producer.flush(30) == 0


def _consume(settings) -> int:
    consumer = build(settings)
    try:
        return consumer.catch_up()
    finally:
        consumer.close()


def _rows(conn, incident: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM event_log.events WHERE correlation_id = %s ORDER BY seq", (incident,)
    ).fetchall()
    conn.commit()
    return rows


def test_order_of_an_incident(settings, conn):
    """Orden de una incidencia: los seq son crecientes en el orden causal de la incidencia"""
    incident = _incident()
    notice = _notice(incident)
    delayed = _delayed(incident, notice)
    plan = _plan(incident, delayed)
    _publish(settings, notice, delayed, plan)

    _consume(settings)

    rows = _rows(conn, incident)
    assert [r["event_id"] for r in rows] == [notice.event_id, delayed.event_id, plan.event_id]
    assert [r["producer"] for r in rows] == ["intake", "interpreter", "planner"]
    assert rows[0]["seq"] < rows[1]["seq"] < rows[2]["seq"]


def test_message_delivered_twice(settings, conn):
    """Mensaje entregado dos veces: la vista sigue teniendo una sola fila con ese event_id"""
    incident = _incident()
    notice = _notice(incident)
    _publish(settings, notice, notice)

    _consume(settings)

    assert [r["event_id"] for r in _rows(conn, incident)] == [notice.event_id]


def test_row_keeps_partition_offset_and_trace_context(settings, conn):
    """Guarda la partición, el offset y el contexto de traza de cada mensaje"""
    incident = _incident()
    _publish(settings, _notice(incident))

    _consume(settings)

    [row] = _rows(conn, incident)
    assert row["kafka_topic"] == settings.events_topic
    assert row["kafka_partition"] >= 0 and row["kafka_offset"] == 0
    assert row["trace_context"] == {"traceparent": TRACEPARENT}


def test_invalid_message_is_skipped_and_its_offset_advances(settings, conn):
    """Un mensaje que no es un evento válido no se guarda ni bloquea la partición"""
    incident = _incident()
    _publish(settings, raw=b"not json", key=incident)
    _publish(settings, _notice(incident))

    _consume(settings)

    assert len(_rows(conn, incident)) == 1
    offsets = conn.execute(
        "SELECT kafka_offset FROM event_log.offsets WHERE consumer = 'event_log' AND kafka_topic = %s",
        (settings.events_topic,),
    ).fetchall()
    conn.commit()
    assert [r["kafka_offset"] for r in offsets] == [1]


def _dead_letters(settings, conn) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM event_log.dead_letters WHERE kafka_topic = %s ORDER BY id", (settings.events_topic,)
    ).fetchall()
    conn.commit()
    return rows


def test_invalid_message_goes_to_the_event_log_dead_letter_once(settings, conn):
    """Evento inválido en el lado de lectura: event_log lo guarda una vez en su dead letter y lo publica en la DLQ"""
    incident = _incident()
    _publish(settings, raw=b"not json", key=incident)
    _publish(settings, _notice(incident))
    consumer = build(settings)
    try:
        consumer.catch_up()
        consumer.reset([])  # reaplicar desde el principio no crea otra dead letter
        consumer.catch_up()
    finally:
        consumer.close()

    [row] = _dead_letters(settings, conn)
    assert (row["consumer"], row["reason"], row["kafka_offset"]) == ("event_log", "invalid_schema", 0)
    assert row["raw_message"] == {"raw": "not json"}
    assert len(_rows(conn, incident)) == 1

    # Como en test_component_outbox: la DLQ es el topic del test (el CI no crea `eda.dlq`).
    # El relay no se cierra: cerraría la conexión del fixture.
    producer = make_producer(settings.kafka_bootstrap_servers)
    relay = ComponentRelay(conn, producer, "event_log", settings.events_topic,
                           dlq_topic=settings.events_topic, outbox=False)
    assert relay.run_once() == 0
    while relay.publish_dead_letters():  # también las pendientes de otros tests de la misma base
        pass
    producer.flush(10)
    [row] = _dead_letters(settings, conn)
    assert row["published_at"] is not None


@pytest.mark.parametrize(
    "statement",
    ["UPDATE event_log.events SET producer = 'x'", "DELETE FROM event_log.events", "TRUNCATE event_log.events"],
)
def test_event_log_is_append_only(settings, conn, statement):
    """La vista es solo de añadir: la base de datos rechaza UPDATE, DELETE y TRUNCATE"""
    _publish(settings, _notice(_incident()))
    _consume(settings)
    with pytest.raises(psycopg.errors.RaiseException), conn.transaction():
        conn.execute(statement)
