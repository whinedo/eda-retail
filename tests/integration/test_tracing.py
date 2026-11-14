"""Observabilidad: trazas de OpenTelemetry síncronas (SQL, funciones) y asíncronas (outbox → Kafka)."""

from __future__ import annotations

import time
import uuid
from dataclasses import replace

import psycopg
import pytest
from opentelemetry.trace import SpanKind, StatusCode

from eda_retail.events.envelope import EventEnvelope, new_event
from eda_retail.infra import tracing
from eda_retail.infra.consumer import EventConsumer, Outcome
from eda_retail.infra.event_store import append_events
from eda_retail.infra.relay import OutboxRelay

PING = "TestPing"


def _ping(incident: str) -> EventEnvelope:
    return new_event(event_type=PING, correlation_id=incident, causation_id=None, producer="test", payload={})


def _echo(conn: psycopg.Connection, event: EventEnvelope) -> None:
    append_events(
        conn,
        [
            new_event(
                event_type="TestPong",
                correlation_id=event.correlation_id,
                causation_id=event.event_id,
                producer="test-consumer",
                payload={},
            )
        ],
    )


def _relay_all(settings) -> None:
    relay = OutboxRelay.from_settings(settings)
    try:
        while relay.run_once() > 0:
            pass
    finally:
        relay.close()


def _consume(settings, event_id: uuid.UUID, group: str):
    consumer = EventConsumer.from_settings(settings, name=group, handlers={PING: _echo})
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            msg = consumer.poll(0.5)
            if msg is None:
                continue
            outcome = consumer.handle_message(msg)
            consumer.commit(msg)
            if consumer.event_id_of(msg) == event_id:
                return msg, outcome
    finally:
        consumer.close()
    raise AssertionError(f"no llegó el evento {event_id}")


def _by_name(spans, name: str, event_id: uuid.UUID):
    found = [s for s in spans if s.name == name and s.attributes.get("eda.event_id") == str(event_id)]
    assert len(found) == 1, f"{name}: {len(found)} spans"
    return found[0]


def test_append_stores_trace_context_in_outbox(conn, spans):
    """El contexto de traza se guarda en outbox.trace_context y no en events"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    event = _ping(incident)
    with tracing.tracer().start_as_current_span("test.root") as root, conn.transaction():
        [seq] = append_events(conn, [event])
    stored = conn.execute("SELECT trace_context FROM outbox WHERE seq = %s", (seq,)).fetchone()
    traceparent = stored["trace_context"]["traceparent"]
    assert traceparent.split("-")[1] == format(root.get_span_context().trace_id, "032x")

    create = _by_name(spans.get_finished_spans(), f"{PING} create", event.event_id)
    assert create.kind is SpanKind.PRODUCER
    assert create.attributes["eda.incident_id"] == incident
    assert create.attributes["eda.event_type"] == PING
    assert create.parent.span_id == root.get_span_context().span_id
    assert traceparent.split("-")[2] == format(create.context.span_id, "016x")


def test_trace_crosses_outbox_relay_kafka_and_consumer(connect_db, settings, spans):
    """Traza por incidencia: escritura, relay, consumo y evento derivado en la misma traza"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    event = _ping(incident)
    conn = connect_db()
    with tracing.tracer().start_as_current_span("test.root"), conn.transaction():
        append_events(conn, [event])
    _relay_all(settings)
    msg, outcome = _consume(settings, event.event_id, f"g-{uuid.uuid4().hex[:8]}")
    assert outcome is Outcome.PROCESSED
    assert "traceparent" in dict(msg.headers())

    finished = spans.get_finished_spans()
    create = _by_name(finished, f"{PING} create", event.event_id)
    publish = _by_name(finished, f"{PING} publish", event.event_id)
    process = _by_name(finished, f"{PING} process", event.event_id)
    pong = next(
        s for s in finished if s.name == "TestPong create" and s.attributes.get("eda.causation_id") == str(event.event_id)
    )

    trace_id = create.context.trace_id
    assert {publish.context.trace_id, process.context.trace_id, pong.context.trace_id} == {trace_id}
    assert publish.parent.span_id == create.context.span_id
    assert process.parent.span_id == publish.context.span_id
    assert pong.parent.span_id == process.context.span_id
    assert process.kind is SpanKind.CONSUMER
    assert process.attributes["eda.outcome"] == "processed"
    assert process.attributes["eda.incident_id"] == incident
    assert process.attributes["messaging.destination.name"] == settings.events_topic


def test_replay_keeps_trace_context(connect_db, settings, spans):
    """La republicación desde el log conserva la traza original"""
    event = _ping(f"I-{uuid.uuid4().hex[:8]}")
    conn = connect_db()
    with tracing.tracer().start_as_current_span("test.root"), conn.transaction():
        [seq] = append_events(conn, [event])
    conn.commit()
    _relay_all(settings)
    relay = OutboxRelay.from_settings(settings)
    try:
        relay.replay_from(seq)
        while relay.run_once() > 0:
            pass
    finally:
        relay.close()
    finished = spans.get_finished_spans()
    create = _by_name(finished, f"{PING} create", event.event_id)
    publishes = [
        s for s in finished if s.name == f"{PING} publish" and s.attributes.get("eda.event_id") == str(event.event_id)
    ]
    assert len(publishes) == 2
    assert {s.context.trace_id for s in publishes} == {create.context.trace_id}


def test_sync_sql_statements_are_traced(conn, spans):
    """Operaciones síncronas: cada sentencia SQL dentro de una operación genera un span"""
    with tracing.tracer().start_as_current_span("test.sync") as parent:
        conn.execute("SELECT 1").fetchone()
    conn.rollback()
    db = [s for s in spans.get_finished_spans() if s.attributes.get("db.system") == "postgresql"]
    assert db and all(s.parent.span_id == parent.get_span_context().span_id for s in db)
    assert db[0].kind is SpanKind.CLIENT


def test_orphan_sql_statements_are_not_traced(conn, spans):
    """Las sentencias sin operación en curso (p. ej. el polling del relay) no crean trazas sueltas"""
    conn.execute("SELECT 1").fetchone()
    conn.rollback()
    assert [s for s in spans.get_finished_spans() if s.attributes.get("db.system") == "postgresql"] == []


def test_traced_function_records_span_and_error(spans):
    """Operaciones síncronas en proceso: el decorador traced crea un span y registra errores"""

    @tracing.traced("test.compute")
    def compute(x: int) -> int:
        if x < 0:
            raise ValueError("negativo")
        return x * 2

    assert compute(2) == 4
    with pytest.raises(ValueError):
        compute(-1)
    done = [s for s in spans.get_finished_spans() if s.name == "test.compute"]
    assert [s.status.status_code for s in done] == [StatusCode.UNSET, StatusCode.ERROR]


def test_tracing_disabled_writes_no_context(conn, monkeypatch):
    """Con el tracing desactivado el sistema funciona y no guarda contexto"""
    monkeypatch.setattr(tracing, "_provider", None)
    event = _ping(f"I-{uuid.uuid4().hex[:8]}")
    with conn.transaction():
        [seq] = append_events(conn, [event])
    assert conn.execute("SELECT trace_context FROM outbox WHERE seq = %s", (seq,)).fetchone()["trace_context"] is None


def test_setup_tracing_respects_switch(base_settings):
    """TRACING_ENABLED=false: setup_tracing no instala proveedor"""
    assert tracing.setup_tracing("test", replace(base_settings, tracing_enabled=False)) is None
