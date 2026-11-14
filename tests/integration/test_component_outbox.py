"""Outbox y relay por componente contra PostgreSQL y Kafka reales (spec event-backbone)."""

from __future__ import annotations

import uuid

from eda_retail.events.envelope import new_event
from eda_retail.infra import dead_letters
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.outbox import ComponentRelay, RelayThread, append

from .kafka_helpers import read_all


def _incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


def _ev(incident: str, event_type: str):
    return new_event(
        event_type=event_type, correlation_id=incident, causation_id=None, producer="planner",
        payload={"incident_id": incident},
    )


def _relay(conn, settings, topic, schema="planner"):
    return ComponentRelay(conn, make_producer(settings.kafka_bootstrap_servers), schema, topic, dlq_topic=topic)


def _drain(conn):
    """Marca como publicadas las filas de otros tests para que el relay solo vea las de este."""
    with conn.transaction():
        conn.execute("UPDATE planner.outbox SET published_at = now() WHERE published_at IS NULL")
        conn.execute("UPDATE planner.dead_letters SET published_at = now() WHERE published_at IS NULL")


def test_state_and_outbox_commit_together(conn):
    """Fallo entre el cambio y la publicación: lo que no se confirma no queda en el outbox"""
    incident = _incident()
    try:
        with conn.transaction():
            append(conn, "planner", [_ev(incident, "PlanCreated")])
            raise RuntimeError("caída antes del COMMIT")
    except RuntimeError:
        pass
    count = conn.execute("SELECT count(*) AS n FROM planner.outbox WHERE correlation_id = %s", (incident,)).fetchone()
    assert count["n"] == 0


def test_relay_publishes_in_outbox_order_and_records_the_position(conn, settings, topic):
    """Orden por incidencia y posición del mensaje publicado"""
    _drain(conn)
    incident = _incident()
    with conn.transaction():
        append(conn, "planner", [_ev(incident, t) for t in ("PlanCreated", "StepCancelled", "IncidentResolved")])
    relay = _relay(conn, settings, topic)

    assert relay.run_once() == 3

    messages = read_all(settings.kafka_bootstrap_servers, topic, expected=3)
    assert [m.event_type for m in messages] == ["PlanCreated", "StepCancelled", "IncidentResolved"]
    assert {m.key for m in messages} == {incident}
    rows = conn.execute(
        "SELECT kafka_partition, kafka_offset FROM planner.outbox WHERE correlation_id = %s ORDER BY id", (incident,)
    ).fetchall()
    assert [(r["kafka_partition"], r["kafka_offset"]) for r in rows] == [(m.partition, m.offset) for m in messages]


def test_pending_event_is_published_after_a_restart(conn, connect_db, settings, topic):
    """Al reiniciar, el relay del componente publica el evento pendiente una sola vez"""
    _drain(conn)
    incident = _incident()
    with conn.transaction():
        append(conn, "planner", [_ev(incident, "PlanCreated")])
    relay = _relay(connect_db(), settings, topic)
    assert relay.run_once() == 1
    assert relay.run_once() == 0
    assert len(read_all(settings.kafka_bootstrap_servers, topic, expected=2, timeout=5)) == 1


def test_replay_republishes_its_own_outbox(conn, settings, topic):
    """Pérdida del topic: el componente republica su outbox"""
    _drain(conn)
    incident = _incident()
    with conn.transaction():
        [first] = append(conn, "planner", [_ev(incident, "PlanCreated")])
    relay = _relay(conn, settings, topic)
    relay.run_once()
    assert relay.replay_from(first) >= 1
    assert relay.run_once() >= 1
    messages = read_all(settings.kafka_bootstrap_servers, topic, expected=2)
    assert [m.key for m in messages] == [incident, incident]


def test_dead_letters_of_the_component_go_to_the_dlq(conn, settings, topic):
    _drain(conn)
    with conn.transaction():
        dead_letters.record(
            conn, consumer="planner", reason="invalid_schema", event_id=None, correlation_id="I-x",
            raw={"bad": True}, schema="planner",
        )
    assert _relay(conn, settings, topic).publish_dead_letters() == 1
    [message] = read_all(settings.kafka_bootstrap_servers, topic, expected=1)
    assert message.event_type == "DeadLetter" and message.value["component"] == "planner"


def test_relay_thread_publishes_and_stops(connect_db, conn, settings, topic):
    _drain(conn)
    incident = _incident()
    with conn.transaction():
        append(conn, "planner", [_ev(incident, "PlanCreated")])
    thread = RelayThread(_relay(connect_db(), settings, topic), idle_sleep=0.05)
    thread.start()
    try:
        [message] = read_all(settings.kafka_bootstrap_servers, topic, expected=1)
    finally:
        thread.stop(timeout=10)
    assert message.key == incident and not thread.is_alive()
