"""event-backbone (entrega al menos una vez) e idempotency (inbox por consumidor)."""

from __future__ import annotations

import time
import uuid

import psycopg
import pytest

from eda_retail.events.envelope import EventEnvelope, new_event
from eda_retail.infra.consumer import EventConsumer, Outcome
from eda_retail.infra.event_store import append_events
from eda_retail.infra.relay import OutboxRelay

PING = "TestPing"


class HandlerCrash(RuntimeError):
    pass


def _publish_ping(connect_db, settings) -> EventEnvelope:
    incident = f"I-{uuid.uuid4().hex[:8]}"
    event = new_event(
        event_type=PING, correlation_id=incident, causation_id=None, producer="test", payload={}
    )
    conn = connect_db()
    with conn.transaction():
        append_events(conn, [event])
    _relay_all(settings)
    return event


def _relay_all(settings) -> None:
    relay = OutboxRelay.from_settings(settings)
    try:
        while relay.run_once() > 0:
            pass
    finally:
        relay.close()


def _echo_handler(conn: psycopg.Connection, event: EventEnvelope) -> None:
    """Handler de prueba: produce un evento derivado por cada evento procesado."""
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


def _consumer(settings, group: str, handler=_echo_handler) -> EventConsumer:
    return EventConsumer.from_settings(settings, name=group, handlers={PING: handler})


def _next_for(consumer: EventConsumer, event_id: uuid.UUID, timeout: float = 30.0):
    """Recibe mensajes hasta el del evento buscado; los demás se procesan como ignorados."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        msg = consumer.poll(0.5)
        if msg is None:
            continue
        if consumer.event_id_of(msg) == event_id:
            return msg
        consumer.handle_message(msg)
        consumer.commit(msg)
    raise AssertionError(f"no llegó el evento {event_id}")


def _pongs(conn: psycopg.Connection, event_id: uuid.UUID) -> int:
    return conn.execute(
        "SELECT count(*) AS n FROM events WHERE causation_id = %s AND event_type = 'TestPong'",
        (event_id,),
    ).fetchone()["n"]


def test_redelivery_after_missing_confirmation(connect_db, settings):
    """Reentrega tras falta de confirmación"""
    event = _publish_ping(connect_db, settings)
    group = f"g-{uuid.uuid4().hex[:8]}"

    def failing(conn, ev):
        raise HandlerCrash("fallo antes de confirmar")

    first = _consumer(settings, group, failing)
    try:
        msg = _next_for(first, event.event_id)
        with pytest.raises(HandlerCrash):
            first.handle_message(msg)
    finally:
        first.close()  # sin commit del offset

    second = _consumer(settings, group)
    try:
        msg = _next_for(second, event.event_id)
        assert second.handle_message(msg) is Outcome.PROCESSED
        second.commit(msg)
    finally:
        second.close()
    assert _pongs(connect_db(), event.event_id) == 1


def test_kafka_redelivery(connect_db, settings):
    """Reentrega de Kafka"""
    event = _publish_ping(connect_db, settings)
    group = f"g-{uuid.uuid4().hex[:8]}"
    consumer = _consumer(settings, group)
    try:
        msg = _next_for(consumer, event.event_id)
        assert consumer.handle_message(msg) is Outcome.PROCESSED
        consumer.commit(msg)

        # Kafka entrega el mismo evento otra vez (republicación del outbox).
        conn = connect_db()
        seq = conn.execute(
            "SELECT seq FROM events WHERE event_id = %s", (event.event_id,)
        ).fetchone()["seq"]
        conn.commit()
        relay = OutboxRelay.from_settings(settings)
        try:
            relay.replay_from(seq)
            while relay.run_once() > 0:
                pass
        finally:
            relay.close()

        dup = _next_for(consumer, event.event_id)
        assert dup.offset() > msg.offset()
        assert consumer.handle_message(dup) is Outcome.DUPLICATE
        consumer.commit(dup)
        assert consumer.committed_offset(dup) == dup.offset() + 1
    finally:
        consumer.close()
    check = connect_db()
    assert _pongs(check, event.event_id) == 1
    rows = check.execute(
        "SELECT count(*) AS n FROM inbox WHERE consumer = %s AND event_id = %s",
        (group, event.event_id),
    ).fetchone()["n"]
    assert rows == 1


def test_crash_before_offset_commit(connect_db, settings):
    """Caída antes de confirmar el offset"""
    event = _publish_ping(connect_db, settings)
    group = f"g-{uuid.uuid4().hex[:8]}"

    first = _consumer(settings, group)
    try:
        msg = _next_for(first, event.event_id)
        assert first.handle_message(msg) is Outcome.PROCESSED  # COMMIT de PostgreSQL hecho
    finally:
        first.close()  # cae antes de confirmar el offset en Kafka

    second = _consumer(settings, group)
    try:
        msg = _next_for(second, event.event_id)
        assert second.handle_message(msg) is Outcome.DUPLICATE
        second.commit(msg)
    finally:
        second.close()
    assert _pongs(connect_db(), event.event_id) == 1


def test_unhandled_event_types_are_ignored_without_inbox(connect_db, settings):
    """Los tipos que el consumidor no procesa no se registran en el inbox"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    other = new_event(
        event_type="SomethingElse", correlation_id=incident, causation_id=None, producer="test", payload={}
    )
    conn = connect_db()
    with conn.transaction():
        append_events(conn, [other])
    _relay_all(settings)

    group = f"g-{uuid.uuid4().hex[:8]}"
    consumer = _consumer(settings, group)
    try:
        msg = _next_for(consumer, other.event_id)
        assert consumer.handle_message(msg) is Outcome.IGNORED
        consumer.commit(msg)
    finally:
        consumer.close()
    rows = connect_db().execute(
        "SELECT count(*) AS n FROM inbox WHERE consumer = %s", (group,)
    ).fetchone()["n"]
    assert rows == 0
