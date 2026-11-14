"""event-backbone: estructura común del evento, log inmutable y log en PostgreSQL."""

from __future__ import annotations

import uuid

import psycopg
import pytest
from pydantic import ValidationError

from eda_retail.events.envelope import EventEnvelope, new_event
from eda_retail.infra.event_store import append_events


def _event(incident_id: str, event_type: str = "ShipmentDelayed") -> EventEnvelope:
    return new_event(
        event_type=event_type,
        correlation_id=incident_id,
        causation_id=None,
        producer="test",
        payload={"incident_id": incident_id},
    )


def _count(conn: psycopg.Connection, sql: str, *params) -> int:
    return conn.execute(sql, params).fetchone()["n"]


def test_complete_event(conn):
    """Evento completo"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    event = _event(incident)
    with conn.transaction():
        [seq] = append_events(conn, [event])
    row = conn.execute("SELECT * FROM events WHERE seq = %s", (seq,)).fetchone()
    assert row["event_id"] == event.event_id
    assert row["correlation_id"] == incident
    assert _count(conn, "SELECT count(*) AS n FROM outbox WHERE seq = %s", seq) == 1


def test_event_without_correlation_id(conn):
    """Evento sin correlation_id"""
    raw = _event("I-x").model_dump()
    del raw["correlation_id"]
    with pytest.raises(ValidationError) as excinfo:
        EventEnvelope.model_validate(raw)
    assert "correlation_id" in str(excinfo.value)

    before = _count(conn, "SELECT count(*) AS n FROM events")
    with pytest.raises(ValueError, match="correlation_id"):
        with conn.transaction():
            append_events(conn, [raw])
    assert _count(conn, "SELECT count(*) AS n FROM events") == before


@pytest.mark.parametrize("statement", ["UPDATE", "DELETE"])
def test_attempt_to_modify_or_delete_event(conn, statement):
    """Intento de modificar un evento / Intento de borrar un evento"""
    event = _event(f"I-{uuid.uuid4().hex[:8]}")
    with conn.transaction():
        [seq] = append_events(conn, [event])
    original = conn.execute("SELECT * FROM events WHERE seq = %s", (seq,)).fetchone()
    conn.commit()

    sql = {
        "UPDATE": "UPDATE events SET producer = 'tampered' WHERE seq = %s",
        "DELETE": "DELETE FROM events WHERE seq = %s",
    }[statement]
    with pytest.raises(psycopg.errors.RaiseException, match="solo añadir"):
        with conn.transaction():
            conn.execute(sql, (seq,))
    assert conn.execute("SELECT * FROM events WHERE seq = %s", (seq,)).fetchone() == original


def test_truncate_is_rejected(conn):
    """Intento de borrar un evento (TRUNCATE)"""
    with pytest.raises(psycopg.errors.RaiseException, match="solo añadir"):
        with conn.transaction():
            conn.execute("TRUNCATE events CASCADE")


def test_seq_is_global_and_increasing(conn):
    """Log en PostgreSQL: seq global creciente"""
    events = [_event(f"I-{uuid.uuid4().hex[:8]}") for _ in range(3)]
    with conn.transaction():
        seqs = append_events(conn, events)
    assert seqs == sorted(seqs) and len(set(seqs)) == 3


def test_failed_transaction_writes_neither_event_nor_outbox(conn):
    """Publicación atómica: un cambio no confirmado no deja evento ni outbox"""
    event = _event(f"I-{uuid.uuid4().hex[:8]}")
    with pytest.raises(RuntimeError):
        with conn.transaction():
            append_events(conn, [event])
            raise RuntimeError("fallo antes del COMMIT")
    assert _count(conn, "SELECT count(*) AS n FROM events WHERE event_id = %s", event.event_id) == 0
    assert _count(conn, "SELECT count(*) AS n FROM outbox") == _count(
        conn, "SELECT count(*) AS n FROM events"
    )
