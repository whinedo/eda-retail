"""Inbox, effect_keys y dead letters en el esquema de cada componente (spec idempotency
"Inbox por consumidor", spec resilience "Dead letter queue"; design.md §2).

Sin PostgreSQL: conexión falsa que registra el SQL. Los escenarios contra la base real están en
tests/integration/test_schema_infra.py.
"""

from __future__ import annotations

import contextlib
import uuid

import pytest

from eda_retail.events.envelope import new_event
from eda_retail.infra import dead_letters, effect_keys, inbox
from eda_retail.infra.consumer import EventConsumer, Outcome


class _Result:
    def __init__(self, rowcount=1, row=None):
        self.rowcount = rowcount
        self._row = row

    def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self, rowcount=1, row=None):
        self.rowcount = rowcount
        self.row = row
        self.statements: list[tuple[str, tuple]] = []

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))
        return _Result(self.rowcount, self.row)


def test_register_uses_the_component_inbox():
    """Cada consumidor registra los eventos procesados en su propia tabla <componente>.inbox"""
    conn = FakeConn()
    event_id = uuid.uuid4()

    assert inbox.register(conn, "planner", event_id, schema="planner") is True

    [(sql, params)] = conn.statements
    assert sql.startswith("INSERT INTO planner.inbox (message_id, kind)")
    assert "ON CONFLICT DO NOTHING" in sql
    assert params == (event_id, "event")


def test_register_reports_a_duplicate():
    """Reentrega de Kafka: el insert en el inbox no inserta ninguna fila"""
    assert inbox.register(FakeConn(rowcount=0), "planner", uuid.uuid4(), schema="planner") is False


def test_seen_reads_the_component_inbox():
    conn = FakeConn(row={"x": 1})
    assert inbox.seen(conn, "planner", uuid.uuid4(), schema="planner") is True
    assert "FROM planner.inbox WHERE message_id" in conn.statements[0][0]


def test_inbox_switch_still_applies_per_schema():
    """INBOX_ENABLED=false (E1): no escribe y trata todo como nuevo"""
    conn = FakeConn(rowcount=0)
    assert inbox.register(conn, "planner", uuid.uuid4(), schema="planner", enabled=False) is True
    assert conn.statements == []




def test_effect_keys_live_in_the_executor_schema():
    conn = FakeConn(rowcount=1)
    claim = effect_keys.acquire(
        conn, key="k", incident_id="I-1", step_id="s1", action="file_claim", lock_seconds=30,
        schema="executor_file_claim",
    )
    effect_keys.complete(conn, "k", {"claim_id": "C-1"}, schema="executor_file_claim")
    assert claim.status is effect_keys.ClaimStatus.ACQUIRED
    assert conn.statements[0][0].startswith("INSERT INTO executor_file_claim.effect_keys")
    assert conn.statements[1][0].startswith("UPDATE executor_file_claim.effect_keys")


def test_effect_keys_report_a_completed_key_in_the_schema():
    conn = FakeConn(rowcount=0, row={"status": "completed", "result": {"claim_id": "C-1"}, "locked": False})
    claim = effect_keys.acquire(
        conn, key="k", incident_id="I-1", step_id="s1", action="file_claim", lock_seconds=30,
        schema="executor_file_claim",
    )
    assert claim.status is effect_keys.ClaimStatus.COMPLETED
    assert "FROM executor_file_claim.effect_keys" in conn.statements[1][0]


def test_dead_letter_goes_to_the_component_table():
    conn = FakeConn(row={"id": 9})
    assert dead_letters.record(
        conn, consumer="planner", reason="invalid_schema", event_id=None, correlation_id="I-1",
        raw={"x": 1}, schema="planner",
    ) == 9
    assert conn.statements[0][0].startswith("INSERT INTO planner.dead_letters")


@pytest.mark.parametrize("bad", ["Planner", "planner;drop"])
def test_invalid_schema_is_rejected(bad):
    with pytest.raises(ValueError):
        inbox.register(FakeConn(), "planner", uuid.uuid4(), schema=bad)


class _Kafka:
    def subscribe(self, topics):
        self.topics = topics


def test_consumer_registers_events_in_its_component_inbox():
    """El consumidor de un componente usa el inbox de su esquema"""
    conn = FakeConn()
    handled = []
    consumer = EventConsumer(
        "planner", conn, _Kafka(), "eda.events", {"ShipmentDelayed": lambda c, e: handled.append(e)},  # type: ignore[arg-type]
        schema="planner",
    )
    event = new_event(event_type="ShipmentDelayed", correlation_id="I-1", causation_id=None, producer="t", payload={})

    assert consumer.process(event) is Outcome.PROCESSED
    assert conn.statements[0][0].startswith("INSERT INTO planner.inbox")
    assert handled == [event]
