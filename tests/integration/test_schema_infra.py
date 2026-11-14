"""Inbox, effect_keys y dead letters por componente contra PostgreSQL real."""

from __future__ import annotations

import uuid

from eda_retail.events.envelope import new_event
from eda_retail.infra import effect_keys, inbox
from eda_retail.infra.consumer import EventConsumer, Outcome


class _Kafka:
    def subscribe(self, topics):
        pass


def test_component_inbox_discards_a_redelivered_event(conn):
    """Reentrega de Kafka: el inbox del componente no inserta otra fila"""
    event_id = uuid.uuid4()
    with conn.transaction():
        assert inbox.register(conn, "planner", event_id, schema="planner")
    with conn.transaction():
        assert not inbox.register(conn, "planner", event_id, schema="planner")
    assert inbox.seen(conn, "planner", event_id, schema="planner")
    assert not inbox.seen(conn, "replanner", event_id, schema="replanner")  # cada componente, su inbox


def test_effect_key_is_acquired_once_per_executor_schema(conn, connect_db):
    """Dos ejecutores a la vez: solo uno obtiene la clave"""
    key = f"k-{uuid.uuid4().hex}"
    args = {"key": key, "incident_id": "I-1", "step_id": "s1", "action": "file_claim", "lock_seconds": 60,
            "schema": "executor_file_claim"}
    other = connect_db()
    with conn.transaction():
        first = effect_keys.acquire(conn, **args)
    with other.transaction():
        second = effect_keys.acquire(other, **args)
    assert first.status is effect_keys.ClaimStatus.ACQUIRED
    assert second.status is effect_keys.ClaimStatus.BUSY
    with conn.transaction():
        effect_keys.complete(conn, key, {"claim_id": "C-1"}, schema="executor_file_claim")
    with other.transaction():
        again = effect_keys.acquire(other, **args)
    assert again.status is effect_keys.ClaimStatus.COMPLETED and again.result == {"claim_id": "C-1"}


def test_consumer_processes_once_with_its_schema(conn):
    handled = []
    consumer = EventConsumer(
        "planner", conn, _Kafka(), "eda.events", {"ShipmentDelayed": lambda c, e: handled.append(e)},  # type: ignore[arg-type]
        schema="planner",
    )
    event = new_event(event_type="ShipmentDelayed", correlation_id="I-1", causation_id=None, producer="t", payload={})
    assert consumer.process(event) is Outcome.PROCESSED
    assert consumer.process(event) is Outcome.DUPLICATE
    assert handled == [event]
