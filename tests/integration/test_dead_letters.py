"""resilience: dead letter queue (topic eda.dlq y tabla dead_letters)."""

from __future__ import annotations

import contextlib
import json
import uuid
from dataclasses import replace

from confluent_kafka.admin import AdminClient, NewTopic

from eda_retail.command.escalation import escalation_hook
from eda_retail.events.envelope import new_event
from eda_retail.executors.base import Transient, dead_letter_on_exhaustion
from eda_retail.infra.consumer import EventConsumer, Outcome
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.relay import OutboxRelay

from .command_helpers import events_of, handler
from .executor_helpers import ScriptedClaim, claim_plan, executor, requested_steps
from .kafka_helpers import read_all


def produce_raw(settings, key: str, value: bytes) -> None:
    producer = make_producer(settings.kafka_bootstrap_servers)
    producer.produce(settings.events_topic, key=key.encode(), value=value,
                     headers=[("event_type", b"StepRequested")])
    assert producer.flush(10) == 0


def consume_one(consumer: EventConsumer, attempts: int = 60) -> Outcome:
    for _ in range(attempts):
        if (outcome := consumer.run_once(0.5)) is not None:
            return outcome
    raise AssertionError("no llegó ningún mensaje")


def dead_letters_of(conn, correlation_id: str | None, consumer: str) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM dead_letters WHERE consumer = %s AND correlation_id IS NOT DISTINCT FROM %s",
        (consumer, correlation_id),
    ).fetchall()
    conn.commit()
    return rows


def claim_consumer(settings, name: str) -> EventConsumer:
    ex, _ = executor(ScriptedClaim())
    return EventConsumer.from_settings(
        settings, name=name, handlers=ex.handlers, on_dead_letter=escalation_hook(handler(), producer=name),
    )


def test_event_with_invalid_schema(conn, settings):
    """Evento con esquema inválido"""
    incident, [_, claim] = requested_steps(conn, claim_plan())
    payload = {k: v for k, v in claim.payload.items() if k != "idempotency_key"}  # falta un campo obligatorio
    bad = new_event(event_type="StepRequested", correlation_id=incident, causation_id=None, producer="test",
                    payload=payload)
    produce_raw(settings, incident, bad.model_dump_json().encode())
    name = f"executor-{uuid.uuid4().hex[:6]}"
    consumer = claim_consumer(settings, name)
    try:
        assert consume_one(consumer) == Outcome.DEAD_LETTER
    finally:
        consumer.close()
    [dead] = dead_letters_of(conn, incident, name)
    assert (dead["reason"], dead["event_id"]) == ("invalid_schema", bad.event_id)
    assert dead["raw_message"]["payload"] == payload
    [escalated] = [e for e in events_of(conn, incident) if e["event_type"] == "IncidentEscalated"]
    assert escalated["payload"]["reason"] == "invalid_schema"

    dlq = f"test.dlq.{uuid.uuid4().hex[:8]}"
    admin = AdminClient({"bootstrap.servers": settings.kafka_bootstrap_servers})
    for fut in admin.create_topics([NewTopic(dlq, num_partitions=1, replication_factor=1)]).values():
        fut.result(timeout=30)
    try:
        relay = OutboxRelay.from_settings(replace(settings, dlq_topic=dlq))
        try:
            while relay.publish_dead_letters() > 0:
                pass
        finally:
            relay.close()
        published = [m for m in read_all(settings.kafka_bootstrap_servers, dlq, expected=50, timeout=5)
                     if m.value["event_id"] == str(bad.event_id)]
        assert [(m.value["reason"], m.key) for m in published] == [("invalid_schema", incident)]
    finally:
        for fut in admin.delete_topics([dlq]).values():
            with contextlib.suppress(Exception):
                fut.result(timeout=30)
    row = conn.execute("SELECT published_at FROM dead_letters WHERE id = %s", (dead["id"],)).fetchone()
    conn.commit()
    assert row["published_at"] is not None


def test_message_that_is_not_json(conn, settings):
    """Un mensaje que no es JSON va a la dead letter queue sin bloquear al consumidor"""
    produce_raw(settings, "x", b"{esto no es json")
    name = f"executor-{uuid.uuid4().hex[:6]}"
    consumer = claim_consumer(settings, name)
    try:
        assert consume_one(consumer) == Outcome.DEAD_LETTER
    finally:
        consumer.close()
    [dead] = dead_letters_of(conn, None, name)
    assert dead["reason"] == "invalid_schema"
    assert dead["raw_message"] == {"raw": "{esto no es json"}


def test_retries_exhausted_go_to_dead_letter(conn):
    """Un StepRequested que agota sus reintentos pasa a la dead letter queue"""
    incident, [_, claim] = requested_steps(conn, claim_plan())
    ex, _ = executor(ScriptedClaim([Transient()] * 3), on_exhausted=dead_letter_on_exhaustion("executor-file_claim"))
    assert ex.process(conn, claim) == Outcome.PROCESSED
    [dead] = dead_letters_of(conn, incident, "executor-file_claim")
    assert (dead["reason"], dead["event_id"]) == ("retries_exhausted", claim.event_id)
    assert dead["raw_message"]["error"] == "timeout"
    assert json.dumps(dead["raw_message"]["payload"]["step_id"]) == '"S2"'
