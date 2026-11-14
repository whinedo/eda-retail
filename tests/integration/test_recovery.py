"""resilience: recuperación tras la caída de un ejecutor y aislamiento entre ejecutores."""

from __future__ import annotations

import time
import uuid
from collections import Counter

from eda_retail.executors.actions import ReserveStock
from eda_retail.executors.base import Effect, NoPreparation, Transient
from eda_retail.infra.consumer import EventConsumer
from eda_retail.infra.kafka import make_producer
from eda_retail.infra.relay import encode_message

from .command_helpers import events_of, open_incident
from .executor_helpers import ScriptedClaim, executor, requested_steps
from .test_executors import make_order
from .test_plans import create, plan_steps, step
from .test_saga_commands import advance


class CountingNotify(NoPreparation):
    """notify_customer que cuenta los efectos por paso y simula una caída tras `crash_after`."""

    action = "notify_customer"

    def __init__(self, crash_after: int | None = None) -> None:
        self.crash_after = crash_after
        self.done: Counter[str] = Counter()

    def execute(self, conn, request, prepared) -> Effect:
        if self.crash_after is not None and sum(self.done.values()) >= self.crash_after:
            raise SystemExit("caída del ejecutor")  # no es un EffectError: el proceso muere
        self.done[request.step_id] += 1
        message = f"M-{uuid.uuid4().hex[:8]}"
        return Effect({"message_id": message}, {"order_id": request.params["order_id"], "message_id": message})


def publish(settings, events) -> None:
    producer = make_producer(settings.kafka_bootstrap_servers)
    for i, ev in enumerate(events, start=1):
        producer.produce(settings.events_topic, key=ev.correlation_id.encode(), value=encode_message(i, ev),
                         headers=[("event_type", ev.event_type.encode())])
    assert producer.flush(10) == 0


def test_executor_crash_mid_plan(conn, settings):
    """Caída de un ejecutor a mitad de un plan"""
    incident, _ = open_incident(conn)
    assert create(conn, incident, [step(f"S{i}", "notify_customer", order_id="P1") for i in range(1, 5)]).accepted
    requested = [e for e in advance(conn, incident).events if e.event_type == "StepRequested"]
    publish(settings, requested)
    group = f"executor-notify-{uuid.uuid4().hex[:6]}"

    first = CountingNotify(crash_after=2)
    ex, _ = executor(first)
    consumer = EventConsumer.from_settings(settings, name=group, handlers=ex.handlers)
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            try:
                consumer.run_once(0.5)
            except SystemExit:
                break
    finally:
        consumer.close()
    assert sum(first.done.values()) == 2  # dos pasos completados antes de la caída

    second = CountingNotify()
    ex, _ = executor(second)
    consumer = EventConsumer.from_settings(settings, name=group, handlers=ex.handlers)
    deadline = time.monotonic() + 30
    try:
        while time.monotonic() < deadline:
            consumer.run_once(0.5)
            if all(s["status"] == "completed" for s in plan_steps(conn, incident).values()):
                break
    finally:
        consumer.close()
    assert sum(second.done.values()) == 2  # procesa los dos pendientes
    assert not set(first.done) & set(second.done)  # y no repite los completados
    completed = [e for e in events_of(conn, incident) if e["event_type"] == "StepCompleted"]
    assert sorted(e["payload"]["step_id"] for e in completed) == ["S1", "S2", "S3", "S4"]


def test_claims_service_down(conn):
    """Servicio de reclamaciones caído"""
    order = make_order(conn, stock=3)
    _, events = requested_steps(conn, [
        step("S1", "notify_customer", order_id=order), step("S2", "file_claim", order_id=order),
        step("S3", "reserve_stock", order_id=order),
    ])
    by_action = {e.payload["action"]: e for e in events}
    claims, _ = executor(ScriptedClaim([Transient()] * 2), max_attempts=2)  # sin respuesta
    notify, _ = executor(CountingNotify())
    reserve, _ = executor(ReserveStock())
    claims.process(conn, by_action["file_claim"])
    notify.process(conn, by_action["notify_customer"])
    reserve.process(conn, by_action["reserve_stock"])
    status = {sid: r["status"] for sid, r in plan_steps(conn, events[0].correlation_id).items()}
    assert status == {"S1": "completed", "S2": "failed", "S3": "completed"}  # avisos y reservas, con normalidad
    assert {claims.name, notify.name, reserve.name} == {
        "executor-file_claim", "executor-notify_customer", "executor-reserve_stock",
    }  # cada uno con su consumer group
