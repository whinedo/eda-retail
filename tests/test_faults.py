"""synthetic-data: inyección de fallos configurable y reproducible."""

from __future__ import annotations

import uuid

import pytest

from eda_retail.experiments import harness
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec
from eda_retail.step_executors.runtime import ExecuteStep
from eda_retail.synthetic.faults import (
    INJECTED_FAILURE,
    ConsumerCrashed,
    FaultPlan,
    FaultyDelivery,
    should_fail,
    step_delay,
)


class _Consumer:
    """Consumer de prueba: entrega los mensajes de una lista y anota qué procesa y confirma."""

    name = "executor-file_claim"

    def __init__(self, messages: list[str]) -> None:
        self.queue = list(messages)
        self.handled: list[str] = []
        self.committed: list[str] = []

    def poll(self, timeout: float):
        return self.queue.pop(0) if self.queue else None

    def handle_message(self, msg):
        self.handled.append(msg)
        return "processed"

    def commit(self, msg) -> None:
        self.committed.append(msg)


def _drain(delivery: FaultyDelivery) -> None:
    while delivery.run_once(0) is not None:
        pass


def test_event_duplication():
    """Duplicación de eventos"""
    consumer = _Consumer(["e1", "e2", "e3"])
    _drain(FaultyDelivery(consumer, FaultPlan(duplicates=3)))
    assert consumer.handled == ["e1"] * 3 + ["e2"] * 3 + ["e3"] * 3
    assert consumer.committed == ["e1", "e2", "e3"]  # el offset se confirma una vez, tras las entregas


def test_delivery_delay():
    """Retrasar la entrega: cada mensaje espera el retraso configurado antes de procesarse"""
    waits: list[float] = []
    consumer = _Consumer(["e1", "e2"])
    _drain(FaultyDelivery(consumer, FaultPlan(delay_seconds=0.5), sleep=waits.append))
    assert waits == [0.5, 0.5] and consumer.handled == ["e1", "e2"]


def test_consumer_crash_at_given_instant():
    """Caer un consumidor en un instante dado: tras K mensajes cae sin confirmar el último offset"""
    consumer = _Consumer(["e1", "e2", "e3"])
    delivery = FaultyDelivery(consumer, FaultPlan(crash_after={"executor-file_claim": 2}))
    delivery.run_once(0)
    with pytest.raises(ConsumerCrashed):
        delivery.run_once(0)
    assert consumer.handled == ["e1", "e2"] and consumer.committed == ["e1"]
    # Reiniciado (mismo plan, proceso nuevo): no vuelve a caer.
    restarted = FaultyDelivery(consumer, FaultPlan(crash_after={"executor-file_claim": 2}), crashed=delivery.crashed)
    _drain(restarted)
    assert consumer.committed == ["e1", "e3"]


def test_same_seed_same_failures():
    """Misma semilla, mismos fallos"""
    keys = [f"key-{i}" for i in range(500)]
    first = {k for k in keys if should_fail(42, "reserve_stock", k, 0.2)}
    second = {k for k in keys if should_fail(42, "reserve_stock", k, 0.2)}
    assert first == second
    assert 70 <= len(first) <= 130  # alrededor del 20 %
    assert first != {k for k in keys if should_fail(43, "reserve_stock", k, 0.2)}
    assert not any(should_fail(42, "reserve_stock", k, 0.0) for k in keys)
    assert all(should_fail(42, "reserve_stock", k, 1.0) for k in keys)


def test_failing_action_fails_chosen_steps_only():
    """Hacer fallar un tipo de paso con una probabilidad dada: falla sin ejecutar el efecto"""
    plan = FaultPlan(seed=42, fail_probability={"reserve_stock": 0.2})
    handled = []
    spec = harness.failing(CommandSpec(ExecuteStep, lambda conn, command, payload, prepared: handled.append(
        payload.idempotency_key) or Accepted([])), "inventory", plan)
    failed = 0
    for i in range(200):
        step = ExecuteStep(incident_id="I1", plan_version=1, step_id="S3", action="reserve_stock",
                           params={"order_id": "P1"}, idempotency_key=f"k{i}")
        command = CommandEnvelope(command_id=uuid.uuid4(), command_type="ExecuteStep", correlation_id="I1",
                                  causation_id=None, payload=step.model_dump())
        outcome = spec.handle(None, command, step, spec.prepare(command, step))
        if should_fail(42, "reserve_stock", step.idempotency_key, 0.2):
            [event] = outcome.events
            assert (event.payload["error"], event.payload["retryable"]) == (INJECTED_FAILURE, False)
            failed += 1
    assert len(handled) == 200 - failed and failed > 0


def _step(key: str, action: str = "file_claim") -> tuple[ExecuteStep, CommandEnvelope]:
    step = ExecuteStep(incident_id="I1", plan_version=1, step_id="S2", action=action,
                       params={"order_id": "P1"}, idempotency_key=key)
    return step, CommandEnvelope(command_id=uuid.uuid4(), command_type="ExecuteStep", correlation_id="I1",
                                 causation_id=None, payload=step.model_dump())


def test_deterministic_late_result():
    """Resultado tardío determinista: los mismos pasos file_claim retrasan su resultado la misma cantidad en las
    dos ejecuciones"""
    keys = [f"k{i}" for i in range(100)]
    first = [step_delay(42, "file_claim", k, 4.0) for k in keys]
    assert first == [step_delay(42, "file_claim", k, 4.0) for k in keys]
    assert all(2.0 <= d < 6.0 for d in first) and len(set(first)) > 90  # jitter en [0,5, 1,5) de 4 s
    assert first != [step_delay(43, "file_claim", k, 4.0) for k in keys]
    assert step_delay(42, "file_claim", "k1", 0.0) == 0.0


def test_delayed_step_result_is_appended_later():
    """El efecto se ejecuta en su transacción, pero sus eventos de resultado se entregan al cabo del retraso"""
    plan = FaultPlan(seed=42, step_delay_seconds={"file_claim": 4.0})
    deferred = []
    events = [object()]
    spec = harness.delaying(CommandSpec(ExecuteStep, lambda conn, command, payload, prepared: Accepted(
        list(events), {"ok": True})), "executor_file_claim", plan,
        lambda schema, command_id, evs, seconds: deferred.append((schema, command_id, evs, seconds)))
    step, command = _step("k1")
    outcome = spec.handle(None, command, step, None)
    assert outcome.events == [] and outcome.body == {"ok": True}
    [(schema, command_id, evs, seconds)] = deferred
    assert (schema, command_id, evs) == ("executor_file_claim", command.command_id, events)
    assert seconds == step_delay(42, "file_claim", "k1", 4.0)


def test_action_without_delay_keeps_its_result():
    plan = FaultPlan(seed=42, step_delay_seconds={"file_claim": 4.0})
    deferred = []
    spec = harness.delaying(CommandSpec(ExecuteStep, lambda conn, command, payload, prepared: Accepted([object()])),
                            "executor_notify_customer", plan, lambda *a: deferred.append(a))
    step, command = _step("k1", action="notify_customer")
    assert len(spec.handle(None, command, step, None).events) == 1 and deferred == []


def test_combination_of_faults():
    """Combinación de fallos: con la misma semilla, los mismos pasos fallan y se retrasan en las dos ejecuciones"""
    plan = FaultPlan(seed=42, duplicates=2, fail_probability={"reserve_stock": 0.5},
                     step_delay_seconds={"reserve_stock": 3.0})
    keys = [f"k{i}" for i in range(50)]

    def run() -> list[tuple[bool, float]]:
        return [(should_fail(plan.seed, "reserve_stock", k, 0.5), step_delay(plan.seed, "reserve_stock", k, 3.0))
                for k in keys]

    assert run() == run()


def test_fault_plan_rejects_invalid_values():
    """La configuración de fallos se valida: entregas de 1 a N, probabilidades entre 0 y 1, acciones permitidas"""
    with pytest.raises(ValueError):
        FaultPlan(duplicates=0)
    with pytest.raises(ValueError):
        FaultPlan(fail_probability={"reserve_stock": 1.5})
    with pytest.raises(ValueError):
        FaultPlan(fail_probability={"delete_order": 0.1})
    with pytest.raises(ValueError):
        FaultPlan(delay_seconds=-1)
    with pytest.raises(ValueError):
        FaultPlan(step_delay_seconds={"file_claim": -1.0})
    with pytest.raises(ValueError):
        FaultPlan(step_delay_seconds={"delete_order": 1.0})


def test_non_idempotent_service_repeats_the_effect():
    """Un servicio sin idempotencia (E1) repite el efecto con la misma clave y cuenta cada ejecución"""
    from eda_retail.step_executors.services import FakeService

    idempotent = FakeService("mail", "M")
    assert idempotent.call("k") == idempotent.call("k")
    assert idempotent.executions == {"k": 1}
    plain = FakeService("mail", "M", idempotent=False)
    assert plain.call("k") != plain.call("k")
    assert plain.executions == {"k": 2}


class FakeAdmin:
    def __init__(self):
        self.altered = []

    def alter_consumer_group_offsets(self, requests):
        class Done:
            def result(self, timeout):
                return None

        [request] = requests  # Kafka admite un solo grupo por petición
        self.altered.append((request.group_id,
                             [(tp.topic, tp.partition, tp.offset) for tp in request.topic_partitions]))
        return {request.group_id: Done()}


def test_reset_group_offsets_to_the_beginning():
    """Grupo reiniciado al principio: los offsets de cada grupo vuelven al 0 de cada partición"""
    from eda_retail.synthetic.faults import reset_group_offsets

    admin = FakeAdmin()
    reset_group_offsets(admin, ["interpreter", "orchestrator"], "eda.t", [2, 0, 1])
    parts = [("eda.t", 0, 0), ("eda.t", 1, 0), ("eda.t", 2, 0)]
    assert admin.altered == [("interpreter", parts), ("orchestrator", parts)]
