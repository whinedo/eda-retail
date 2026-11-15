"""Piezas del arnés de experimentos que no necesitan PostgreSQL ni Kafka (T11)."""

from __future__ import annotations

import uuid
from types import SimpleNamespace

import pytest

from eda_retail.experiments import harness
from eda_retail.infra.command_client import CommandUnavailable
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandResponse, CommandSpec
from eda_retail.step_executors.runtime import ExecuteStep
from eda_retail.synthetic.faults import INJECTED_FAILURE, FaultPlan, should_fail
from eda_retail.synthetic.generator import generate


def _invoke(commands, payload, command_id=None):
    return commands("http://x", "ExecuteStep", command_id=command_id or uuid.uuid4(), correlation_id="I-1",
                    causation_id=None, payload=payload, timeout=1)


@pytest.fixture
def executed(monkeypatch):
    calls = []

    def fake(conn, schema, command, spec, *, inbox_enabled=True):
        calls.append((schema, command.command_id, inbox_enabled))
        return CommandResponse(202, {"status": "accepted"})

    monkeypatch.setattr(harness, "execute_command", fake)
    return calls


def _commands(plan, *, inbox=True, crashed=None):
    target = harness.Target("executor_notify_customer", {"ExecuteStep": None}, inbox)
    return harness.InProcessCommands(None, {"http://x": target}, plan, crashed if crashed is not None else set())


PAYLOAD = {"action": "notify_customer", "step_id": "S1"}


def test_each_command_is_delivered_the_configured_times(executed):
    """E1: cada comando llega N veces con el mismo command_id (entrega al menos una vez)"""
    command_id = uuid.uuid4()
    _invoke(_commands(FaultPlan(duplicates=3), inbox=False), PAYLOAD, command_id)
    assert executed == [("executor_notify_customer", command_id, False)] * 3


def test_an_executor_crashes_after_committing_and_before_answering(executed):
    """E2: el ejecutor cae tras confirmar la mitad de sus pasos; la respuesta se pierde una vez"""
    crashed: set[str] = set()
    commands = _commands(FaultPlan(crash_after={"executor-notify_customer": 2}), crashed=crashed)

    _invoke(commands, PAYLOAD)
    with pytest.raises(CommandUnavailable):
        _invoke(commands, PAYLOAD)
    _invoke(commands, PAYLOAD)  # reiniciado: no vuelve a caer

    assert len(executed) == 3  # el paso de la caída se confirmó antes de caer
    assert commands.crashes == 1 and crashed == {"executor-notify_customer"}


def _failing_spec(plan):
    seen = []

    def prepare(command, payload):
        seen.append("prepare")
        return "prepared"

    def handle(conn, command, payload, prepared):
        seen.append(("handle", prepared))
        return Accepted([])

    spec = CommandSpec(ExecuteStep, handle, prepare=prepare)
    return harness.failing(spec, "executor-reissue_shipment", plan), seen


def _step(key: str) -> ExecuteStep:
    return ExecuteStep(incident_id="I-1", plan_version=1, step_id="S1", action="reissue_shipment",
                       params={"order_id": "P-1"}, idempotency_key=key)


def _key(plan, chosen: bool) -> str:
    return next(k for k in (f"K-{i}" for i in range(1000))
                if should_fail(plan.seed, "reissue_shipment", k, plan.fail_probability["reissue_shipment"]) == chosen)


def test_a_chosen_step_fails_without_preparing_the_effect():
    """Paso que falla: StepFailed injected_failure sin preparar ni ejecutar el efecto"""
    plan = FaultPlan(seed=1, fail_probability={"reissue_shipment": 0.5})
    spec, seen = _failing_spec(plan)
    step = _step(_key(plan, True))
    command = CommandEnvelope(command_id=uuid.uuid4(), command_type="ExecuteStep", correlation_id="I-1",
                              causation_id=None, payload=step.model_dump())

    outcome = spec.handle(None, command, step, spec.prepare(command, step))

    [failed] = outcome.events
    assert (failed.event_type, failed.payload["error"], failed.payload["retryable"]) == (
        "StepFailed", INJECTED_FAILURE, False)
    assert seen == []


def test_other_steps_run_normally():
    plan = FaultPlan(seed=1, fail_probability={"reissue_shipment": 0.5})
    spec, seen = _failing_spec(plan)
    step = _step(_key(plan, False))
    command = SimpleNamespace(correlation_id="I-1", command_id=uuid.uuid4())
    spec.handle(None, command, step, spec.prepare(command, step))
    assert seen == ["prepare", ("handle", "prepared")]


def test_seed_payload_empties_the_chosen_warehouses():
    """Almacenes vacíos: las existencias de esos almacenes se siembran a 0 con SeedSampleData"""
    data = generate(7, 4)
    payload = harness.seed_payload(data, (data.stock[0].warehouse,))
    assert {o["order_id"] for o in payload["orders"]} == {o.order_id for o in data.orders}
    emptied = [s for s in payload["stock"] if s["warehouse"] == data.stock[0].warehouse]
    assert emptied and all(s["available"] == 0 for s in emptied)
    assert set(payload["orders"][0]) == {"order_id", "customer_ref", "sku", "quantity", "amount", "carrier"}


def test_the_harness_planner_does_not_consult_the_carrier_by_default(monkeypatch):
    """E5 y E6 sin agente del transportista: el harness no consulta por A2A aunque POC_VERSION=2"""
    from eda_retail.config import Settings
    from eda_retail.experiments.harness import Harness, RunConfig
    from eda_retail.llm.fake import FakeLlmClient

    monkeypatch.setenv("POC_VERSION", "2")
    harness = Harness(Settings(), RunConfig(seed=1, incidents=1), llm=FakeLlmClient([]))
    harness.settings = Settings()
    assert harness._targets()["planner"].specs["CreatePlan"].prepare._carrier is None

    injected = object()
    harness = Harness(Settings(), RunConfig(seed=1, incidents=1), llm=FakeLlmClient([]), carrier=injected)
    harness.settings = Settings()
    assert harness._targets()["planner"].specs["CreatePlan"].prepare._carrier is injected
