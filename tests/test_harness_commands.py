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
    return harness.InProcessCommands(None, {"http://x": target}, plan, crashed if crashed is not None else set(),
                                     connect=lambda: None)


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


STEP = {"action": "notify_customer", "step_id": "S1", "idempotency_key": "K1"}


def test_same_command_from_several_threads(executed):
    """Mismo comando desde varios hilos: el mismo comando desde N hilos a la vez, con el mismo command_id; se anotan
    las respuestas de cada entrega"""
    command_id = uuid.uuid4()
    commands = _commands(FaultPlan(concurrent_commands={"ExecuteStep": 4}))
    _invoke(commands, STEP, command_id)
    assert executed == [("executor_notify_customer", command_id, True)] * 4
    [delivery] = commands.deliveries
    assert (delivery.command_id, delivery.idempotency_key, len(delivery.responses)) == (command_id, "K1", 4)
    assert harness.distinct_responses(commands.deliveries) == 1


def test_reinvocation_with_another_command_id(executed):
    """Reinvocación con otro command_id: el ExecuteStep de esas acciones llega también, a la vez, con un command_id
    nuevo y la misma idempotency_key (una reinvocación de la saga)"""
    command_id = uuid.uuid4()
    commands = _commands(FaultPlan(reinvoke_steps=("notify_customer",)))
    _invoke(commands, STEP, command_id)
    ids = {c for _, c, _ in executed}
    assert len(executed) == 2 and command_id in ids and len(ids) == 2
    assert {d.idempotency_key for d in commands.deliveries} == {"K1"}
    _invoke(commands, PAYLOAD | {"action": "file_claim", "idempotency_key": "K2"})
    assert len(executed) == 3  # file_claim no se reinvoca


@pytest.mark.parametrize("inbox", [True, False])
def test_entry_commands_keep_one_delivery_with_inbox(executed, inbox):
    """Comandos de entrada del arnés (datos de ejemplo, avisos): una entrega con inbox aunque el plan duplique los
    comandos o el sistema no use inbox (comportamiento anterior a E22)"""
    command = CommandEnvelope(command_id=uuid.uuid4(), command_type="ExecuteStep", correlation_id="I-1",
                              causation_id=None, payload=STEP)
    commands = _commands(FaultPlan(duplicates=3), inbox=inbox)
    commands.deliver(harness.Target("intake", {"ExecuteStep": None}, inbox), command, entry=True)
    assert executed == [("intake", command.command_id, True)]


def test_distinct_responses_per_command_id():
    """Respuestas distintas por command_id: el máximo de respuestas diferentes que recibió un mismo command_id"""
    c = uuid.uuid4()
    same = harness.CommandDelivery("ExecuteStep", c, "notify_customer", "K", [(202, {"a": 1}), (202, {"a": 1})])
    other = harness.CommandDelivery("ExecuteStep", c, "notify_customer", "K", [(422, {"a": 2})])
    assert harness.distinct_responses([same]) == 1
    assert harness.distinct_responses([same, other]) == 2


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


# --- Servidor de correo MCP en el arnés (POC_VERSION=2) ---------------------------------------


def test_version_one_runs_without_a_mail_server():
    """POC_VERSION=1: el arnés no monta el servidor MCP; notify_customer usa el servicio simulado"""
    assert harness.mail_server_for(harness.Services(), 1) is None


@pytest.mark.parametrize(("idempotent", "repeated"), [(True, 0), (False, 1)])
def test_version_two_mail_goes_through_the_in_process_mcp_server(idempotent, repeated):
    """POC_VERSION=2 sin proceso aparte: el correo pasa por un servidor MCP en proceso respaldado por el
    servicio simulado, así E1 sigue contando los envíos repetidos cuando la idempotencia está desactivada"""
    from eda_retail.executor_notify_customer.mail_client import McpMailClient, SendEmailInput

    services = harness.Services()
    services.mail.idempotent = idempotent
    client = McpMailClient(harness.mail_server_for(services, 2))
    request = SendEmailInput(customer_ref="CUST-0042", subject="Su pedido", body="Llega tarde.", idempotency_key="K1")
    first, again = client.send_email(request), client.send_email(request)
    assert first.message_id.startswith("M-")
    assert (first.message_id == again.message_id) is idempotent
    assert harness.service_duplicates(services)["notify_customer"] == repeated


# --- Agente del transportista A2A en el arnés (POC_VERSION=2) ----------------------------------


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


def test_duplicated_commands_answer_with_the_last_delivery(monkeypatch):
    """E1: con `duplicates`, la saga recibe la respuesta de la última entrega (sin inbox, el segundo `CreatePlan` se
    rechaza y la saga escala con `command_rejected`)"""
    answers = iter([CommandResponse(202, {"status": "accepted"}), CommandResponse(422, {"reason": "plan_exists"})])
    monkeypatch.setattr(harness, "execute_command", lambda *a, **k: next(answers))
    reply = _invoke(_commands(FaultPlan(duplicates=2), inbox=False), {"action": "notify_customer"})
    assert (reply.status_code, reply.body) == (422, {"reason": "plan_exists"})


def test_busy_claims_are_counted_and_retried():
    """E22: un trabajo que encuentra su clave `in_progress` se cuenta y el error sigue para que el trabajo se
    reintente"""
    from eda_retail.infra.effect_keys import EffectInProgress

    def handle(conn, command, payload, prepared):
        raise EffectInProgress("K en curso")

    counted = []
    spec = harness.counting_busy(CommandSpec(ExecuteStep, handle), lambda: counted.append(1))
    with pytest.raises(EffectInProgress):
        spec.handle(None, None, None, None)
    assert counted == [1]


# --- Ejecutor caído o colgado (E12) ------------------------------------------------------------


def _outage_commands(fault, clock, slept):
    from eda_retail.synthetic.faults import FaultPlan as Plan

    target = harness.Target("executor_file_claim", {"ExecuteStep": None}, True)
    return harness.InProcessCommands(None, {"http://x": target}, Plan(executor_faults=(fault,)), set(),
                                     connect=lambda: None, clock=clock, sleep=slept.append)


CLAIM = {"action": "file_claim", "step_id": "S2", "idempotency_key": "K2"}


def test_executor_down_rejects_the_connection(executed):
    """Ejecutor caído: la llamada falla al momento con CommandUnavailable y el comando no llega al ejecutor"""
    from eda_retail.synthetic.faults import ExecutorFault

    slept = []
    commands = _outage_commands(ExecutorFault("executor_file_claim", "down"), lambda: 0.0, slept)
    with pytest.raises(CommandUnavailable):
        _invoke(commands, CLAIM)
    assert executed == [] and slept == []


def test_hung_executor_waits_the_timeout_and_keeps_the_request(executed):
    """Ejecutor colgado (spec synthetic-data): con `COMMAND_HTTP_TIMEOUT_S` de 5 s, la petición vence a los 5 s con
    CommandUnavailable; se anota el tiempo de espera y la petición queda aceptada sin respuesta. Que el dispatcher la
    reprograme con backoff y el mismo `command_id` lo prueba
    `test_unavailable_system_is_retried_with_the_same_command_id` (integración)."""
    from eda_retail.synthetic.faults import ExecutorFault

    slept = []
    commands = _outage_commands(ExecutorFault("executor_file_claim", "hang"), lambda: 0.0, slept)
    command_id = uuid.uuid4()
    for _ in range(2):  # el dispatcher reintenta con el mismo command_id
        with pytest.raises(CommandUnavailable):
            commands("http://x", "ExecuteStep", command_id=command_id, correlation_id="I-1", causation_id=None,
                     payload=CLAIM, timeout=5)
    assert executed == [] and slept == [5, 5]
    assert commands.hang_wait_s == {"executor_file_claim": 10}
    assert not commands.awaiting_recovery()  # colgado para siempre: no se espera a que vuelva
    assert [c.command_id for c in commands.backlog["executor_file_claim"]] == [command_id]


def test_recovered_executor_answers_its_backlog_once(executed):
    """Recuperación del ejecutor: al terminar el intervalo, el ejecutor atiende una vez lo que tenía aceptado, y los
    comandos siguientes llegan con normalidad"""
    from eda_retail.synthetic.faults import ExecutorFault

    clock, slept = [0.0], []
    commands = _outage_commands(ExecutorFault("executor_file_claim", "hang", duration_s=5), lambda: clock[0], slept)
    command_id = uuid.uuid4()
    with pytest.raises(CommandUnavailable):
        _invoke(commands, CLAIM, command_id)
    assert commands.flush_recovered(lambda c: "current") == {}  # aún colgado
    assert commands.awaiting_recovery()  # el arnés sigue hasta atenderlo
    clock[0] = 6.0
    assert commands.flush_recovered(lambda c: "stale") == {"stale": 1}
    assert commands.flush_recovered(lambda c: "stale") == {}  # una sola vez
    assert not commands.awaiting_recovery()
    _invoke(commands, CLAIM)
    assert executed[0][1] == command_id and len(executed) == 2


def _capacity_commands(known: bool, open_jobs: int):
    target = harness.Target("executor_file_claim", {"ExecuteStep": None}, True)
    return harness.InProcessCommands(None, {"http://x": target},
                                     FaultPlan(executor_capacity={"executor_file_claim": 3}), set(),
                                     connect=lambda: None, jobs=lambda schema, command_id: (known, open_jobs))


def test_full_executor_answers_503(executed):
    """Ejecutor lleno: con tantos trabajos abiertos como su capacidad, un `ExecuteStep` nuevo falla con
    `CommandUnavailable` sin entregarse, y el informe cuenta un 503 del ejecutor"""
    commands = _capacity_commands(known=False, open_jobs=3)
    with pytest.raises(CommandUnavailable, match="503"):
        _invoke(commands, CLAIM)
    assert executed == []
    assert commands.unavailable_503 == {"executor_file_claim": 1}


def test_redelivery_to_a_full_executor_is_delivered(executed):
    """Reentrega con el ejecutor lleno: un `ExecuteStep` cuyo trabajo ya existe se entrega al command handler"""
    commands = _capacity_commands(known=True, open_jobs=3)
    _invoke(commands, CLAIM)
    assert len(executed) == 1 and commands.unavailable_503 == {}


def test_executor_below_capacity_accepts(executed):
    """Ejecutor saturado (control): por debajo de su capacidad, el `ExecuteStep` nuevo se entrega"""
    commands = _capacity_commands(known=False, open_jobs=2)
    _invoke(commands, CLAIM)
    assert len(executed) == 1 and commands.unavailable_503 == {}
