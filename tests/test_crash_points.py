"""Caída en un punto de la transacción (spec synthetic-data, «Caída en un punto de la transacción»; odd/tasks/
e8-crash-matrix.md T1, S3 y S5).

Sin PostgreSQL ni Kafka: la base falsa de `tests/test_command_jobs.py` (inbox, outbox y `command_jobs` con rollback),
un broker falso que vuelve a entregar desde el último offset confirmado y un relay con conexión y productor falsos.
"""

from __future__ import annotations

import contextlib
import copy
import json
import time
import uuid
from collections import Counter
from types import SimpleNamespace

import pytest
from pydantic import BaseModel, ConfigDict

from eda_retail.config import Settings
from eda_retail.events.envelope import new_event
from eda_retail.experiments import harness
from eda_retail.infra import commands
from eda_retail.infra.command_client import CommandUnavailable
from eda_retail.infra.commands import Accepted, CommandSpec
from eda_retail.infra.consumer import Outcome
from eda_retail.infra.outbox import ComponentRelay, RelayDeliveryError
from eda_retail.interpreter import consumer as interpreter_consumer
from eda_retail.llm.fake import CountingLlmClient, FakeLlmClient
from eda_retail.llm.generation import LlmRequest
from eda_retail.rules import BusinessRules
from eda_retail.synthetic.faults import (
    AFTER_COMMIT_BEFORE_OFFSET,
    AFTER_COMMIT_BEFORE_RESPONSE,
    BEFORE_TX,
    RELAY_AFTER_PUBLISH_BEFORE_MARK,
    ConsumerCrashed,
    CrashPointReached,
    CrashPoints,
    FaultPlan,
    FaultyDelivery,
    WorkerCrashed,
)
from tests.test_command_jobs import FakeDb, Payload, Work, _command


# -- dobles ------------------------------------------------------------------------------------
class _Db(FakeDb):
    """La base falsa de los trabajos, con lo que espera una conexión que el worker abre y cierra."""

    closed = False

    def close(self) -> None:
        pass


class _Msg:
    def __init__(self, offset: int, event) -> None:
        self._offset, self.event = offset, event

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return self._offset


class _Broker:
    """Una partición: cada consumidor nuevo lee desde el último offset confirmado (reentrega tras una caída)."""

    def __init__(self, events) -> None:
        self.messages = [_Msg(i, e) for i, e in enumerate(events)]
        self.committed = 0


class _Consumer:
    """Consumidor de Kafka falso; `handle` es la unidad de trabajo del componente."""

    def __init__(self, name: str, broker: _Broker, handle) -> None:
        self.name, self._broker, self._handle = name, broker, handle
        self._next = broker.committed
        self.closed = False

    def poll(self, timeout):
        if self._next >= len(self._broker.messages):
            return None
        msg = self._broker.messages[self._next]
        self._next += 1
        return msg

    def handle_message(self, msg):
        return self._handle(msg)

    def commit(self, msg) -> None:
        self._broker.committed = msg.offset() + 1

    def close(self) -> None:
        self.closed = True


def _bare(faults: FaultPlan, **config) -> harness.Harness:
    h = harness.Harness.__new__(harness.Harness)
    h.config = harness.RunConfig(seed=1, incidents=1, faults=faults, **config)
    h.settings = Settings()
    h._init_threads()
    h._crashed = set()
    h._failures = Counter()
    h.consumer_crashes = h.poison_messages = h.commit_failures = h.component_crashes = 0
    h.crash_points = CrashPoints(faults, crashed=h._crashed)
    h._closables = []
    return h


def _drive(h: harness.Harness, i: int = 0) -> None:
    """Como un turno de `Harness.step`: entrega hasta vaciar el topic; una caída reinicia el consumidor."""
    delivery, factory = h._deliveries[i]
    while True:
        try:
            if delivery.run_once(0) is None:
                return
        except Exception as exc:  # noqa: BLE001 — como `step`
            h._on_crash(i, delivery, factory, exc)


def _wait(condition, seconds: float = 5.0) -> bool:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if condition():
            return True
        time.sleep(0.01)
    return False


class _AnyPayload(BaseModel):
    model_config = ConfigDict(extra="allow")


# -- validación del plan -----------------------------------------------------------------------
def test_a_point_that_does_not_apply_is_rejected():
    """Punto que no aplica"""
    with pytest.raises(ValueError, match="`before_tx` no aplica al orquestador"):
        FaultPlan(crash_at={("orchestrator", BEFORE_TX): 1})


@pytest.mark.parametrize(("crash_at", "message"), [
    ({("event_log", AFTER_COMMIT_BEFORE_OFFSET): 1}, "no aplica a event_log"),
    ({("incident_status", AFTER_COMMIT_BEFORE_OFFSET): 1}, "no aplica a incident_status"),
    ({("event_log", RELAY_AFTER_PUBLISH_BEFORE_MARK): 1}, "no aplica a event_log"),
    ({("executor_file_claim", BEFORE_TX): 1}, "no aplica a executor_file_claim"),
    ({("intake", "after_effect"): 1}, "punto de caída desconocido: after_effect"),
    ({("planner", BEFORE_TX): 0}, "crash_at debe ser al menos 1"),
])
def test_crash_at_rejects_points_outside_their_components(crash_at, message):
    """El plan rechaza un punto en un componente al que no aplica, un punto desconocido y un K menor que 1"""
    with pytest.raises(ValueError, match=message):
        FaultPlan(crash_at=crash_at)


def test_crash_at_accepts_every_point_where_it_applies():
    plan = FaultPlan(crash_at={("interpreter", BEFORE_TX): 3, ("replanner", BEFORE_TX): 1,
                               ("orchestrator", AFTER_COMMIT_BEFORE_OFFSET): 5,
                               ("planner", AFTER_COMMIT_BEFORE_RESPONSE): 2, ("intake", AFTER_COMMIT_BEFORE_RESPONSE): 2,
                               ("executor_issue_refund", AFTER_COMMIT_BEFORE_RESPONSE): 1,
                               ("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK): 4})
    assert len(plan.crash_at) == 7


def test_crash_points_crash_once_at_unit_k_and_record_the_restart():
    """Cada caída queda registrada con su componente, punto, unidad y hora, y la hora de la unidad siguiente"""
    clock = iter([10.0, 11.0, 12.0, 13.0]).__next__
    points = CrashPoints(FaultPlan(crash_at={("planner", BEFORE_TX): 2}), clock=clock)
    assert [points.decide("planner", BEFORE_TX) for _ in range(4)] == [None, 2, None, None]
    [record] = points.records
    assert (record.component, record.point, record.unit, record.at, record.resumed_at) == (
        "planner", BEFORE_TX, 2, 11.0, 12.0)
    assert points.units[("planner", BEFORE_TX)] == 4


def test_crash_points_count_units_without_a_crash():
    """Sin `crash_at` no cae nadie, pero cuenta las unidades (E8 elige K con las de `baseline`)"""
    points = CrashPoints(FaultPlan())
    points.hit("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK, 3)
    points.hit("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK, 2)
    assert points.units[("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK)] == 5 and points.records == []


def test_crash_points_crash_in_the_batch_that_holds_unit_k():
    points = CrashPoints(FaultPlan(crash_at={("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK): 4}))
    points.hit("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK, 3)
    with pytest.raises(CrashPointReached, match="unidad 4"):
        points.hit("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK, 3)
    points.hit("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK, 3)  # reiniciado: no vuelve a caer


def test_counting_llm_client_counts_calls_per_agent():
    """La inyección cuenta las llamadas en el cliente del LLM, por agente"""
    inner = FakeLlmClient(responder=lambda r: "ok")
    llm = CountingLlmClient(inner)
    for agent in ("interpreter", "planner", "interpreter"):
        llm.complete(LlmRequest(agent=agent, incident_id="I-1", system="s", prompt="p", schema={}))
    assert llm.calls == Counter({"interpreter": 2, "planner": 1})
    assert llm.model == inner.model and len(llm.requests) == 3


# -- before_tx en el intérprete ----------------------------------------------------------------
def _interpreter(monkeypatch, llm, after_prepare):
    """El `InterpretNotice` real, con `execute_command` real sobre la base falsa; el LLM y el handler, simulados."""

    class Preparer:
        def __init__(self, llm_, rules, **kw) -> None:
            self._llm = llm_

        def __call__(self, command, payload):
            return self._llm.complete(LlmRequest(agent="interpreter", incident_id=command.correlation_id,
                                                 system="s", prompt="p", schema={})).text

    opened: list[str] = []

    def open_incident(settings):
        def handle(conn, command, payload, prepared):
            opened.append(command.correlation_id)
            return Accepted([])
        return handle

    monkeypatch.setattr(interpreter_consumer, "NoticePreparer", Preparer)
    monkeypatch.setattr(interpreter_consumer, "open_incident", open_incident)
    monkeypatch.setattr(interpreter_consumer, "OpenIncident", _AnyPayload)
    notice = interpreter_consumer.InterpretNotice(Settings(), llm, BusinessRules(), after_prepare=after_prepare)
    return notice, opened


def _notices(n: int):
    return [SimpleNamespace(event_id=uuid.uuid4(), payload={"carrier_notice_id": f"N-{i}"}) for i in range(n)]


def test_a_consumer_crashes_before_its_transaction(monkeypatch):
    """Caída de un consumidor antes de la transacción"""
    h = _bare(FaultPlan(crash_at={("interpreter", BEFORE_TX): 3}))
    llm = CountingLlmClient(FakeLlmClient(responder=lambda r: "ok"))
    notice, opened = _interpreter(monkeypatch, llm, h._interpreter_before_tx)
    db, broker = _Db("interpreter"), _Broker(_notices(3))

    def make():
        return _Consumer("interpreter", broker, lambda msg: notice.process(db, msg.event))

    first = make()
    h._deliveries = [(FaultyDelivery(first, h.config.faults, crashed=h._crashed, crash_points=h.crash_points), make)]
    _drive(h)

    assert first.closed and h.consumer_crashes == 1 and h.poison_messages == 0
    assert llm.calls["interpreter"] == 4  # el tercer aviso llama al LLM dos veces
    assert len(opened) == 3 and len(set(opened)) == 3  # la incidencia se abre una sola vez
    assert broker.committed == 3
    [record] = h.crash_points.records
    assert (record.component, record.point, record.unit) == ("interpreter", BEFORE_TX, 3)
    assert record.resumed_at is not None


def test_the_interpreter_without_the_hook_prepares_as_before(monkeypatch):
    """Sin `after_prepare`, el intérprete usa su `NoticePreparer` tal cual"""
    seen = []
    monkeypatch.setattr(interpreter_consumer, "execute_command",
                        lambda conn, schema, command, spec: seen.append(spec.prepare) or SimpleNamespace(replayed=False))
    notice = interpreter_consumer.InterpretNotice(Settings(), FakeLlmClient(), BusinessRules())
    assert notice.process(None, _notices(1)[0]) is Outcome.PROCESSED
    assert isinstance(seen[0], interpreter_consumer.NoticePreparer)


def test_the_harness_wires_the_before_tx_hook_into_the_interpreter(monkeypatch):
    h = _bare(FaultPlan(crash_at={("interpreter", BEFORE_TX): 1}))
    h._inbox_off = frozenset()
    h.rules = BusinessRules()
    seen = {}
    monkeypatch.setattr(harness, "make_consumer", lambda *a, **kw: None)
    monkeypatch.setattr(harness.interpreter, "build_consumer", lambda *a, **kw: seen.update(kw))
    monkeypatch.setattr(harness.Harness, "_connect", lambda self: None)
    monkeypatch.setattr(harness.Harness, "llm", property(lambda self: None))
    h._factories()[harness.interpreter.SCHEMA]()
    with pytest.raises(CrashPointReached):
        seen["after_prepare"]()
    assert h.crash_points.units[("interpreter", BEFORE_TX)] == 1


# -- before_tx en el worker de trabajos --------------------------------------------------------
def test_a_job_worker_crashes_before_its_transaction(monkeypatch):
    """Caída de un worker de trabajos antes de la transacción"""
    db = _Db("replanner")
    monkeypatch.setattr(harness, "connect", lambda url: db)
    h = _bare(FaultPlan(crash_at={("replanner", BEFORE_TX): 1}), job_lease_seconds=5.0)
    llm = CountingLlmClient(FakeLlmClient(responder=lambda r: "plan"))

    def prepare(command, payload):
        return llm.complete(LlmRequest(agent="replanner", incident_id="I-1", system="s", prompt="p", schema={})).text

    def handle(conn, command, payload, prepared):
        return Accepted([new_event(event_type="PlanRevised", correlation_id=command.correlation_id,
                                   causation_id=command.command_id, producer="replanner", payload={})])

    work = Work(prepare=prepare, handle=handle)
    specs = {"RevisePlan": work.spec()}
    command = _command(command_type="RevisePlan")
    assert commands.execute_command(db, "replanner", command, specs["RevisePlan"]).status_code == 202
    h.targets = {"replanner": harness.Target("replanner", h._crashing_before_tx("replanner", specs))}
    h._deliveries = []

    h._start_threads()
    try:
        [first] = h._workers
        assert _wait(lambda: not first.is_alive())
        assert db.jobs[0]["status"] == "running" and db.outbox == []  # no escribe nada; el lease sigue
        assert h.component_crashes == 1 and llm.calls["replanner"] == 1
        h._revive_workers()
        [second] = h._workers
        assert second is not first and second.is_alive() and second.name == first.name
        time.sleep(0.2)
        assert db.jobs[0]["status"] == "running" and llm.calls["replanner"] == 1  # el lease aún no venció
        db.now = 6.0  # vence el lease: el worker reiniciado lo retoma
        assert _wait(lambda: db.jobs[0]["status"] == "done")
    finally:
        h._stop_threads()
    assert [e["event_type"] for e in db.outbox] == ["PlanRevised"]
    assert llm.calls["replanner"] == 2 and db.jobs[0]["attempts"] == 2
    [record] = h.crash_points.records
    assert (record.component, record.point, record.unit) == ("replanner", BEFORE_TX, 1)
    assert record.resumed_at is not None
    assert h.crashes == 1


def test_the_harness_lease_defaults_to_the_settings():
    h = _bare(FaultPlan())
    assert h._lease_seconds() == h.settings.job_lease_seconds
    assert _bare(FaultPlan(), job_lease_seconds=2.0)._lease_seconds() == 2.0


def test_worker_crash_does_not_derive_from_exception():
    assert not issubclass(WorkerCrashed, Exception)
    assert issubclass(CrashPointReached, ConsumerCrashed)


def test_a_job_worker_survives_an_ordinary_error():
    """Un error corriente en el trabajo (la base no responde) no mata el worker: se reconecta y sigue"""
    from eda_retail.infra.backoff import Backoff
    from eda_retail.infra.jobs import JobWorker

    calls = []

    def run_once(conn, *a, **kw):
        calls.append(1)
        if len(calls) == 1:
            raise RuntimeError("la base no responde")
        return False

    worker = JobWorker(lambda: _Db(), "planner", {}, lease_seconds=1, poll_interval=0.01,
                       backoff=Backoff(0.01, 0.01), run_once=run_once)
    worker.start()
    try:
        assert _wait(lambda: len(calls) >= 3)
        assert worker.is_alive()
    finally:
        worker.stop(5)


# -- after_commit_before_offset ----------------------------------------------------------------
def test_a_crash_after_the_commit_and_before_the_offset():
    """Caída tras el commit, antes del offset"""
    h = _bare(FaultPlan(crash_at={("orchestrator", AFTER_COMMIT_BEFORE_OFFSET): 5}))
    seen: set[int] = set()
    transitions: list[int] = []

    def saga(msg):  # el inbox descarta el evento reentregado
        if msg.offset() in seen:
            return Outcome.DUPLICATE
        seen.add(msg.offset())
        transitions.append(msg.offset())
        return Outcome.PROCESSED

    broker = _Broker(range(7))

    def make():
        return _Consumer("orchestrator", broker, saga)

    h._deliveries = [(FaultyDelivery(make(), h.config.faults, crashed=h._crashed, crash_points=h.crash_points), make)]
    outcomes = []
    delivery, factory = h._deliveries[0]
    while True:
        try:
            outcome = delivery.run_once(0)
        except Exception as exc:  # noqa: BLE001 — como `step`
            h._on_crash(0, delivery, factory, exc)
            continue
        if outcome is None:
            break
        outcomes.append(outcome)

    assert transitions == list(range(7))  # ninguna transición se repite
    assert outcomes.count(Outcome.DUPLICATE) == 1 and broker.committed == 7
    assert h.consumer_crashes == 1 and h.poison_messages == 0
    [record] = h.crash_points.records
    assert (record.component, record.point, record.unit) == ("orchestrator", AFTER_COMMIT_BEFORE_OFFSET, 5)


def test_ignored_messages_are_not_units_after_the_commit():
    """Un mensaje que el consumidor ignora no abre transacción: no cuenta como unidad"""
    points = CrashPoints(FaultPlan(crash_at={("orchestrator", AFTER_COMMIT_BEFORE_OFFSET): 1}))
    broker = _Broker(range(2))
    outcomes = iter([Outcome.IGNORED, Outcome.PROCESSED])
    delivery = FaultyDelivery(_Consumer("orchestrator", broker, lambda msg: next(outcomes)), FaultPlan(),
                              crash_points=points)
    delivery.run_once(0)
    with pytest.raises(CrashPointReached):
        delivery.run_once(0)
    assert broker.committed == 1


def test_crash_after_still_crashes_with_crash_points():
    """`crash_after` (E2) sigue igual aunque la entrega tenga `crash_at`"""
    broker = _Broker(range(3))
    plan = FaultPlan(crash_after={"interpreter": 2})
    delivery = FaultyDelivery(_Consumer("interpreter", broker, lambda msg: Outcome.PROCESSED), plan,
                              crash_points=CrashPoints(plan))
    delivery.run_once(0)
    with pytest.raises(ConsumerCrashed):
        delivery.run_once(0)
    assert broker.committed == 1 and delivery.crashed == {"interpreter"}


# -- after_commit_before_response --------------------------------------------------------------
def test_a_crash_after_the_commit_and_before_the_response():
    """Caída tras el commit, antes de la respuesta"""
    db = FakeDb("planner")
    plan = FaultPlan(crash_at={("planner", AFTER_COMMIT_BEFORE_RESPONSE): 2})
    points = CrashPoints(plan)
    target = harness.Target("planner", {"CreatePlan": Work().spec()})
    invoke = harness.InProcessCommands(db, {"http://planner": target}, plan, set(), crash_points=points)

    def call(command_id):
        return invoke("http://planner", "CreatePlan", command_id=command_id, correlation_id="I-1",
                      causation_id=None, payload={"incident_id": "I-1"}, timeout=1)

    first, second = uuid.uuid4(), uuid.uuid4()
    assert call(first).status_code == 202
    with pytest.raises(CommandUnavailable):
        call(second)  # confirmó y cae sin responder
    reply = call(second)  # la saga lo reinvoca con el mismo command_id
    assert reply.status_code == 202 and reply.replayed
    assert sum(j["command_id"] == second for j in db.jobs) == 1
    assert invoke.crashes == 1
    [record] = points.records
    assert (record.component, record.point, record.unit) == ("planner", AFTER_COMMIT_BEFORE_RESPONSE, 2)
    assert record.resumed_at is not None


def test_intake_crashes_after_the_commit_and_before_the_response():
    """Caída de intake tras el commit, antes de la respuesta"""
    db = FakeDb("intake")
    registered: list[uuid.UUID] = []

    def handle(conn, command, payload, prepared):
        registered.append(command.command_id)
        return Accepted([])

    h = _bare(FaultPlan(crash_at={("intake", AFTER_COMMIT_BEFORE_RESPONSE): 2}))
    h.targets = {"intake": harness.Target("intake", {"RegisterCarrierNotice": CommandSpec(Payload, handle)})}
    h.commands = harness.InProcessCommands(db, {}, h.config.faults, h._crashed, crash_points=h.crash_points)
    h._seed_conn = db

    for _ in range(2):
        response = h._command("intake", "RegisterCarrierNotice", "I-1", {"incident_id": "I-1"})
        assert response.status_code == 202
    assert response.replayed  # el inbox devuelve la respuesta guardada
    assert len(registered) == 2 and len(set(registered)) == 2  # cada aviso se registra una sola vez
    resent = [d for d in h.commands.deliveries if d.command_id == registered[1]]
    assert len(resent) == 2  # el harness reenvía el aviso con el mismo command_id
    assert h.crashes == 1
    [record] = h.crash_points.records
    assert (record.component, record.point, record.unit) == ("intake", AFTER_COMMIT_BEFORE_RESPONSE, 2)


def test_entry_commands_other_than_notices_never_crash(monkeypatch):
    h = _bare(FaultPlan(crash_at={("inventory", AFTER_COMMIT_BEFORE_RESPONSE): 1}))
    db = FakeDb("inventory")
    h.targets = {"inventory": harness.Target("inventory", {"SeedSampleData": CommandSpec(
        Payload, lambda conn, command, payload, prepared: Accepted([]))})}
    h.commands = harness.InProcessCommands(db, {}, h.config.faults, h._crashed, crash_points=h.crash_points)
    h._seed_conn = db
    assert h._command("inventory", "SeedSampleData", "I-1", {"incident_id": "I-1"}).status_code == 202
    assert h.crash_points.records == []


def test_e2_crash_after_is_unchanged_with_crash_points(monkeypatch):
    """E2: `crash_after` con `executor-<acción>` cae igual; el punto del ejecutor no cuenta ese paso"""
    monkeypatch.setattr(harness, "execute_command",
                        lambda conn, schema, command, spec, *, inbox_enabled=True: commands.CommandResponse(202, {}))
    plan = FaultPlan(crash_after={"executor-notify_customer": 1})
    points = CrashPoints(plan)
    target = harness.Target("executor_notify_customer", {"ExecuteStep": None})
    invoke = harness.InProcessCommands(None, {"http://x": target}, plan, set(), crash_points=points)
    with pytest.raises(CommandUnavailable, match="executor-notify_customer cae tras 1 pasos"):
        invoke("http://x", "ExecuteStep", command_id=uuid.uuid4(), correlation_id="I-1", causation_id=None,
               payload={"action": "notify_customer"}, timeout=1)
    assert invoke.crashes == 1 and points.units[("executor_notify_customer", AFTER_COMMIT_BEFORE_RESPONSE)] == 0


# -- relay_after_publish_before_mark -----------------------------------------------------------
class _Result:
    def __init__(self, rows) -> None:
        self._rows = rows

    def fetchall(self):
        return self._rows


class _OutboxConn:
    """El outbox de un componente en memoria, con rollback de la transacción si algo falla."""

    def __init__(self) -> None:
        self.rows: list[dict] = []

    def add(self, n: int) -> None:
        for _ in range(n):
            e = new_event(event_type="PlanCreated", correlation_id="I-1", causation_id=None, producer="orchestrator",
                          payload={"incident_id": "I-1"})
            self.rows.append({"id": len(self.rows) + 1, "trace_context": None, "published": False, **e.model_dump()})

    @contextlib.contextmanager
    def transaction(self):
        snapshot = copy.deepcopy(self.rows)
        try:
            yield
        except BaseException:
            self.rows = snapshot
            raise

    def execute(self, sql, params=()):
        if sql.lstrip().startswith("SELECT"):
            return _Result([r for r in self.rows if not r["published"]][:params[0]])
        for r in self.rows:
            if r["id"] in params[0]:
                r["published"] = True
        return _Result([])

    def close(self) -> None:
        pass


class _Producer:
    def __init__(self) -> None:
        self.published: list[uuid.UUID] = []

    def produce(self, topic, key, value, headers, on_delivery):
        self.published.append(uuid.UUID(json.loads(value)["event_id"]))
        offset = len(self.published)
        on_delivery(None, SimpleNamespace(partition=lambda: 0, offset=lambda: offset))

    def flush(self, timeout=None):
        return 0


def test_the_relay_crashes_after_publishing(monkeypatch):
    """Caída del relay tras publicar"""
    h = _bare(FaultPlan(crash_at={("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK): 4}))
    conn, producer = _OutboxConn(), _Producer()
    monkeypatch.setattr(harness.ComponentRelay, "from_settings", classmethod(
        lambda cls, s, schema, **kw: cls(conn, producer, schema, "t", **kw)))
    h._relays = [h._relay("orchestrator")]
    first = h._relays[0]

    conn.add(3)
    assert h._publish(h._relays[0]) == 3
    conn.add(3)
    h._publish(h._relays[0])  # el lote con la cuarta fila: publica y cae sin marcar
    assert h._relays[0] is not first and [r["published"] for r in conn.rows] == [True] * 3 + [False] * 3
    assert h._publish(h._relays[0]) == 3  # el relay reiniciado las vuelve a publicar

    ids = [r["event_id"] for r in conn.rows]
    assert producer.published == ids + ids[3:]  # en orden y con el mismo event_id
    assert all(r["published"] for r in conn.rows)  # I4: ninguna fila pendiente
    assert h.crashes == 1
    [record] = h.crash_points.records
    assert (record.component, record.point, record.unit) == ("orchestrator", RELAY_AFTER_PUBLISH_BEFORE_MARK, 4)
    assert record.resumed_at is not None


def test_the_relay_without_hook_marks_its_batch():
    """Sin hook, `run_once` publica y marca el lote como siempre"""
    conn, producer = _OutboxConn(), _Producer()
    conn.add(2)
    relay = ComponentRelay(conn, producer, "orchestrator", "t")  # type: ignore[arg-type]
    assert relay.run_once() == 2 and all(r["published"] for r in conn.rows)


def test_a_relay_error_that_is_not_injected_still_escapes_the_turn():
    """Un error del relay que no es una caída inyectada sigue saliendo del turno, como antes"""

    class Failing:
        _schema = "orchestrator"

        def run_once(self, **kw):
            raise RelayDeliveryError("entrega no confirmada")

    h = _bare(FaultPlan())
    h._relays = [Failing()]
    with pytest.raises(RelayDeliveryError):
        h._publish(h._relays[0])
    assert h.component_crashes == 0


def test_the_harness_counts_llm_calls_in_the_client():
    h = _bare(FaultPlan())
    h._llm = CountingLlmClient(FakeLlmClient(responder=lambda r: "ok"))
    h.llm.complete(LlmRequest(agent="planner", incident_id="I-1", system="s", prompt="p", schema={}))
    assert h.llm_client_calls == {"planner": 1}

