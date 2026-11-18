"""El arnés corre cada unidad larga en su propio hilo, como lo harían procesos separados (odd/tasks/
eda-async-commands.md S6): el worker de trabajos de cada componente y la entrega del intérprete. Sin PostgreSQL ni
Kafka: se sustituyen el worker, el consumidor y las consultas a la base.
"""

from __future__ import annotations

import io
import threading
import time
from types import SimpleNamespace
from typing import ClassVar

import pytest
from pydantic import BaseModel

from eda_retail.config import Settings
from eda_retail.experiments import harness
from eda_retail.infra.commands import CommandSpec
from eda_retail.synthetic.faults import FaultPlan, FaultyDelivery


class _Worker:
    started: ClassVar[list[_Worker]] = []

    def __init__(self, connect, schema, specs, **kw) -> None:
        self.schema, self.specs, self.kw = schema, specs, kw
        self.running = False
        _Worker.started.append(self)

    def start(self) -> None:
        self.running = True

    def stop(self, timeout=None) -> None:
        self.running = False


def _spec(deferred: bool) -> CommandSpec:
    return CommandSpec(BaseModel, lambda *a: None, deferred=deferred)


def _bare(config: harness.RunConfig | None = None) -> harness.Harness:
    h = harness.Harness.__new__(harness.Harness)
    h.config = config or harness.RunConfig(seed=1, incidents=1)
    h.settings = Settings()
    h._init_threads()
    return h


def test_each_component_with_long_commands_gets_its_own_job_worker(monkeypatch):
    """Cada componente con comandos de trabajo largo tiene su worker en su hilo; los demás, ninguno"""
    _Worker.started = []
    monkeypatch.setattr(harness, "JobWorker", _Worker)
    h = _bare()
    h.targets = {"planner": harness.Target("planner", {"CreatePlan": _spec(True)}),
                 "intake": harness.Target("intake", {"RegisterCarrierNotice": _spec(False)}),
                 "executor_file_claim": harness.Target("executor_file_claim", {"ExecuteStep": _spec(True)})}
    h._deliveries = []

    h._start_threads()

    assert sorted(w.schema for w in _Worker.started) == ["executor_file_claim", "planner"]
    assert all(w.running for w in _Worker.started)
    h._stop_threads()
    assert not any(w.running for w in _Worker.started)


class _Msg:
    def __init__(self, key: str) -> None:
        self.key = key

    def partition(self) -> int:
        return 0

    def offset(self) -> int:
        return 0

    def __eq__(self, other) -> bool:
        return getattr(other, "key", other) == self.key


class _Consumer:
    name = "interpreter"

    def __init__(self, messages: list, *, slow: threading.Event | None = None) -> None:
        self.messages = messages
        self.slow = slow
        self.handled: list = []
        self.committed: list = []
        self.closed = False

    def poll(self, timeout):
        if self.messages:
            return self.messages.pop(0)
        time.sleep(timeout)
        return None

    def handle_message(self, msg):
        if self.slow is not None:
            self.slow.wait(5)
        self.handled.append(msg)

    def commit(self, msg):
        self.committed.append(msg)

    def close(self):
        self.closed = True


def _wait(condition, seconds=5.0) -> bool:
    end = time.monotonic() + seconds
    while time.monotonic() < end:
        if condition():
            return True
        time.sleep(0.01)
    return False


def test_the_interpreter_consumes_on_its_own_thread_and_counts_as_busy_while_handling(monkeypatch):
    """La entrega del intérprete corre en su hilo; mientras atiende un mensaje (el LLM) el arnés no está ocioso"""
    monkeypatch.setattr(harness, "JobWorker", _Worker)
    release = threading.Event()
    consumer = _Consumer(["m1"], slow=release)
    h = _bare()
    h.targets = {}
    h._deliveries = [(FaultyDelivery(h._tracked(consumer), FaultPlan()), lambda: h._tracked(consumer))]
    h._threaded = {"interpreter"}

    h._start_threads()
    try:
        assert _wait(lambda: h._activity.busy == 1)
        release.set()
        assert _wait(lambda: consumer.committed == ["m1"])
        assert _wait(lambda: h._activity.busy == 0 and h._activity.done == 1)
    finally:
        h._stop_threads()
    assert h._threads == []


def test_a_crash_of_the_threaded_interpreter_restarts_its_consumer(monkeypatch):
    """E2 con el intérprete en su hilo: cae sin confirmar el offset y se crea otro consumidor"""
    monkeypatch.setattr(harness, "JobWorker", _Worker)
    first, second = _Consumer([_Msg("m1")]), _Consumer([_Msg("m1")])
    consumers = iter([second])
    h = _bare(harness.RunConfig(seed=1, incidents=1, faults=FaultPlan(crash_after={"interpreter": 1})))
    h.targets = {}
    crashed: set[str] = set()
    h._crashed = crashed
    h._failures = {}
    h.consumer_crashes = h.poison_messages = 0
    delivery = FaultyDelivery(h._tracked(first), h.config.faults, crashed=crashed)
    h._deliveries = [(delivery, lambda: h._tracked(next(consumers)))]
    h._threaded = {"interpreter"}

    h._start_threads()
    try:
        assert _wait(lambda: second.committed == ["m1"])
    finally:
        h._stop_threads()
    assert first.committed == [] and first.closed
    assert h.consumer_crashes == 1 and crashed == {"interpreter"}


class _StuckConsumer(_Consumer):
    """Su commit no vuelve hasta `release`, como un commit síncrono de librdkafka tras perder el coordinador (E9)."""

    def __init__(self, release: threading.Event) -> None:
        super().__init__(["m1"])
        self.release = release
        self.in_commit = threading.Event()

    def commit(self, msg):
        self.in_commit.set()
        self.release.wait(10)
        super().commit(msg)


def test_a_consumer_whose_thread_is_still_alive_is_not_closed_from_another_thread(monkeypatch):
    """Al parar, un consumidor cuyo hilo sigue vivo tras la espera no se cierra desde el hilo principal (se colgaba
    E9) y el diagnóstico lo da por vivo"""
    monkeypatch.setattr(harness, "JobWorker", _Worker)
    monkeypatch.setattr(harness, "JOIN_TIMEOUT_S", 0.1)
    release = threading.Event()
    consumer = _StuckConsumer(release)
    h = _bare()
    h.targets = {}
    h._deliveries = [(FaultyDelivery(h._tracked(consumer), FaultPlan()), lambda: h._tracked(consumer))]
    h._threaded = {"interpreter"}

    h._start_threads()
    try:
        assert _wait(consumer.in_commit.is_set)
        h._stop_threads()
        h._close_deliveries()
        assert not consumer.closed
        assert h.consumer_state() == {"interpreter": {"thread_alive": True}}
    finally:
        release.set()


def test_run_does_not_finish_while_jobs_are_pending(monkeypatch):
    """El arnés no da la ejecución por terminada mientras quedan trabajos pendientes o en curso"""
    h = _bare(harness.RunConfig(seed=1, incidents=1, idle_seconds=0.05, timeout_seconds=10))
    open_jobs = iter([1] * 20 + [0] * 1000)
    calls = SimpleNamespace(step=0)

    def step():
        calls.step += 1
        return 0

    monkeypatch.setattr(h, "closed", lambda: 0)
    monkeypatch.setattr(h, "step", step)
    monkeypatch.setattr(h, "_open_jobs", lambda: next(open_jobs))

    h.run_until_closed()

    assert calls.step > 20  # siguió mientras había trabajos, y luego venció el plazo de inactividad
    assert next(open_jobs) == 0


def test_final_sweep_publishes_what_a_thread_left_after_the_last_turn(monkeypatch):
    """Un hilo confirma una fila de outbox justo tras el último turno y deja de trabajar: el repaso final da un turno
    más, que la publica, y se para cuando dos turnos seguidos no avanzan, sin esperar a ningún lag"""
    h = _bare(harness.RunConfig(seed=1, incidents=1, idle_seconds=5, timeout_seconds=10))
    turns = iter([0, 1, 0, 0])  # quieto; el turno extra publica la fila; después, quieto otra vez
    calls = SimpleNamespace(step=0)

    def step():
        calls.step += 1
        return next(turns)

    monkeypatch.setattr(h, "step", step)
    monkeypatch.setattr(h, "_working", lambda: False)
    monkeypatch.setattr(h, "lag", lambda: pytest.fail("el repaso final no consulta el lag"))
    start = time.monotonic()

    h._settle()

    assert calls.step == 4 and time.monotonic() - start < 1


def test_catch_up_log_reads_event_log_up_to_the_high_watermarks():
    """Antes de las invariantes, event_log lee hasta las marcas de agua altas; si no llega, lo dice sin fallar"""
    h = _bare(harness.RunConfig(seed=1, incidents=1, idle_seconds=5, timeout_seconds=10))
    timeouts: list[float] = []
    h._readers = [SimpleNamespace(catch_up=timeouts.append)]
    assert h.catch_up_log(3.0) is True and timeouts == [3.0]

    def stuck(timeout):
        raise TimeoutError("event_log: no llegó a {0: 5} en 3.0 s")

    h._readers = [SimpleNamespace(catch_up=stuck)]
    assert h.catch_up_log(3.0) is False


def _stuck_run(monkeypatch, *, timeout_seconds: float) -> tuple[harness.Harness, io.StringIO, threading.Event]:
    out = io.StringIO()
    monkeypatch.setattr(harness, "STALL_STREAM", out)
    h = _bare(harness.RunConfig(seed=1, incidents=1, idle_seconds=5, timeout_seconds=timeout_seconds))
    release = threading.Event()

    def handling_a_message():  # cuenta como trabajo en curso sin avanzar, como el intérprete atascado de E9 y E17
        with h._activity.working():
            release.wait(10)

    thread = threading.Thread(target=handling_a_message, name="harness-interpreter", daemon=True)
    thread.start()
    h._threads.append(thread)
    monkeypatch.setattr(h, "step", lambda: 0)
    monkeypatch.setattr(h, "_open_jobs", lambda: 2)
    monkeypatch.setattr(h, "_settle", lambda: None)
    return h, out, release


def test_a_run_that_reaches_its_timeout_dumps_what_is_stuck(monkeypatch):
    """Una ejecución que llega a `timeout_seconds` sin cerrar sus incidencias deja en `stall`, y en stderr, la pila de
    cada hilo, lo que cuenta como ocupado y los trabajos abiertos"""
    h, out, release = _stuck_run(monkeypatch, timeout_seconds=0.2)
    monkeypatch.setattr(h, "closed", lambda: 0)
    try:
        assert _wait(lambda: h._activity.busy == 1)
        h.run_until_closed()
    finally:
        release.set()

    assert h.stall["busy"] == 1 and h.stall["open_jobs"] == 2 and h.stall["awaiting_recovery"] is None
    assert any("handling_a_message" in line for line in h.stall["threads"]["harness-interpreter"])
    assert "database" in h.stall  # sin base en este test: el error queda en el diagnóstico
    assert "harness-interpreter" in out.getvalue() and "handling_a_message" in out.getvalue()


def test_a_run_that_closes_its_incidents_dumps_nothing(monkeypatch):
    """Una ejecución que cierra sus incidencias antes del plazo no deja diagnóstico de atasco"""
    h, out, release = _stuck_run(monkeypatch, timeout_seconds=10)
    monkeypatch.setattr(h, "closed", lambda: 1)
    try:
        h.run_until_closed()
    finally:
        release.set()

    assert h.stall is None and out.getvalue() == ""


def test_a_run_that_goes_idle_with_open_incidents_dumps_what_is_stuck(monkeypatch):
    """Una ejecución que se queda quieta (`idle_seconds`) con incidencias abiertas también deja `stall`, con el motivo
    y el estado de cada consumidor: E9 terminaba así, sin llegar a `timeout_seconds`"""
    out = io.StringIO()
    monkeypatch.setattr(harness, "STALL_STREAM", out)
    h = _bare(harness.RunConfig(seed=1, incidents=1, idle_seconds=0.1, timeout_seconds=10))
    monkeypatch.setattr(h, "closed", lambda: 0)
    monkeypatch.setattr(h, "step", lambda: 0)
    monkeypatch.setattr(h, "_open_jobs", lambda: 0)
    monkeypatch.setattr(h, "_settle", lambda: None)
    monkeypatch.setattr(h, "consumer_state", lambda: {"interpreter": {"thread_alive": True, "health": {"polls": 9}}})
    h.run_until_closed()
    assert h.stall["reason"] == "idle"
    assert h.stall["consumers"]["interpreter"]["health"] == {"polls": 9}
    assert "idle" in out.getvalue()


def test_consumer_state_of_a_threaded_consumer_includes_its_own_health(monkeypatch):
    """El diagnóstico de un consumidor con hilo propio lee lo que él mismo anotó, sin llamar a Kafka desde este hilo"""
    monkeypatch.setattr(harness, "JobWorker", _Worker)
    consumer = _Consumer([])
    consumer.health = lambda: {"assigned": [0, 1, 2]}
    consumer.positions = lambda: pytest.fail("no se consulta Kafka desde otro hilo")
    h = _bare()
    h.targets = {}
    h._deliveries = [(FaultyDelivery(h._tracked(consumer), FaultPlan()), lambda: h._tracked(consumer))]
    h._threaded = {"interpreter"}
    assert h.consumer_state() == {"interpreter": {"thread_alive": False, "health": {"assigned": [0, 1, 2]}}}


def test_consumer_state_includes_the_librdkafka_log_of_a_consumer(monkeypatch):
    """Con `consumer_kafka_debug`, el diagnóstico de cada consumidor lleva sus cambios de estado del grupo y sus
    últimas líneas de librdkafka"""
    from eda_retail.infra.kafka import KafkaLogRing

    monkeypatch.setattr(harness, "JobWorker", _Worker)
    consumer = _Consumer([])
    consumer.health = lambda: {"assigned": []}
    consumer.positions = lambda: pytest.fail("no se consulta Kafka desde otro hilo")
    h = _bare()
    h.targets = {}
    h._deliveries = [(FaultyDelivery(h._tracked(consumer), FaultPlan()), lambda: h._tracked(consumer))]
    h._threaded = {"interpreter"}
    ring = KafkaLogRing()
    ring.lines.append("12:00:00.000 CGRPJOINSTATE wait-join")
    ring.group_lines.append("12:00:00.000 CGRPJOINSTATE wait-join")
    h._kafka_logs = {"interpreter": ring}
    state = h.consumer_state()["interpreter"]
    assert state["kafka_log"] == state["kafka_group"] == ["12:00:00.000 CGRPJOINSTATE wait-join"]
    assert state["health"] == {"assigned": []}
