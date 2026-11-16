"""El arnés corre cada unidad larga en su propio hilo, como lo harían procesos separados (odd/tasks/
eda-async-commands.md S6): el worker de trabajos de cada componente y la entrega del intérprete. Sin PostgreSQL ni
Kafka: se sustituyen el worker, el consumidor y las consultas a la base.
"""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
from typing import ClassVar

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
