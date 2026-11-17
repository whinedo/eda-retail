"""Modo multiproceso del harness y congelación de un proceso (spec experiments, «Modo multiproceso del harness»; spec
synthetic-data, «Congelación de un proceso»): lo que no necesita Kafka ni PostgreSQL.

Las ejecuciones con instancias reales del orquestador están en tests/integration/test_multiprocess_harness.py y
tests/integration/test_e16_zombie_consumer.py.
"""

from __future__ import annotations

import io
import logging
import subprocess
import sys
import time
import uuid
from pathlib import Path

import pytest
from confluent_kafka import TopicPartition

from eda_retail.config import Settings
from eda_retail.events.envelope import new_event
from eda_retail.experiments.processes import OrchestratorProcess, orchestrator_env, read_log
from eda_retail.infra import kafka
from eda_retail.infra.consumer import EventConsumer
from eda_retail.infra.logs import FORMAT
from eda_retail.orchestrator import saga


class _Kafka:
    def subscribe(self, topics, **callbacks):
        self.topics = topics
        self.callbacks = callbacks


class _Conn:
    """Conexión mínima: transacciones vacías, inserciones de una fila y `fetchone` sin fila."""

    rowcount = 1

    def transaction(self):
        return _Tx()

    def execute(self, *_args, **_kwargs):
        return self

    def fetchone(self):
        return None


class _Tx:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


@pytest.fixture
def process_log():
    """Lo que escribiría el proceso del orquestador, con el formato de `setup_logging`."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(FORMAT.format(process="orchestrator")))
    logger = logging.getLogger("eda_retail")  # otro test (el CLI de experimentos) puede haberlo dejado en WARNING
    old_level = logger.level
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    yield stream
    logger.removeHandler(handler)
    logger.setLevel(old_level)


def test_session_timeout_from_env(monkeypatch):
    monkeypatch.setenv("KAFKA_SESSION_TIMEOUT_MS", "6000")
    assert Settings().kafka_session_timeout_ms == 6000
    monkeypatch.setenv("KAFKA_SESSION_TIMEOUT_MS", "")
    assert Settings().kafka_session_timeout_ms is None


def test_make_consumer_sets_the_session_timeout_only_when_given(monkeypatch):
    monkeypatch.setattr(kafka, "Consumer", lambda config: config)
    assert kafka.make_consumer("k:9092", "orchestrator", session_timeout_ms=6000)["session.timeout.ms"] == 6000
    plain = kafka.make_consumer("k:9092", "orchestrator")
    assert "session.timeout.ms" not in plain
    assert plain == {"bootstrap.servers": "k:9092", "group.id": "orchestrator", "enable.auto.commit": False,
                     "auto.offset.reset": "earliest"}


def test_process_log_records_rebalances_messages_and_lock_waits(process_log, monkeypatch):
    """El log de una instancia da los rebalanceos, el inicio y el fin de cada mensaje y las esperas al bloqueo"""
    fake = _Kafka()
    consumer = EventConsumer("orchestrator", _Conn(), fake, "eda.t", {"PlanCreated": lambda c, e: None},  # type: ignore[arg-type]
                             schema="orchestrator")
    fake.callbacks["on_assign"](None, [TopicPartition("eda.t", 2), TopicPartition("eda.t", 0)])
    event = new_event(event_type="PlanCreated", correlation_id="I-1", causation_id=None, producer="planner",
                      payload={})

    class Msg:
        def value(self):
            return b"{}"

        def partition(self):
            return 2

        def offset(self):
            return 7

        def headers(self):
            return []

        def topic(self):
            return "eda.t"

    monkeypatch.setattr("eda_retail.infra.consumer.decode_checked", lambda _msg: event)
    outcome = consumer.handle_message(Msg())  # type: ignore[arg-type]
    saga.lock(_Conn(), None, None, "I-1", uuid.uuid4())  # type: ignore[arg-type]
    fake.callbacks["on_lost"](None, [TopicPartition("eda.t", 0), TopicPartition("eda.t", 2)])

    log = read_log(process_log.getvalue().splitlines())
    assert [(kind, parts) for _, kind, parts in log.rebalances] == [("assigned", [0, 2]), ("lost", [0, 2])]
    assert log.assignment() == []
    assert log.assignment(before=log.rebalances[1][0] - 0.001) in ([0, 2], [])  # misma marca de tiempo posible
    [msg] = log.messages
    assert (msg.event_type, msg.incident, msg.partition, msg.offset, msg.outcome) == ("PlanCreated", "I-1", 2, 7,
                                                                                    outcome.value)
    assert msg.end is not None and msg.end >= msg.start
    [(_, incident, wait, thread)] = log.lock_waits
    assert incident == "I-1" and wait >= 0 and thread == "MainThread"


def test_read_log_keeps_an_unfinished_message():
    lines = [
        "2026-11-17 10:00:00,000 INFO [orchestrator] orchestrator: particiones asignadas [0, 1, 2]",
        "2026-11-17 10:00:01,000 DEBUG [orchestrator] -> StepCompleted I-2 p1 o4",
        "2026-11-17 10:00:02,000 DEBUG [orchestrator] -> StepCompleted I-3 p1 o5",
        "2026-11-17 10:00:02,500 INFO [orchestrator] <- StepCompleted I-3: duplicate",
        "Traceback (most recent call last):",
        "2026-11-17 10:00:03,000 DEBUG [orchestrator] -> PlanRevised I-4 p0 o9",
        "2026-11-17 10:00:04,000 INFO [orchestrator] orchestrator: particiones revocadas [0, 1, 2]",
        "2026-11-17 10:00:05,000 INFO [orchestrator] orchestrator: particiones asignadas [1]",
    ]
    log = read_log(lines)
    assert [(m.incident, m.outcome) for m in log.messages] == [("I-2", None), ("I-3", "duplicate"), ("I-4", None)]
    assert log.assignment() == [1]
    assert log.assignment(before=log.rebalances[1][0]) == []
    assert log.assignment(before=log.rebalances[0][0]) == [0, 1, 2]


def test_orchestrator_env_makes_the_run_explicit():
    settings = Settings(database_url="postgresql://x/exp_1", events_topic="eda.exp_1", dlq_topic="eda.exp_1.dlq",
                        kafka_bootstrap_servers="k:9092", inbox_enabled=True, saga_step_timeout_s=6.0)
    env = orchestrator_env(settings, {"inventory": "http://127.0.0.1:5/inventory"}, session_timeout_ms=6000,
                           base={"PATH": "/bin", "DATABASE_URL": "postgresql://otra", "EVENTS_TOPIC": "eda.events"})
    assert env["PATH"] == "/bin"
    assert (env["DATABASE_URL"], env["EVENTS_TOPIC"], env["DLQ_TOPIC"]) == ("postgresql://x/exp_1", "eda.exp_1",
                                                                            "eda.exp_1.dlq")
    assert env["INVENTORY_URL"] == "http://127.0.0.1:5/inventory"
    assert env["KAFKA_SESSION_TIMEOUT_MS"] == "6000" and env["SAGA_STEP_TIMEOUT_S"] == "6.0"
    assert env["TRACING_ENABLED"] == "false" and env["LOG_LEVEL"] == "DEBUG"
    assert orchestrator_env(settings, {}, session_timeout_ms=None, base={})["KAFKA_SESSION_TIMEOUT_MS"] == ""


def _state(pid: int) -> str:
    return subprocess.run(["ps", "-o", "state=", "-p", str(pid)], check=False, capture_output=True,
                          text=True).stdout.strip()


def test_freeze_and_resume_a_process(tmp_path: Path):
    """Congelación de un proceso: SIGSTOP lo detiene, SIGCONT lo reanuda y al parar no queda vivo"""
    popen = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"])
    process = OrchestratorProcess("orchestrator-a", popen, tmp_path / "a.log")
    try:
        frozen = process.freeze()
        time.sleep(0.2)
        assert _state(popen.pid).startswith("T")
        resumed = process.resume()
        time.sleep(0.2)
        assert not _state(popen.pid).startswith("T")
        assert process.frozen_at == [frozen] and process.resumed_at == [resumed] and resumed >= frozen
        process.freeze()  # parado mientras está congelado: se reanuda y termina
    finally:
        code = process.stop()
    assert not process.alive() and code is not None


def test_command_gateway_routes_by_target_prefix():
    """El servidor de comandos del harness atiende `<base>/<destino>/commands/<Comando>`: un comando que el destino
    no conoce es un 404 de ese destino, y un destino que no existe no tiene ruta"""
    import urllib.error
    import urllib.request

    from eda_retail.experiments.processes import CommandGateway

    gateway = CommandGateway({"inventory": ("inventory", {}, True)}, connect=lambda: None,  # type: ignore[arg-type]
                             track=lambda call: call()).start()
    try:
        assert gateway.urls() == {"inventory": f"{gateway.base_url}/inventory"}
        for url, body in ((f"{gateway.urls()['inventory']}/commands/Nope", b"unknown_command"),
                          (f"{gateway.base_url}/planner/commands/CreatePlan", b"Not Found")):
            request = urllib.request.Request(url, data=b"{}", headers={"Content-Type": "application/json"},
                                             method="POST")
            with pytest.raises(urllib.error.HTTPError) as error:
                urllib.request.urlopen(request, timeout=5)
            assert error.value.code == 404 and body in error.value.read()
    finally:
        gateway.stop()
