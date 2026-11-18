"""E10 Caída de PostgreSQL (spec experiments): avisos reintentados durante el corte, líneas de reconexión de cada
proceso, mensajes vueltos a atender, conexión y lag del harness con los componentes en procesos, métricas y H10.

Sin Docker, PostgreSQL ni Kafka. La ejecución de punta a punta está en tests/integration/test_e10_postgres_down.py.
"""

from __future__ import annotations

from collections import Counter
from types import SimpleNamespace

import psycopg
import pytest

from eda_retail.config import Settings
from eda_retail.experiments import cli, harness
from eda_retail.experiments import e10_postgres_down as e10
from eda_retail.expexecutor.catalog import CATALOG
from eda_retail.infra.command_client import CommandReply, CommandUnavailable


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def _line(process: str, level: str, message: str) -> str:
    return f"2026-11-18 10:00:00,123 {level} [{process}] {message}"


# --- líneas de reconexión y mensajes vueltos a atender ------------------------------------------------------------


def test_connection_errors_count_the_reconnection_lines_of_every_process_type():
    """Cuenta cada línea propia del proceso que dice que la base no respondió, y no las de la traza ni otras"""
    text = "\n".join([
        _line("orchestrator", "WARNING", "orchestrator: la base no responde; se reintenta el mensaje p1 o7"),
        "Traceback (most recent call last):",
        "psycopg.OperationalError: la base no responde",  # línea de la traza: no cuenta
        _line("orchestrator", "WARNING", "orchestrator: sin conexión a la base; se reintenta"),
        _line("orchestrator", "ERROR", "relay de orchestrator: error; se reintenta en 2.0 s"),
        _line("orchestrator", "WARNING", "relay de orchestrator: sin conexión a la base; se reintenta"),
        _line("orchestrator", "ERROR", "dispatcher: error; se reintenta"),
        _line("planner", "ERROR", "worker de trabajos de planner: error"),
        _line("event-log", "WARNING", "event_log: la base no responde; se reintenta"),
        _line("orchestrator", "INFO", "<- PlanCreated inc-1: processed"),
        _line("orchestrator", "WARNING", "relay de orchestrator: lote sin confirmar; se reintenta"),  # Kafka
    ])
    assert e10.connection_errors(text) == 7
    assert e10.connection_errors("") == 0


def test_message_retries_separate_in_process_retries_from_kafka_redeliveries_and_inbox_discards():
    """Un mensaje que se vuelve a atender tras un «se reintenta el mensaje» es un reintento del proceso; si no hay
    esa línea, Kafka lo volvió a entregar porque su offset no se confirmó. Los descartes del inbox son `duplicate`"""
    text = "\n".join([
        _line("orchestrator", "DEBUG", "-> PlanCreated inc-1 p1 o7"),
        _line("orchestrator", "WARNING", "orchestrator: la base no responde; se reintenta el mensaje p1 o7"),
        _line("orchestrator", "DEBUG", "-> PlanCreated inc-1 p1 o7"),
        _line("orchestrator", "INFO", "<- PlanCreated inc-1: processed"),
        _line("orchestrator", "DEBUG", "-> StepCompleted inc-2 p0 o3"),
        _line("orchestrator", "INFO", "<- StepCompleted inc-2: processed"),
        _line("orchestrator", "DEBUG", "-> StepCompleted inc-2 p0 o3"),  # sin reintento: entrega repetida de Kafka
        _line("orchestrator", "INFO", "<- StepCompleted inc-2: duplicate"),
        _line("orchestrator", "DEBUG", "-> StepCompleted inc-3 p2 o1"),
        _line("orchestrator", "INFO", "<- StepCompleted inc-3: processed"),
    ])
    assert e10.message_retries(text) == {"retried_in_process": 1, "kafka_redeliveries": 1, "inbox_discards": 1}


def test_command_retries_come_from_the_dispatcher_lines():
    text = "\n".join([
        _line("orchestrator", "WARNING", "comando ExecuteStep 1234 sin respuesta (intento 1): HTTP 500"),
        _line("orchestrator", "WARNING", "comando ExecuteStep 1234 sin respuesta (intento 2): no responde"),
        _line("orchestrator", "WARNING", "comando ExecuteStep 1234 rechazado: invalid"),
    ])
    assert e10.command_retries(text) == 2


def test_process_report_gives_every_count_per_process():
    procs = [SimpleNamespace(name="intake", log_text=lambda: _line("intake", "ERROR", "relay de intake: error; se "
                                                                                   "reintenta en 2.0 s")),
             SimpleNamespace(name="projector", log_text=lambda: "")]
    report = e10.process_report(procs)
    assert report["intake"] == {"connection_errors": 1, "retried_in_process": 0, "kafka_redeliveries": 0,
                                "inbox_discards": 0, "command_retries": 0}
    assert report["projector"]["connection_errors"] == 0


# --- avisos reintentados ---------------------------------------------------------------------------------------


def _sender(replies, cutting):
    clock = Clock()
    answers = iter(replies)

    def send():
        reply = next(answers)
        if isinstance(reply, Exception):
            raise reply
        return reply

    out = e10.send_until_accepted(send, cutting=cutting, timeout_s=10, retry_s=1.0, clock=clock, sleep=clock.sleep)
    return out, clock


def test_notice_rejected_during_the_cut_is_retried_until_accepted():
    """Avisos durante la caída de la base: un aviso que falla con 5xx o sin respuesta durante el corte se reintenta hasta que se
    acepta"""
    cut = iter([True, True, False, False])
    out, clock = _sender([CommandUnavailable("HTTP 500"), CommandUnavailable("no responde"),
                          CommandUnavailable("HTTP 500"), CommandReply(202, {"status": "accepted"})],
                         lambda: next(cut))
    assert (out.attempts, out.rejected_during_outage, out.rejected_after_outage, out.accepted) == (4, 2, 1, True)
    assert clock.now == 3.0


def test_a_notice_registered_by_an_attempt_whose_answer_was_lost_counts_as_accepted():
    out, _ = _sender([CommandUnavailable("no responde"), CommandReply(422, {"reason": "duplicate_notice"})],
                     lambda: False)
    assert out.accepted and out.attempts == 2


def test_a_business_rejection_is_not_retried():
    out, _ = _sender([CommandReply(422, {"reason": "invalid_command"})], lambda: False)
    assert (out.accepted, out.refused, out.attempts) == (False, True, 1)


def test_the_sender_gives_up_after_its_deadline():
    out, clock = _sender([CommandUnavailable("HTTP 500")] * 100, lambda: True)
    assert not out.accepted and out.rejected_during_outage == 11 and clock.now == 10.0


def test_notice_summary():
    sends = [e10.NoticeSend(attempts=3, rejected_during_outage=2, accepted=True),
             e10.NoticeSend(attempts=1, accepted=True), e10.NoticeSend(attempts=2, rejected_after_outage=1)]
    assert e10.notice_summary(sends) == {"sent": 3, "accepted": 2, "rejected_during_outage": 2,
                                         "rejected_after_outage": 1, "retries": 3, "refused": 0}


def test_send_notices_reuses_one_command_id_per_notice(monkeypatch):
    """Cada aviso se reintenta con el mismo `command_id`: si un intento llegó a registrarse, el inbox lo repite"""
    seen: dict[int, set] = {}
    attempts = Counter()

    def send_notice(notice, *, check, command_id):
        attempts[notice] += 1
        seen.setdefault(notice, set()).add(command_id)
        if attempts[notice] == 1:
            raise CommandUnavailable("HTTP 500")
        return CommandReply(202, {})

    h = SimpleNamespace(data=SimpleNamespace(notices=["n0", "n1", "n2"]), _send_notice=send_notice)
    sends = e10.send_notices(h, [1, 2], cutting=lambda: True, timeout_s=5, retry_s=0.0)
    assert [s.accepted for s in sends] == [True, True] and set(seen) == {"n1", "n2"}
    assert all(len(ids) == 1 for ids in seen.values())


# --- el harness durante y después del corte -----------------------------------------------------------------------


class _Conn:
    def __init__(self, fail: bool = False) -> None:
        self.fail, self.closed, self.broken = fail, False, fail
        self.executed = 0

    def execute(self, sql, params=None):
        self.executed += 1
        if self.fail:
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        return SimpleNamespace(fetchone=lambda: {"n": 1})

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        self.closed = True


def _bare(config: harness.RunConfig) -> harness.Harness:
    h = harness.Harness(Settings(), config)
    h.settings = Settings(database_url="postgresql://x/exp_1", events_topic="eda.exp_1", dlq_topic="eda.exp_1.dlq")
    return h


def test_harness_reconnects_after_the_database_comes_back(monkeypatch):
    """La conexión del harness, rota por el corte, se cambia por otra en cuanto la base vuelve a responder"""
    fresh = [psycopg.OperationalError("connection refused"), _Conn()]

    def connect(url):
        out = fresh.pop(0)
        if isinstance(out, Exception):
            raise out
        return out

    monkeypatch.setattr(harness, "connect", connect)
    monkeypatch.setattr(harness.time, "sleep", lambda s: None)
    h = _bare(harness.RunConfig(seed=1, incidents=1, component_processes=True))
    broken = h.conn = _Conn(fail=True)
    assert h.reconnect(timeout=5) is True
    assert broken.closed and h.conn is not broken and h.conn.executed == 1


def test_harness_keeps_a_healthy_connection():
    h = _bare(harness.RunConfig(seed=1, incidents=1, component_processes=True))
    conn = h.conn = _Conn()
    assert h.reconnect(timeout=5) is False and h.conn is conn


def test_harness_reconnect_gives_up_after_its_deadline(monkeypatch):
    def refuse(url):
        raise psycopg.OperationalError("connection refused")

    clock = Clock()
    monkeypatch.setattr(harness, "connect", refuse)
    monkeypatch.setattr(harness.time, "sleep", clock.sleep)
    monkeypatch.setattr(harness.time, "monotonic", clock)
    h = _bare(harness.RunConfig(seed=1, incidents=1, component_processes=True))
    h.conn = _Conn(fail=True)
    with pytest.raises(psycopg.OperationalError):
        h.reconnect(timeout=3)


def test_catch_up_after_stopping_the_processes_does_not_wait():
    """Con los procesos parados, `catch_up_log` comprueba una sola vez: el proceso `event-log` ya no avanza"""
    h = _bare(harness.RunConfig(seed=1, incidents=1, component_processes=True))
    waits = []
    h._catch_up_processes = lambda timeout: waits.append(timeout) or True
    h.catch_up_log(timeout=30)
    h.exit_codes = {"event-log": 0}
    h.catch_up_log(timeout=30)
    assert waits == [30, 0.0]


def test_positioned_lags_from_the_stored_offsets():
    """El lag de `event_log` y de cada proyector sale de su posición guardada frente a las marcas de agua altas"""
    rows = [{"consumer": "event_log", "kafka_partition": 0, "kafka_offset": 9},
            {"consumer": "event_log", "kafka_partition": 1, "kafka_offset": 3},
            {"consumer": "view_orders", "kafka_partition": 0, "kafka_offset": 4}]
    highs = {0: 10, 1: 5, 2: 0}
    assert harness.positioned_lags(rows, highs, ["event_log", "view_orders", "view_incident_status"]) == {
        "event_log": 1, "view_orders": 5 + 5, "view_incident_status": 10 + 5}


def test_lags_in_component_mode_use_the_groups_and_the_stored_positions(monkeypatch):
    h = _bare(harness.RunConfig(seed=1, incidents=1, component_processes=True))
    h._admin = object()
    monkeypatch.setattr(h, "_process_highs", lambda: {0: 2})
    monkeypatch.setattr(harness, "group_lag", lambda admin, group, topic, highs: {"orchestrator": 1}.get(group, 0))
    h.conn = SimpleNamespace(execute=lambda sql, params=None: SimpleNamespace(fetchall=list),
                             commit=lambda: None)
    lags = h.lags()
    assert lags["orchestrator"] == 1 and lags["interpreter"] == 0 and lags["event_log"] == 2


# --- muestreo tras el corte ----------------------------------------------------------------------------------------


def test_drain_sampler_tolerates_database_errors_and_finds_the_drain_time(monkeypatch):
    """Una lectura que falla no tumba la ejecución: se cuenta, el harness se reconecta y se sigue muestreando"""
    samples = iter([psycopg.OperationalError("caída"), ({"intake": 2}, {"orchestrator": 0}),
                    ({"intake": 0}, {"orchestrator": 3}), ({"intake": 0}, {"orchestrator": 0})])

    def sample(h):
        out = next(samples)
        if isinstance(out, Exception):
            raise out
        return out

    reconnects = []
    monkeypatch.setattr(e10, "sample_backlog", sample)
    clock = Clock()
    sampler = e10.DrainSampler(SimpleNamespace(reconnect=lambda: reconnects.append(1)), since=0.0, clock=clock)
    for _ in range(4):
        clock.now += e10.SAMPLE_EVERY_S
        sampler()
    assert sampler.errors == 1 and reconnects == [1]
    assert sampler.drain_seconds() == 4 * e10.SAMPLE_EVERY_S
    clock.now += 0.01
    sampler()  # ya vació: no vuelve a muestrear
    assert sampler.drain_seconds() == 4 * e10.SAMPLE_EVERY_S


# --- H10 -----------------------------------------------------------------------------------------------------------

PROCESSES = ("projector", "event-log", "intake", "orchestrator")


def _metrics(**over):
    base = {"open": 0, "extra_notices": 20, "process_deaths": {}, "exit_codes": {p: 0 for p in PROCESSES},
            "pending_at_end": {"intake": 0}, "missing_in_log": 0,
            "executions": {"keys": 10, "max_per_key": 1, "repeated": 0},
            "notices": {"accepted": 20, "rejected_during_outage": 3},
            "connection_errors": {p: 2 for p in PROCESSES}, "db_probe_failures_during_outage": 4,
            "invariants": {f"I{i}": {"violations": 0} for i in range(1, 6)}}
    return {**base, **over}


def test_provoked_per_variant():
    """Cada variante con corte provoca su efecto: avisos rechazados y, parada, un error de conexión en cada proceso;
    congelada, la base no acepta conexiones nuevas (las abiertas esperan sin error)"""
    assert e10.provoked("baseline", _metrics(notices={"accepted": 20, "rejected_during_outage": 0},
                                             connection_errors={}, db_probe_failures_during_outage=0))
    assert e10.provoked("stopped", _metrics(db_probe_failures_during_outage=0))
    assert not e10.provoked("stopped", _metrics(connection_errors={**{p: 2 for p in PROCESSES}, "projector": 0}))
    assert not e10.provoked("stopped", _metrics(connection_errors={}))
    assert e10.provoked("paused", _metrics(connection_errors={p: 0 for p in PROCESSES}))
    assert not e10.provoked("paused", _metrics(db_probe_failures_during_outage=0))
    for variant in ("stopped", "paused"):
        assert not e10.provoked(variant, _metrics(notices={"accepted": 20, "rejected_during_outage": 0}))


@pytest.mark.parametrize("over", [
    {"open": 1}, {"process_deaths": {"intake": 1}}, {"exit_codes": {"intake": -9}}, {"exit_codes": {}},
    {"pending_at_end": {"intake": 1}}, {"missing_in_log": 1},
    {"executions": {"keys": 10, "max_per_key": 2, "repeated": 1}},
    {"notices": {"accepted": 19, "rejected_during_outage": 3}},
    {"connection_errors": {}}, {"invariants": {"I4": {"violations": 1}}},
])
def test_h10_fails_on_each_broken_condition(over):
    """Cumplimiento de H10: falla si un proceso muere o queda colgado, queda outbox pendiente o un evento sin
    `event_log`, un efecto se repite, una incidencia queda abierta (también las de los avisos del corte), un
    invariante tiene violaciones o la variante no provoca su efecto"""
    assert e10.h10_variant("stopped", _metrics())
    assert not e10.h10_variant("stopped", _metrics(**over))


def test_executions_summary():
    assert e10.executions_summary(Counter({("mail", "a"): 1, ("payments", "b"): 3})) == {
        "keys": 2, "max_per_key": 3, "repeated": 2}
    assert e10.executions_summary(Counter()) == {"keys": 0, "max_per_key": 0, "repeated": 0}


def test_latency_ratios_against_baseline():
    metrics = {"latency_ms": {"mean": 200.0, "median": 150.0, "p95": 300.0}}
    baseline = {"latency_ms": {"mean": 100.0, "median": None, "p95": 100.0}}
    assert e10.latency_ratios(metrics, baseline) == {"mean": 2.0, "median": None, "p95": 3.0}


def test_run_e10_runs_every_variant_in_component_mode(monkeypatch):
    """H10 se cumple solo si se cumple en las tres variantes; el informe da el cociente de latencia frente a baseline"""
    def variant(base, name, **kw):
        return {**_metrics(), "latency_ms": {"mean": 100.0, "median": 100.0, "p95": 100.0}, "h10": name != "paused"}

    monkeypatch.setattr(e10, "run_variant", variant)
    report = e10.run_e10(Settings(), incidents=4, extra=2, outage_s=5)
    assert set(report.metrics["variants"]) == {"baseline", "stopped", "paused"}
    assert report.config["mode"] == "component_processes" and report.hypothesis == {"H10": False}
    assert report.metrics["variants"]["stopped"]["latency_vs_baseline"]["p95"] == 1.0


def test_e10_is_in_the_cli_and_the_catalog():
    assert cli.EXPERIMENTS["e10"] is e10.run_e10 and cli.DEFAULT_INCIDENTS["e10"] == 50
    report = {"hypothesis": {"H10": True}, "metrics": {"variants": {
        v: {"h10": True, "open": 0, "missing_in_log": 0, "process_deaths": {},
            "invariants": {"I1": {"violations": 0}, "I2": {"violations": 0}}} for v in e10.VARIANTS}}}
    checks = CATALOG["e10"].checks(report)
    assert checks and all(c.ok for c in checks)
