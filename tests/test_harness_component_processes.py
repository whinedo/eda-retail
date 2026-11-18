"""El harness con todos los componentes en procesos propios (`RunConfig.component_processes`; spec experiments,
«Todos los componentes en procesos propios»), sin PostgreSQL ni Kafka: se sustituyen el lanzador, la conexión y el
cliente de comandos."""

from __future__ import annotations

import json
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from eda_retail.config import Settings
from eda_retail.experiments import harness
from eda_retail.experiments.processes import COMPONENTS, ComponentProcess
from eda_retail.infra.command_client import CommandReply
from eda_retail.synthetic.faults import FaultPlan


def _bare(config: harness.RunConfig) -> harness.Harness:
    h = harness.Harness(Settings(), config)
    h.settings = Settings(database_url="postgresql://x/exp_1", events_topic="eda.exp_1", dlq_topic="eda.exp_1.dlq",
                          pending_notice_timeout_s=harness.PENDING_NOTICE_TIMEOUT_S)
    return h


def test_the_switch_is_off_by_default():
    """Sin el interruptor, el harness monta los componentes en proceso como siempre"""
    config = harness.RunConfig(seed=1, incidents=1)
    assert config.component_processes is False
    assert harness.Harness(Settings(), config).components == []


@pytest.mark.parametrize(("change", "kwargs", "message"), [
    ({"orchestrator_processes": 2}, {}, "no se combina con orchestrator_processes"),
    ({"idempotency": "none"}, {}, "solo admite idempotency='full'"),
    ({"faults": FaultPlan(duplicates=2)}, {}, "no admite los fallos del FaultPlan"),
    ({}, {"poc_version": 2}, "solo admite POC_VERSION=1"),
    ({}, {"custom": True}, "no admite llm, data ni carrier propios"),
])
def test_component_mode_rejects_what_only_the_in_process_harness_can_do(change, kwargs, message):
    """Lo que solo inyecta el harness en proceso (fallos del FaultPlan, otro modo de idempotencia, otro LLM) no se
    acepta en silencio en el modo con todos los componentes en procesos"""
    config = harness.RunConfig(seed=1, incidents=1, component_processes=True, **change)
    with pytest.raises(ValueError, match=message):
        harness.check_component_mode(config, kwargs.get("poc_version", 1), custom=kwargs.get("custom", False))
    harness.check_component_mode(harness.RunConfig(seed=1, incidents=1, component_processes=True), 1, custom=False)


def test_start_components_wires_every_process_to_the_run(monkeypatch):
    """Cada proceso recibe la base de datos y el topic de la ejecución, su puerto, el oráculo de la ejecución como
    LLM y el plazo de los avisos aparcados del harness; el harness espera a que estén listos"""
    seen = {}

    def start(components, env, urls, log_dir, *, processes):
        seen.update(components=list(components), env=env, urls=urls, log_dir=log_dir)
        processes.append("p")
        return processes

    monkeypatch.setattr(harness, "start_components", start)
    monkeypatch.setattr(harness, "wait_ready", lambda procs: seen.setdefault("ready", list(procs)))
    h = _bare(harness.RunConfig(seed=7, incidents=4, component_processes=True))
    try:
        h._start_components()
        env = seen["env"]
        assert seen["components"] == list(COMPONENTS) and seen["ready"] == ["p"]
        assert (env["DATABASE_URL"], env["EVENTS_TOPIC"]) == ("postgresql://x/exp_1", "eda.exp_1")
        assert (env["LLM_CLIENT"], env["ORACLE_SEED"], env["ORACLE_INCIDENTS"]) == ("oracle", "7", "4")
        assert env["PENDING_NOTICE_TIMEOUT_S"] == str(harness.PENDING_NOTICE_TIMEOUT_S)
        assert env["EFFECT_KEYS_ENABLED"] == "true" and env["INBOX_ENABLED"] == "true"
        assert set(seen["urls"]) == {c.target for c in COMPONENTS if c.target} == set(h.component_urls)
        assert len(set(seen["urls"].values())) == len(seen["urls"])
        assert env["INTAKE_URL"] == h.component_urls["intake"]
    finally:
        h._process_dir.cleanup()


def test_entry_commands_go_to_the_process_by_rest(monkeypatch):
    """Los comandos de entrada del harness (datos de ejemplo y avisos) van por REST al proceso de su destino"""
    calls = []

    def invoke(url, command_type, **kw):
        calls.append((url, command_type, kw))
        return CommandReply(202, {"status": "accepted"})

    monkeypatch.setattr(harness, "invoke", invoke)
    h = _bare(harness.RunConfig(seed=7, incidents=1, component_processes=True))
    h.component_urls = {"intake": "http://127.0.0.1:5"}
    n = h.data.notices[0]
    assert h._send_notice(n).status_code == 202
    url, command_type, kw = calls[0]
    assert (url, command_type) == ("http://127.0.0.1:5", "RegisterCarrierNotice")
    assert kw["payload"]["carrier_notice_id"] == n.carrier_notice_id and isinstance(kw["command_id"], uuid.UUID)
    monkeypatch.setattr(harness, "invoke", lambda *a, **kw: CommandReply(422, {"reason": "invalid"}))
    with pytest.raises(RuntimeError, match="RegisterCarrierNotice rechazado"):
        h._send_notice(n)


class _Conn:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, sql, params=None):
        return SimpleNamespace(fetchone=lambda: self.rows.pop(0), fetchall=list)

    def commit(self):
        pass

    def transaction(self):
        return self

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def test_a_turn_counts_the_new_rows_of_the_event_log(monkeypatch):
    """En este modo un turno no ejecuta nada: mide lo que avanzaron los procesos (filas nuevas de `event_log`)"""
    monkeypatch.setattr(harness.time, "sleep", lambda s: None)
    h = _bare(harness.RunConfig(seed=1, incidents=1, component_processes=True))
    h.conn = _Conn([{"n": 5}, {"n": 5}, {"n": 8}])
    assert [h.step(), h.step(), h.step()] == [5, 0, 3]


def test_duplicates_come_from_the_executions_of_the_executor_processes(tmp_path: Path):
    """Las ejecuciones de los servicios simulados se leen de los ficheros de los ejecutores: una clave ejecutada dos
    veces es un efecto repetido"""
    h = _bare(harness.RunConfig(seed=1, incidents=1, component_processes=True))
    path = tmp_path / "executor-issue_refund.executions.jsonl"
    path.write_text("".join(json.dumps({"service": "payments", "idempotency_key": k}) + "\n" for k in ("a", "a", "b")))
    h.components = [ComponentProcess("executor-issue_refund", SimpleNamespace(poll=lambda: None), tmp_path / "x.log",
                                     executions_path=path)]
    h.conn = _Conn([{"extra": 0}])
    assert h.service_executions() == {("payments", "a"): 2, ("payments", "b"): 1}
    assert h.duplicates() == {"issue_refund": 1}
