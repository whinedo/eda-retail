"""Lanzador genérico de los componentes en procesos propios (spec experiments, «Todos los componentes en procesos
propios»): lo que no necesita Kafka ni PostgreSQL. La ejecución de punta a punta está en
tests/integration/test_multiprocess_components.py."""

from __future__ import annotations

import re
import signal
import subprocess
import sys
import time
import tomllib
from pathlib import Path

import pytest

from eda_retail.config import Settings
from eda_retail.experiments import processes
from eda_retail.experiments.processes import (
    COMPONENTS,
    Component,
    ComponentProcess,
    assigned_partitions,
    component_env,
    component_launcher,
    free_ports,
    start_components,
    stop_all,
    wait_ready,
)

ROOT = Path(__file__).resolve().parents[1]


def _run_agents_processes() -> list[tuple[str, str, tuple[str, ...]]]:
    """(nombre, módulo, argumentos) de cada proceso de `scripts/run-agents.sh` (versión 1)."""
    script = (ROOT / "scripts" / "run-agents.sh").read_text()
    block = script.split("PROCESSES=(", 1)[1].split(")", 1)[0]
    scripts = tomllib.loads((ROOT / "pyproject.toml").read_text())["project"]["scripts"]
    out = []
    for name, command in re.findall(r'"([\w-]+)\|([^"]+)"', block):
        program, *args = command.split()
        out.append((name, scripts[program].split(":")[0], tuple(args)))
    return out


def test_components_are_the_processes_of_run_agents():
    """Los mismos procesos que scripts/run-agents.sh: el modo lanza exactamente sus procesos, con el mismo nombre,
    el módulo de su comando `eda-*` y sus argumentos"""
    assert [(c.name, c.module, c.args) for c in COMPONENTS] == _run_agents_processes()
    assert {c.target for c in COMPONENTS if c.target} == set(processes.TARGET_URL_ENV)


def test_free_ports_are_distinct():
    ports = free_ports(12)
    assert len(set(ports)) == 12 and all(p > 0 for p in ports)


def test_component_env_gives_every_url_and_the_extras():
    """Cada proceso recibe la base de datos y el topic de la ejecución, la URL de cada destino y los extras"""
    settings = Settings(database_url="postgresql://x/exp_1", events_topic="eda.exp_1", dlq_topic="eda.exp_1.dlq")
    urls = {"intake": "http://127.0.0.1:5", "executor_issue_refund": "http://127.0.0.1:6"}
    env = component_env(settings, urls, extra={"LLM_CLIENT": "oracle"}, base={"DATABASE_URL": "postgresql://otra"})
    assert (env["DATABASE_URL"], env["EVENTS_TOPIC"], env["DLQ_TOPIC"]) == ("postgresql://x/exp_1", "eda.exp_1",
                                                                            "eda.exp_1.dlq")
    assert env["INTAKE_URL"] == "http://127.0.0.1:5" and env["EXECUTOR_ISSUE_REFUND_URL"] == "http://127.0.0.1:6"
    assert env["LLM_CLIENT"] == "oracle" and env["KAFKA_SESSION_TIMEOUT_MS"] == ""


class _Popen:
    def __init__(self, argv, **kw) -> None:
        self.argv, self.kw = argv, kw
        self.pid = 4242
        self.returncode: int | None = None
        self.signals: list[int] = []

    def poll(self):
        return self.returncode

    def send_signal(self, sig):
        self.signals.append(sig)
        self.returncode = 0

    def wait(self, timeout=None):
        return self.returncode

    def kill(self):
        self.returncode = -9


def test_start_components_launches_each_module_with_its_args_env_and_log(tmp_path: Path):
    """Cada componente arranca con el intérprete de este proceso, su módulo y sus argumentos, en el directorio de
    logs (sin `.env`), con su log y, si es un ejecutor, con `SERVICE_EXECUTIONS_LOG`"""
    started: list[_Popen] = []

    def popen(argv, **kw):
        started.append(_Popen(argv, **kw))
        return started[-1]

    chosen = [c for c in COMPONENTS if c.name in ("intake", "executor-file_claim", "projector")]
    procs = start_components(chosen, {"DATABASE_URL": "db"}, {"intake": "http://127.0.0.1:7"}, tmp_path,
                             popen=popen)
    assert [p.name for p in procs] == ["projector", "intake", "executor-file_claim"]
    for c, launched in zip(chosen, started, strict=True):
        assert launched.argv == [sys.executable, "-c", component_launcher(c.module), *c.args]
        assert launched.kw["cwd"] == tmp_path and launched.kw["env"]["DATABASE_URL"] == "db"
        assert launched.kw["stdout"].name == str(tmp_path / f"{c.name}.log")
    assert "SERVICE_EXECUTIONS_LOG" not in started[1].kw["env"]
    assert started[2].kw["env"]["SERVICE_EXECUTIONS_LOG"] == str(tmp_path / "executor-file_claim.executions.jsonl")
    assert procs[1].url == "http://127.0.0.1:7" and procs[0].url is None
    assert procs[2].executions_path == tmp_path / "executor-file_claim.executions.jsonl"


def _process(tmp_path: Path, name: str, *, group: str | None = None, url: str | None = None,
             log: str = "") -> ComponentProcess:
    path = tmp_path / f"{name}.log"
    path.write_text(log)
    return ComponentProcess(name, _Popen([]), path, component=Component(name, "m", group=group), url=url)


def test_assigned_partitions_read_from_the_process_log():
    lines = ["2026-11-18 10:00:00,000 INFO [interpreter] interpreter: particiones asignadas [0, 1, 2]",
             "2026-11-18 10:00:01,000 INFO [orchestrator] orchestrator: particiones asignadas [1]"]
    assert assigned_partitions(lines, "interpreter", "interpreter") == [0, 1, 2]
    assert assigned_partitions(lines[:1], "orchestrator", "orchestrator") is None
    revoked = [*lines, "2026-11-18 10:00:02,000 INFO [interpreter] interpreter: particiones revocadas [0, 1, 2]"]
    assert assigned_partitions(revoked, "interpreter", "interpreter") == []


def test_wait_ready_waits_for_ports_and_group_assignments(tmp_path: Path):
    """Procesos listos antes de empezar: el harness espera a que cada proceso con API acepte conexiones y a que cada
    consumidor de grupo tenga particiones"""
    api = _process(tmp_path, "intake", url="http://127.0.0.1:9")
    consumer = _process(tmp_path, "interpreter", group="interpreter")
    opened = iter([False, True, True, True])
    turns = []

    def sleep(_s):
        turns.append(1)
        consumer.log_path.write_text("x INFO [interpreter] interpreter: particiones asignadas [0, 1, 2]\n")

    wait_ready([api, consumer], 5, is_open=lambda url: next(opened), sleep=sleep)
    assert turns == [1]


def test_wait_ready_fails_when_a_process_dies(tmp_path: Path):
    """Procesos listos antes de empezar: si un proceso muere al arrancar, el error da su nombre, su código y el final
    de su log"""
    dead = _process(tmp_path, "planner", url="http://127.0.0.1:9", log="Traceback\nValueError: LLM_CLIENT\n")
    dead.popen.returncode = 1
    with pytest.raises(RuntimeError, match=r"planner terminó al arrancar con código 1:\nTraceback"):
        wait_ready([dead], 5, is_open=lambda url: False, sleep=lambda s: None)


def test_wait_ready_times_out(tmp_path: Path):
    slow = _process(tmp_path, "intake", url="http://127.0.0.1:9")
    clock = iter([0.0, 0.0, 10.0])
    with pytest.raises(RuntimeError, match=r"no están listos en 5 s: \['intake'\]"):
        wait_ready([slow], 5, is_open=lambda url: False, sleep=lambda s: None, clock=lambda: next(clock))


def test_stop_all_interrupts_every_process_and_tells_deaths_apart(tmp_path: Path, monkeypatch):
    """Parada y procesos caídos: el harness para todos los procesos con SIGINT y da su código de salida; un proceso
    que terminó solo antes de la parada consta como caído"""
    sent = []
    monkeypatch.setattr(processes.os, "kill", lambda pid, sig: sent.append((pid, sig)))
    alive = _process(tmp_path, "intake")
    dead = _process(tmp_path, "planner")
    dead.popen.returncode = 1
    assert dead.died() and not alive.died()
    assert stop_all([alive, dead]) == {"intake": 0, "planner": 1}
    assert alive.popen.signals == [signal.SIGINT] and sent == [(4242, signal.SIGCONT)]
    assert not alive.died() and dead.died()


def test_component_launcher_passes_args_and_exits_zero_on_sigint(tmp_path: Path):
    """El lanzador pasa los argumentos al módulo y, con SIGINT, deja que su `finally` se ejecute y sale con 0"""
    package = tmp_path / "pkg"
    package.mkdir()
    marker = tmp_path / "out"
    (package / "probe.py").write_text(
        "import pathlib, sys, time\n"
        "if __name__ == '__main__':\n"
        f"    out = pathlib.Path({str(marker)!r})\n"
        "    try:\n        out.write_text(' '.join(sys.argv[1:]))\n        time.sleep(60)\n"
        "    finally:\n        out.write_text(out.read_text() + ' closed')\n")
    popen = subprocess.Popen([sys.executable, "-c", component_launcher("probe"), "file_claim"], cwd=package,
                             env={"PYTHONPATH": str(package)})
    process = ComponentProcess("probe", popen, tmp_path / "probe.log")
    end = time.monotonic() + 10
    while not marker.exists() and time.monotonic() < end:
        time.sleep(0.05)
    assert stop_all([process]) == {"probe": 0}
    assert marker.read_text() == "file_claim closed" and not process.died()
