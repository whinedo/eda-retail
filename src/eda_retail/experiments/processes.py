"""Modo multiproceso del harness (spec experiments, «Modo multiproceso del harness»; E16).

Solo el orquestador corre en procesos propios (decisión del usuario): N instancias de `eda-orchestrator`, el mismo
proceso que `scripts/run-agents.sh`, en el grupo `orchestrator` y contra la base de datos y el topic de la ejecución.
El resto de componentes sigue en el harness, con el cliente LLM simulado, y atiende los comandos de los
orquestadores por REST en `CommandGateway`, un servidor HTTP local con un prefijo por destino.

Cada instancia escribe su log en un fichero; `read_log` lo convierte en registros: rebalanceos del grupo, mensajes
procesados (inicio y fin) y esperas al bloqueo de la incidencia. La congelación (spec synthetic-data, «Congelación de
un proceso») es `SIGSTOP` y la reanudación `SIGCONT`.
"""

from __future__ import annotations

import contextlib
import os
import re
import signal
import socket
import subprocess
import sys
import threading
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

import psycopg
import uvicorn
from fastapi import FastAPI

from eda_retail.config import Settings
from eda_retail.infra.commands import CommandEnvelope, CommandResponse, CommandSpec, command_router, execute_command

ORCHESTRATOR_MODULE = "eda_retail.orchestrator.cli"
TARGET_URL_ENV = {
    "intake": "INTAKE_URL", "inventory": "INVENTORY_URL", "planner": "PLANNER_URL", "replanner": "REPLANNER_URL",
    "executor_notify_customer": "EXECUTOR_NOTIFY_CUSTOMER_URL", "executor_file_claim": "EXECUTOR_FILE_CLAIM_URL",
    "executor_reissue_shipment": "EXECUTOR_REISSUE_SHIPMENT_URL", "executor_issue_refund": "EXECUTOR_ISSUE_REFUND_URL",
}
START_TIMEOUT_S = 10.0
STOP_TIMEOUT_S = 10.0

_LINE = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d,\d{3}) (\w+) \[orchestrator\] (.*)$")
_REBALANCE = re.compile(r"^orchestrator: particiones (asignadas|revocadas|perdidas) \[([\d, ]*)\]$")
_START = re.compile(r"^-> (\w+) (\S+) p(\d+) o(\d+)$")
_END = re.compile(r"^<- (\w+) (\S+): (\w+)$")
_LOCK = re.compile(r"^bloqueo de (\S+): ([\d.]+) s en (\S+)$")
REBALANCE_KIND = {"asignadas": "assigned", "revocadas": "revoked", "perdidas": "lost"}


# -- servidor de comandos ---------------------------------------------------------------------
Track = Callable[[Callable[[], CommandResponse]], CommandResponse]


class CommandGateway:
    """Los receptores de comandos del harness tras un servidor HTTP local: `POST <base>/<destino>/commands/<Comando>`.

    `targets`: destino -> (esquema, specs, inbox activo). Cada petición abre su conexión y ejecuta el comando con
    `execute_command`, dentro de `track` (el harness la cuenta como trabajo en curso)."""

    def __init__(self, targets: Mapping[str, tuple[str, Mapping[str, CommandSpec], bool]],
                 connect: Callable[[], psycopg.Connection], track: Track) -> None:
        app = FastAPI()
        for name, (schema, specs, inbox_enabled) in targets.items():
            app.include_router(command_router(schema, specs, connect, executor=self._executor(inbox_enabled, track)),
                               prefix=f"/{name}")
        self._names = list(targets)
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self.base_url = f"http://127.0.0.1:{self._sock.getsockname()[1]}"
        self._server = uvicorn.Server(uvicorn.Config(app, log_level="warning", lifespan="off"))
        self._thread = threading.Thread(target=self._server.run, kwargs={"sockets": [self._sock]}, daemon=True,
                                        name="harness-command-gateway")

    @staticmethod
    def _executor(inbox_enabled: bool, track: Track) -> Callable[..., CommandResponse]:
        def run(conn: psycopg.Connection, schema: str, command: CommandEnvelope, spec: CommandSpec) -> CommandResponse:
            return track(lambda: execute_command(conn, schema, command, spec, inbox_enabled=inbox_enabled))

        return run

    def urls(self) -> dict[str, str]:
        return {name: f"{self.base_url}/{name}" for name in self._names}

    def start(self) -> CommandGateway:
        self._thread.start()
        end = time.monotonic() + START_TIMEOUT_S
        while not self._server.started:
            if time.monotonic() > end or not self._thread.is_alive():
                raise RuntimeError("el servidor de comandos del harness no arranca")
            time.sleep(0.01)
        return self

    def stop(self) -> None:
        self._server.should_exit = True
        self._thread.join(STOP_TIMEOUT_S)
        with contextlib.suppress(OSError):
            self._sock.close()


# -- procesos del orquestador -----------------------------------------------------------------
def orchestrator_env(settings: Settings, urls: Mapping[str, str], *, session_timeout_ms: int | None,
                     base: Mapping[str, str] | None = None) -> dict[str, str]:
    """El entorno de una instancia: el de este proceso con todo lo de la ejecución explícito (base de datos, topic,
    plazos, URLs del servidor de comandos), para que ningún `.env` lo cambie."""
    env = dict(os.environ if base is None else base)
    env.update({
        "DATABASE_URL": settings.database_url,
        "KAFKA_BOOTSTRAP_SERVERS": settings.kafka_bootstrap_servers,
        "EVENTS_TOPIC": settings.events_topic,
        "DLQ_TOPIC": settings.dlq_topic,
        "POC_VERSION": str(settings.poc_version),
        "INBOX_ENABLED": "true" if settings.inbox_enabled else "false",
        "SAGA_STEP_TIMEOUT_S": str(settings.saga_step_timeout_s),
        "SAGA_PLAN_TIMEOUT_S": str(settings.saga_plan_timeout_s),
        "COMMAND_HTTP_TIMEOUT_S": str(settings.command_http_timeout_s),
        "STEP_BACKOFF_BASE_SECONDS": str(settings.step_backoff_base_seconds),
        "STEP_BACKOFF_MAX_SECONDS": str(settings.step_backoff_max_seconds),
        "TRACING_ENABLED": "false",
        "LOG_LEVEL": "DEBUG",  # inicio de cada mensaje y esperas al bloqueo
        "KAFKA_SESSION_TIMEOUT_MS": "" if session_timeout_ms is None else str(session_timeout_ms),
    })
    env.update({TARGET_URL_ENV[target]: url for target, url in urls.items()})
    return env


@dataclass
class OrchestratorProcess:
    """Una instancia de `eda-orchestrator` con su log. `frozen_at`/`resumed_at`: instantes (epoch) de cada
    congelación y reanudación."""

    name: str
    popen: subprocess.Popen
    log_path: Path
    frozen_at: list[float] = field(default_factory=list)
    resumed_at: list[float] = field(default_factory=list)

    @property
    def pid(self) -> int:
        return self.popen.pid

    def alive(self) -> bool:
        return self.popen.poll() is None

    def freeze(self) -> float:
        """`SIGSTOP`: el proceso entero (consumidor, heartbeats de Kafka, relay y dispatcher) deja de ejecutarse."""
        os.kill(self.pid, signal.SIGSTOP)
        self.frozen_at.append(time.time())
        return self.frozen_at[-1]

    def resume(self) -> float:
        os.kill(self.pid, signal.SIGCONT)
        self.resumed_at.append(time.time())
        return self.resumed_at[-1]

    def stop(self) -> int | None:
        """Lo para (reanudándolo antes, por si está congelado) y devuelve su código de salida."""
        if self.alive():
            with contextlib.suppress(ProcessLookupError):
                os.kill(self.pid, signal.SIGCONT)
                self.popen.terminate()
            try:
                self.popen.wait(STOP_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                self.popen.kill()
                self.popen.wait(STOP_TIMEOUT_S)
        return self.popen.returncode

    def log(self) -> ProcessLog:
        return read_log(self.log_path.read_text(errors="replace").splitlines())


def start_orchestrators(n: int, settings: Settings, urls: Mapping[str, str], log_dir: Path, *,
                        session_timeout_ms: int | None) -> list[OrchestratorProcess]:
    """Arranca `n` instancias (`orchestrator-a`, `orchestrator-b`...) con el intérprete de Python de este proceso:
    las señales llegan a Python, no a un lanzador como `uv run`."""
    env = orchestrator_env(settings, urls, session_timeout_ms=session_timeout_ms)
    processes = []
    for i in range(n):
        name = f"orchestrator-{chr(ord('a') + i)}"
        log_path = log_dir / f"{name}.log"
        with log_path.open("wb") as out:
            popen = subprocess.Popen([sys.executable, "-m", ORCHESTRATOR_MODULE], cwd=log_dir, env=env,
                                     stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        processes.append(OrchestratorProcess(name, popen, log_path))
    return processes


# -- log de una instancia ---------------------------------------------------------------------
@dataclass(frozen=True)
class Message:
    """Un mensaje que la instancia empezó a procesar; `end` y `outcome` son None si no lo terminó."""

    event_type: str
    incident: str
    partition: int
    offset: int
    start: float
    end: float | None = None
    outcome: str | None = None


@dataclass
class ProcessLog:
    rebalances: list[tuple[float, str, list[int]]] = field(default_factory=list)  # (instante, tipo, particiones)
    messages: list[Message] = field(default_factory=list)
    lock_waits: list[tuple[float, str, float, str]] = field(default_factory=list)  # (instante, incidencia, s, hilo)

    def assignment(self, before: float | None = None) -> list[int]:
        """Las particiones asignadas tras el último rebalanceo anterior a `before` (todos, si es None)."""
        current: list[int] = []
        for t, kind, partitions in self.rebalances:
            if before is not None and t > before:
                break
            current = partitions if kind == "assigned" else []
        return current


def _epoch(stamp: str) -> float:
    return datetime.strptime(stamp, "%Y-%m-%d %H:%M:%S,%f").timestamp()


def read_log(lines: Iterable[str]) -> ProcessLog:
    """Los registros de una instancia a partir de las líneas de su log; ignora lo demás."""
    out = ProcessLog()
    open_message: Message | None = None
    for line in lines:
        m = _LINE.match(line)
        if m is None:
            continue
        t, text = _epoch(m[1]), m[3]
        if r := _REBALANCE.match(text):
            out.rebalances.append((t, REBALANCE_KIND[r[1]], [int(p) for p in r[2].split(",") if p.strip()]))
        elif r := _START.match(text):
            if open_message is not None:
                out.messages.append(open_message)  # empezó otro sin terminar este (cayó en el handler)
            open_message = Message(r[1], r[2], int(r[3]), int(r[4]), t)
        elif r := _END.match(text):
            if open_message is not None and (open_message.event_type, open_message.incident) == (r[1], r[2]):
                out.messages.append(Message(**{**open_message.__dict__, "end": t, "outcome": r[3]}))
                open_message = None
        elif r := _LOCK.match(text):
            out.lock_waits.append((t, r[1], float(r[2]), r[3]))
    if open_message is not None:
        out.messages.append(open_message)
    return out


def wait_for_group(processes: list[OrchestratorProcess], partitions: Iterable[int], timeout: float,
                   *, step: Callable[[], Any] = lambda: None) -> dict[str, list[int]]:
    """Espera a que cada instancia tenga particiones y entre todas cubran `partitions`; devuelve la asignación.
    `step` avanza el harness mientras tanto."""
    wanted = set(partitions)
    end = time.monotonic() + timeout
    while True:
        assignment = {p.name: p.log().assignment() for p in processes}
        if all(assignment.values()) and set().union(*assignment.values()) == wanted:
            return assignment
        dead = [p.name for p in processes if not p.alive()]
        if dead or time.monotonic() > end:
            raise RuntimeError(f"el grupo del orquestador no se forma: asignación {assignment}, caídas {dead}")
        step()
        time.sleep(0.05)
