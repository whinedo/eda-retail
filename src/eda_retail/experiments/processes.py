"""Modo multiproceso del harness (spec experiments, «Modo multiproceso del harness»; E16).

Solo el orquestador corre en procesos propios (decisión del usuario): N instancias de `eda-orchestrator`, el mismo
proceso que `scripts/run-agents.sh`, en el grupo `orchestrator` y contra la base de datos y el topic de la ejecución.
El resto de componentes sigue en el harness, con el cliente LLM simulado, y atiende los comandos de los
orquestadores por REST en `CommandGateway`, un servidor HTTP local con un prefijo por destino.

Cada instancia escribe su log en un fichero; `read_log` lo convierte en registros: rebalanceos del grupo, mensajes
procesados (inicio y fin) y esperas al bloqueo de la incidencia. La congelación (spec synthetic-data, «Congelación de
un proceso») es `SIGSTOP` y la reanudación `SIGCONT`.

Los comandos que llegan por `CommandGateway` no pasan por `InProcessCommands`: los fallos de entrega del `FaultPlan`
(`duplicates`, `concurrent_commands`, `reinvoke_steps`, `executor_faults`) no se aplican en este modo; los de las specs
de los receptores (fallos, retrasos y caídas de los ejecutores) sí.

Todos los componentes en procesos propios (spec experiments, «Todos los componentes en procesos propios»): `COMPONENTS`
son los procesos de `scripts/run-agents.sh` (versión 1). `start_components` los lanza con el intérprete de este proceso
y un log por proceso, `wait_ready` espera a que escuchen en su puerto y a que los consumidores de grupo tengan
particiones, y `stop_all` los para a la vez. `ComponentProcess` dice si un proceso murió solo (`died`) y con qué
código, y da su log (`log_text`); los ejecutores anotan sus ejecuciones en `executions_path`.
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
from eda_retail.infra.component import listen_address
from eda_retail.step_executors.actions import ACTIONS as EXECUTOR_ACTIONS

ORCHESTRATOR_MODULE = "eda_retail.orchestrator.cli"
# Como `python -m <módulo>`, pero con SIGINT convertido en `KeyboardInterrupt` aunque este proceso lo ignore (lanzado
# en segundo plano): `OrchestratorProcess.stop` lo usa para que el orquestador cierre su consumidor y salga del grupo.
LAUNCHER = ("import runpy, signal; signal.signal(signal.SIGINT, signal.default_int_handler); "
            f"runpy.run_module({ORCHESTRATOR_MODULE!r}, run_name='__main__', alter_sys=True)")
TARGET_URL_ENV = {
    "intake": "INTAKE_URL", "inventory": "INVENTORY_URL", "planner": "PLANNER_URL", "replanner": "REPLANNER_URL",
    "executor_notify_customer": "EXECUTOR_NOTIFY_CUSTOMER_URL", "executor_file_claim": "EXECUTOR_FILE_CLAIM_URL",
    "executor_reissue_shipment": "EXECUTOR_REISSUE_SHIPMENT_URL", "executor_issue_refund": "EXECUTOR_ISSUE_REFUND_URL",
}
START_TIMEOUT_S = 10.0
STOP_TIMEOUT_S = 20.0  # el orquestador para su relay (10 s) y sus hilos (5 s) antes de salir

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
class ManagedProcess:
    """Un proceso lanzado por el harness con su log. `frozen_at`/`resumed_at`: instantes (epoch) de cada
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
        """Lo para (reanudándolo antes, por si está congelado) y devuelve su código de salida. Con `SIGINT`, no
        `SIGTERM`: el `KeyboardInterrupt` deja que `cli.main` cierre el consumidor y salga del grupo; con `SIGTERM`
        el proceso muere sin hacerlo y su miembro sigue en el grupo `orchestrator` hasta que vence su sesión,
        retrasando el rebalanceo de las ejecuciones siguientes."""
        if self.alive():
            self.interrupt()
            self.wait_stopped()
        return self.popen.returncode

    def interrupt(self) -> None:
        """La primera mitad de `stop`: lo reanuda y le manda `SIGINT`, sin esperar."""
        with contextlib.suppress(ProcessLookupError):
            os.kill(self.pid, signal.SIGCONT)
            self.popen.send_signal(signal.SIGINT)

    def wait_stopped(self) -> int | None:
        """La segunda mitad de `stop`: espera a que salga y, pasado `STOP_TIMEOUT_S`, lo mata."""
        try:
            self.popen.wait(STOP_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            self.popen.kill()
            self.popen.wait(STOP_TIMEOUT_S)
        return self.popen.returncode

    @property
    def returncode(self) -> int | None:
        return self.popen.poll()

    def log_text(self) -> str:
        """Lo que el proceso escribió hasta ahora (stdout y stderr); vacío si aún no hay fichero."""
        return self.log_path.read_text(errors="replace") if self.log_path.is_file() else ""


@dataclass
class OrchestratorProcess(ManagedProcess):
    """Una instancia de `eda-orchestrator` (E16)."""

    def log(self) -> ProcessLog:
        return read_log(self.log_text().splitlines())


def start_orchestrators(n: int, settings: Settings, urls: Mapping[str, str], log_dir: Path, *,
                        session_timeout_ms: int | None,
                        processes: list[OrchestratorProcess] | None = None) -> list[OrchestratorProcess]:
    """Arranca `n` instancias (`orchestrator-a`, `orchestrator-b`...) con el intérprete de Python de este proceso:
    las señales llegan a Python, no a un lanzador como `uv run`. Las añade a `processes` según arrancan, para que
    quien las pare las encuentre aunque falle el arranque de una posterior."""
    env = orchestrator_env(settings, urls, session_timeout_ms=session_timeout_ms)
    processes = [] if processes is None else processes
    for i in range(n):
        name = f"orchestrator-{chr(ord('a') + i)}"
        log_path = log_dir / f"{name}.log"
        with log_path.open("wb") as out:
            popen = subprocess.Popen([sys.executable, "-c", LAUNCHER], cwd=log_dir, env=env,
                                     stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        processes.append(OrchestratorProcess(name, popen, log_path))
    return processes


# -- todos los componentes en procesos propios -------------------------------------------------
@dataclass(frozen=True)
class Component:
    """Un proceso de `scripts/run-agents.sh`: su nombre, su módulo (el de su comando `eda-*`) y sus argumentos.
    `target`: el destino de comandos que atiende por REST (clave de `TARGET_URL_ENV`); `group`: el grupo de Kafka
    cuyo log de asignación dice que ya consume."""

    name: str
    module: str
    args: tuple[str, ...] = ()
    target: str | None = None
    group: str | None = None


COMPONENTS: tuple[Component, ...] = (
    Component("projector", "eda_retail.projectors.cli"),
    Component("event-log", "eda_retail.event_log.cli"),
    Component("intake", "eda_retail.intake.cli", target="intake"),
    Component("inventory", "eda_retail.inventory.cli", target="inventory"),
    Component("orchestrator", ORCHESTRATOR_MODULE, group="orchestrator"),
    Component("interpreter", "eda_retail.interpreter.cli", group="interpreter"),
    Component("planner", "eda_retail.planner.cli", target="planner"),
    Component("replanner", "eda_retail.replanner.cli", target="replanner"),
    *(Component(f"executor-{a}", "eda_retail.step_executors.cli", (a,), target=f"executor_{a}")
      for a in EXECUTOR_ACTIONS),
)
COMPONENT_START_TIMEOUT_S = 90.0  # importar, migrar nada (la base ya existe), escuchar y unirse al grupo
EXECUTIONS_ENV = "SERVICE_EXECUTIONS_LOG"  # step_executors/executions.py


def component_launcher(module: str) -> str:
    """Como `LAUNCHER`, para cualquier módulo: `SIGINT` es `KeyboardInterrupt` y, si el proceso sale por él tras su
    limpieza, el código de salida es 0. Así un código distinto de 0 es siempre una muerte, no una parada."""
    return ("import runpy, signal, sys\n"
            "signal.signal(signal.SIGINT, signal.default_int_handler)\n"
            "try:\n"
            f"    runpy.run_module({module!r}, run_name='__main__', alter_sys=True)\n"
            "except KeyboardInterrupt:\n"
            "    sys.exit(0)\n")


def free_ports(n: int) -> list[int]:
    """`n` puertos libres de 127.0.0.1, todos distintos (se abren a la vez y se cierran antes de devolverlos)."""
    socks = []
    try:
        for _ in range(n):
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.bind(("127.0.0.1", 0))
            socks.append(sock)
        return [sock.getsockname()[1] for sock in socks]
    finally:
        for sock in socks:
            sock.close()


def component_urls(ports: Mapping[str, int]) -> dict[str, str]:
    """Destino de comandos -> URL base de su proceso, en 127.0.0.1."""
    return {target: f"http://127.0.0.1:{port}" for target, port in ports.items()}


def component_env(settings: Settings, urls: Mapping[str, str], *, extra: Mapping[str, str] | None = None,
                  base: Mapping[str, str] | None = None) -> dict[str, str]:
    """El entorno común de los procesos: el de `orchestrator_env` (base de datos, topic, plazos, las URL de cada
    destino) más `extra`, sin sesión de Kafka propia."""
    env = orchestrator_env(settings, urls, session_timeout_ms=None, base=base)
    env.update(extra or {})
    return env


_ASSIGNED = r" \[{tag}\] {group}: particiones (asignadas|revocadas|perdidas) \[([\d, ]*)\]$"


def assigned_partitions(lines: Iterable[str], tag: str, group: str) -> list[int] | None:
    """Las particiones del grupo `group` tras el último rebalanceo que registró el proceso `tag`; None si no hay."""
    pattern = re.compile(_ASSIGNED.format(tag=re.escape(tag), group=re.escape(group)))
    current = None
    for line in lines:
        if m := pattern.search(line):
            current = [int(p) for p in m[2].split(",") if p.strip()] if m[1] == "asignadas" else []
    return current


@dataclass
class ComponentProcess(ManagedProcess):
    """Un componente en su proceso. `url`: dónde atiende comandos (None si no atiende); `executions_path`: el JSONL de
    las ejecuciones de su servicio simulado (solo los ejecutores); `stopping`: el harness lo está parando."""

    component: Component | None = None
    url: str | None = None
    executions_path: Path | None = None
    stopping: bool = False

    def interrupt(self) -> None:
        self.stopping = True
        super().interrupt()

    def died(self) -> bool:
        """Terminó sin que el harness lo parase."""
        return not self.alive() and not self.stopping

    def assignment(self) -> list[int] | None:
        group = self.component.group if self.component is not None else None
        if group is None:
            return None
        return assigned_partitions(self.log_text().splitlines(), self.name, group)

    def ready(self, is_open: Callable[[str], bool]) -> bool:
        """Acepta conexiones en su `url` (si atiende comandos) y, si consume en un grupo, ya tiene particiones."""
        if self.url is not None and not is_open(self.url):
            return False
        return self.component is None or self.component.group is None or bool(self.assignment())


Popen = Callable[..., subprocess.Popen]


def start_components(components: Iterable[Component], env: Mapping[str, str], urls: Mapping[str, str],
                     log_dir: Path, *, processes: list[ComponentProcess] | None = None,
                     popen: Popen = subprocess.Popen) -> list[ComponentProcess]:
    """Lanza cada componente con el intérprete de este proceso (`component_launcher`), en `log_dir` (sin `.env`) y con
    su log `<nombre>.log`. Los ejecutores reciben además `SERVICE_EXECUTIONS_LOG=<nombre>.executions.jsonl`. Los
    añade a `processes` según arrancan, para que quien los pare los encuentre aunque falle uno posterior."""
    processes = [] if processes is None else processes
    for component in components:
        log_path = log_dir / f"{component.name}.log"
        own = dict(env)
        executions = None
        if component.module == "eda_retail.step_executors.cli":
            executions = log_dir / f"{component.name}.executions.jsonl"
            own[EXECUTIONS_ENV] = str(executions)
        with log_path.open("wb") as out:
            process = popen([sys.executable, "-c", component_launcher(component.module), *component.args],
                            cwd=log_dir, env=own, stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL)
        processes.append(ComponentProcess(component.name, process, log_path, component=component,
                                          url=urls.get(component.target) if component.target else None,
                                          executions_path=executions))
    return processes


def port_open(url: str, timeout: float = 0.2) -> bool:
    """El proceso de `url` acepta conexiones TCP."""
    try:
        with socket.create_connection(listen_address(url), timeout=timeout):
            return True
    except OSError:
        return False


def _tail(process: ManagedProcess, lines: int = 20) -> str:
    return "\n".join(process.log_text().splitlines()[-lines:])


def wait_ready(processes: Iterable[ComponentProcess], timeout: float = COMPONENT_START_TIMEOUT_S, *,
               is_open: Callable[[str], bool] = port_open, sleep: Callable[[float], None] = time.sleep,
               clock: Callable[[], float] = time.monotonic) -> None:
    """Espera a que cada proceso con `url` acepte conexiones y a que cada consumidor de grupo tenga particiones.
    RuntimeError si uno muere (con su código y el final de su log) o si vence `timeout`."""
    pending = list(processes)
    end = clock() + timeout
    while pending:
        for process in pending:
            if process.died():
                raise RuntimeError(f"{process.name} terminó al arrancar con código {process.returncode}:\n"
                                   f"{_tail(process)}")
        pending = [p for p in pending if not p.ready(is_open)]
        if not pending:
            return
        if clock() > end:
            raise RuntimeError(f"los procesos no están listos en {timeout} s: {[p.name for p in pending]}")
        sleep(0.1)


def stop_all(processes: Iterable[ComponentProcess]) -> dict[str, int | None]:
    """Para todos a la vez (`SIGINT` a cada uno y después la espera) y devuelve el código de salida de cada uno."""
    processes = list(processes)
    for process in processes:
        if process.alive():
            process.interrupt()
    return {process.name: process.wait_stopped() for process in processes}


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
