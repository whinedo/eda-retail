"""Arnés de los experimentos (decisión D1 de odd/tasks/experiments.md; T11 de eda-component-ownership).

Monta los componentes de `scripts/run-agents.sh` en un solo proceso contra PostgreSQL y Kafka reales.
Cada ejecución usa una base de datos y un topic propios, así no ve datos de otras.

- Comandos: `intake`, `inventory`, `planner`, `replanner` y los cuatro ejecutores atienden sus comandos
  con su propio command handler (`execute_command`), sin HTTP: `InProcessCommands` sustituye a
  `command_client.invoke` en el dispatcher de la saga. Un comando con trabajo largo (`CreatePlan`,
  `RevisePlan`, `ExecuteStep` de los ejecutores) solo se acepta en la llamada; su trabajo lo hace el
  worker de su componente.
- Hilos: lo que en el despliegue es un proceso aparte y tarda (el LLM, los servicios) corre en su propio
  hilo, con su propia conexión, como lo haría ese proceso: el worker de trabajos de cada componente
  (`JobWorker`) y la entrega del intérprete. Así E6 mide el diseño y no un arnés de un solo hilo.
- Turnos: el resto se atiende por turnos en el hilo principal: relays, lado de lectura, consumidor del
  orquestador, dispatcher, plazos de la saga y el de los avisos aparcados.
- Eventos: el intérprete y el orquestador consumen Kafka envueltos en `FaultyDelivery`; `event_log` y
  los proyectores mantienen el lado de lectura; cada componente publica su outbox con su relay.

Fallos (spec experiments; L55):
- E1: cada evento llega `duplicates` veces a cada consumidor de Kafka y cada comando se entrega
  `duplicates` veces con el mismo `command_id` (entrega al menos una vez del dispatcher).
- E2: un ejecutor cae tras confirmar su transacción y antes de responder (`crash_after` con el nombre
  `executor-<acción>`); la saga lo reinvoca con el mismo `command_id`. Con un nombre de consumidor
  (`interpreter`, `orchestrator`), cae el consumidor sin confirmar el offset y se crea otro.
- Pasos que fallan (`fail_probability`): el `ExecuteStep` elegido responde `StepFailed`
  `injected_failure` sin preparar ni ejecutar el efecto.
- E21: `replay` para el sistema, lleva al principio los offsets de los grupos de Kafka, rebobina el lado de
  lectura y arranca otra vez hasta que el lag sea 0; `redeliver` entrega de nuevo los comandos ya atendidos
  con su `command_id` original.
- E22 (spec synthetic-data, «Entrega concurrente de comandos»): `concurrent_commands` entrega el comando desde N
  hilos a la vez, cada uno con su conexión (`deliver_concurrently`); `reinvoke_steps` entrega a la vez el mismo
  `ExecuteStep` con otro `command_id`; `crash_between_effect_and_record` hace caer el ejecutor entre el efecto y
  el registro de su clave (`crashing`). `InProcessCommands.deliveries` anota las respuestas de cada entrega,
  `busy_claims` las claves que un trabajo encontró `in_progress` y `RunConfig.job_workers` da varios workers por
  componente para que dos trabajos con la misma clave corran a la vez.
- E12 (spec synthetic-data, «Ejecutor caído o colgado»): `executor_faults` deja un sistema destino `down` o `hang`
  durante un intervalo o para siempre (`InProcessCommands`); al recuperarse, el sistema colgado atiende una vez lo
  que tenía aceptado (`flush_recovered`, en cada turno). Con `RunConfig.dispatcher_thread`, el dispatcher va en su
  propio hilo, como en el orquestador, y un ejecutor colgado no para los relays ni el lado de lectura.
- E14 (spec synthetic-data, «Capacidad limitada de un ejecutor y trabajo lento»): con `executor_capacity`,
  `InProcessCommands` responde 503 (`CommandUnavailable`) a un `ExecuteStep` nuevo si el ejecutor ya tiene tantos
  trabajos abiertos como su capacidad y lo cuenta en `unavailable_503`; `prepare_delay_seconds` retrasa el `prepare`
  de los trabajos de un componente (`slowed`).
- E19 (spec synthetic-data, «Pérdida del topic»): con el sistema parado, `lose_topic` vacía el topic o lo borra y lo
  crea de nuevo (recreando productores y lado de lectura, con los grupos en 0 y la posición del lado de lectura en -1),
  y `republish` republica a la vez el outbox de todos los componentes en el orden que cada partición tenía
  (`outbox.republish_in_order`).
- E9 (spec synthetic-data, «Caída del broker»): con `RunConfig.tolerate_relay_errors`, el turno trata un lote que el
  broker no confirma como `ComponentRelay.run_until`: lo cuenta en `relay_errors` y sigue (las filas quedan
  pendientes), con el plazo de entrega `relay_flush_timeout_seconds`; `consumer_session_timeout_ms` acorta lo que un
  commit espera sin broker. Un fallo de commit de offset (`KafkaException`, también si no se confirma en su plazo)
  reinicia el consumidor como cualquier caída, pero cuenta en `commit_failures` y no como veneno: el mensaje ya se
  procesó y el inbox descarta la reentrega. Al parar, un consumidor cuyo hilo no termina no se cierra.

Modos de idempotencia (E1, supuesto A3):
- `full`: inbox en los consumidores y en los comandos, `effect_keys` y servicios con clave.
- `no_effect_keys`: los ejecutores e `inventory` pierden su inbox de comandos, `effect_keys` y la
  deduplicación de los servicios; el resto conserva su inbox. Es lo que mide H1.
- `none`: todo desactivado.
Un duplicado es una ejecución externa repetida del mismo paso o una reserva de más.

Versión 2 (`POC_VERSION=2`): `notify_customer` envía el correo con su cliente MCP a un servidor de correo en
proceso (`mail_server_for`), sin el proceso `eda-mail-server`. Su buzón no deduplica: genera el `message_id`
con el servicio simulado `mail`, que conserva la semántica de E1 (clave reconocida o no) y cuenta las
ejecuciones. Con la versión 1, el ejecutor llama al servicio simulado directamente, como antes.

Todos los componentes en procesos propios (`RunConfig.component_processes`; spec experiments, «Todos los componentes en
procesos propios»): en lugar de montar los componentes, el harness lanza los procesos de `scripts/run-agents.sh`
(`processes.COMPONENTS`) contra la base de datos y el topic de la ejecución, con el oráculo como LLM (`LLM_CLIENT`) y
un puerto libre por componente, y solo siembra los datos, envía los avisos por REST y mide: un turno cuenta las filas
nuevas de `event_log`, `closed` lee las vistas que mantiene el proceso de los proyectores y los efectos repetidos salen
de los ficheros de ejecuciones de los ejecutores (`SERVICE_EXECUTIONS_LOG`). Los fallos del `FaultPlan`, los otros
modos de idempotencia y la versión 2 son del harness en proceso: este modo los rechaza (`check_component_mode`).
"""

from __future__ import annotations

import contextlib
import json
import logging
import sys
import tempfile
import threading
import time
import traceback
import uuid
from collections import Counter
from collections.abc import Callable, Iterable, Iterator
from dataclasses import dataclass, field, replace
from decimal import Decimal
from pathlib import Path
from typing import Any, Self

import psycopg
from confluent_kafka import KafkaException, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from mcp.server.mcpserver import MCPServer

from eda_retail.config import Settings
from eda_retail.event_log import cli as event_log
from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.processes import (
    COMPONENTS,
    CommandGateway,
    ComponentProcess,
    OrchestratorProcess,
    component_env,
    component_urls,
    free_ports,
    start_components,
    start_orchestrators,
    stop_all,
    wait_ready,
)
from eda_retail.infra import outbox
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.command_client import CommandReply, CommandUnavailable, invoke
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec, execute_command
from eda_retail.infra.consumer import EventConsumer
from eda_retail.infra.db import CONNECTION_ERRORS, connect
from eda_retail.infra.effect_keys import EffectInProgress
from eda_retail.infra.jobs import JobWorker, run_job_once
from eda_retail.infra.kafka import make_consumer, make_producer
from eda_retail.infra.migrations import upgrade
from eda_retail.infra.outbox import ComponentRelay, RelayDeliveryError
from eda_retail.infra.positions import caught_up, high_watermark, pending_messages
from eda_retail.infra.schemas import qualified
from eda_retail.intake import handler as intake
from eda_retail.interpreter import consumer as interpreter
from eda_retail.inventory import handler as inventory
from eda_retail.llm.generation import LlmClient
from eda_retail.mail_server.directory import synthetic_directory
from eda_retail.mail_server.server import build_server as build_mail_server
from eda_retail.mail_server.store import MailStore, MemoryMailStore
from eda_retail.orchestrator import consumer as orchestrator
from eda_retail.orchestrator.dispatcher import dispatch_once, expire_once
from eda_retail.orchestrator.saga import target_url
from eda_retail.planner import handler as planner
from eda_retail.planner.carrier import CarrierConsultant
from eda_retail.projectors.base import consumer_for
from eda_retail.projectors.registry import build_all
from eda_retail.replanner import handler as replanner
from eda_retail.rules import BusinessRules
from eda_retail.step_executors import component as step_executors
from eda_retail.step_executors.actions import ACTIONS as EXECUTOR_ACTIONS
from eda_retail.step_executors.executions import read_executions
from eda_retail.step_executors.services import SERVICE_OF, Services
from eda_retail.synthetic.faults import (
    INJECTED_FAILURE,
    ConsumerCrashed,
    EffectRecordCrash,
    EffectRecordCrashes,
    ExecutorOutage,
    FaultPlan,
    FaultyDelivery,
    group_lag,
    reset_group_offsets,
    should_fail,
    step_delay,
)
from eda_retail.synthetic.generator import Dataset, generate

log = logging.getLogger(__name__)

IDEMPOTENCY_MODES = ("full", "no_effect_keys", "none")
POISON_AFTER = 3  # caídas con el mismo mensaje antes de saltarlo
TURN_COMMANDS = 200  # comandos que el dispatcher invoca como mucho en un turno
# Un aviso cuyo pedido aún no está en view_orders se aparca (design.md §4.3); en el arnés el plazo es corto
# para que una carrera entre particiones no deje la incidencia abierta hasta el plazo del despliegue.
PENDING_NOTICE_TIMEOUT_S = 2.0
JOB_POLL_S = 0.05  # sondeo de un worker de trabajos sin trabajo
CONSUMER_POLL_S = 0.1  # sondeo de Kafka de un consumidor en su hilo
STALL_STREAM: Any = None  # dónde se vuelca el atasco al vencer `timeout_seconds`; None: `sys.__stderr__`
STALL_FRAMES = 15  # marcos de la pila de cada hilo en ese volcado
JOIN_TIMEOUT_S = 30.0  # espera al parar un hilo (puede estar dentro de una llamada al LLM)
TOPIC_LOSS_MODES = ("truncated", "recreated")
TOPIC_RECREATE_TIMEOUT_S = 60.0  # el borrado de un topic es asíncrono: se reintenta crearlo hasta este plazo
LOG_CATCH_UP_S = 30.0  # espera máxima de event_log a las marcas de agua altas antes de las invariantes
IDLE_SLEEP_S = 0.01  # pausa de un turno sin avance mientras los hilos trabajan
THREADED_CONSUMERS = frozenset({interpreter.SCHEMA})  # consumidores con su propio hilo
PUBLISHERS = ("intake", "interpreter", "planner", "replanner", "orchestrator", "inventory",
              *(f"executor_{a}" for a in EXECUTOR_ACTIONS))
READ_SIDE = ("event_log", "views")  # sin outbox; sus dead letters, migración 0024
PROCESS_POLL_S = 0.2  # pausa de un turno sin avance con todos los componentes en procesos propios
RECONNECT_TIMEOUT_S = 60.0  # espera máxima a que la base vuelva para la conexión del harness (E10)
RECONNECT_RETRY_S = 1.0


def positioned_lags(rows: Iterable[Any], highs: dict[int, int], consumers: Iterable[str]) -> dict[str, int]:
    """Lag de cada consumidor posicionado (`event_log`, cada proyector) desde sus filas de `*.offsets` (consumidor,
    partición y último offset aplicado) frente a las marcas de agua altas; sin fila, la partición está sin leer."""
    consumed: dict[str, dict[int, int]] = {c: {} for c in consumers}
    for r in rows:
        if r["consumer"] in consumed:
            consumed[r["consumer"]][r["kafka_partition"]] = r["kafka_offset"]
    return {c: pending_messages(offsets, highs) for c, offsets in consumed.items()}

@contextlib.contextmanager
def isolated_database(base: Settings, name: str) -> Iterator[str]:
    """Crea una base de datos propia con el esquema y las migraciones; la borra al salir."""
    base_url, _, _ = base.database_url.rpartition("/")
    with psycopg.connect(f"{base_url}/postgres", autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name}")
    try:
        url = f"{base_url}/{name}"
        upgrade(url)
        yield url
    finally:
        with contextlib.suppress(Exception), psycopg.connect(f"{base_url}/postgres", autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


# Restricciones de unicidad de negocio de las tablas de efectos (design.md §8): la variante «sin idempotencia»
# de E1 las elimina en su base de pruebas, junto con INBOX_ENABLED y EFFECT_KEYS_ENABLED.
BUSINESS_UNIQUES = ("executor_file_claim.claims_incident_order_key",
                    "executor_notify_customer.customer_messages_incident_step_key",
                    "executor_notify_customer.customer_messages_incident_order_version_key",
                    "executor_issue_refund.refunds_incident_step_key",
                    "executor_issue_refund.refunds_incident_order_key")


def drop_business_uniques(conn: psycopg.Connection) -> None:
    for index in BUSINESS_UNIQUES:
        conn.execute(f"DROP INDEX IF EXISTS {index}")
    conn.commit()


def service_duplicates(services: Services) -> Counter[str]:
    """Ejecuciones externas repetidas del mismo paso, por servicio simulado."""
    out: Counter[str] = Counter()
    for action, attr in SERVICE_OF.items():
        out[action] += sum(n - 1 for n in getattr(services, attr).executions.values() if n > 1)
    return out


def mail_server_for(services: Services, poc_version: int,
                    build: Callable[[MailStore], MCPServer] = build_mail_server) -> MCPServer | None:
    """El servidor de correo MCP en proceso de la versión 2, respaldado por el servicio simulado `mail`. `build`
    construye el servidor sobre ese buzón (E20 lo cambia por uno con fallos)."""
    if poc_version != 2:
        return None
    return build(MemoryMailStore(synthetic_directory(), new_id=services.mail.call, dedup=False))


def effect_duplicates(conn: psycopg.Connection, services: Services) -> dict[str, int]:
    """Efectos repetidos: ejecuciones externas repetidas y reservas de más en `inventory.reservations`."""
    out = service_duplicates(services)
    with conn.transaction():
        row = conn.execute(
            "SELECT count(*) - count(DISTINCT order_id) AS extra FROM inventory.reservations").fetchone()
    out["reserve_stock"] += row["extra"]
    return {k: v for k, v in out.items() if v}


def failing(spec: CommandSpec, producer: str, plan: FaultPlan) -> CommandSpec:
    """`ExecuteStep` cuyos pasos elegidos por `should_fail` fallan sin preparar ni ejecutar el efecto."""

    def chosen(payload: Any) -> bool:
        return should_fail(plan.seed, payload.action, payload.idempotency_key,
                           plan.fail_probability.get(payload.action, 0.0))

    def prepare(command: CommandEnvelope, payload: Any) -> Any:
        if chosen(payload):
            return None
        return spec.prepare(command, payload) if spec.prepare is not None else None

    def handle(conn: psycopg.Connection, command: CommandEnvelope, payload: Any, prepared: Any) -> Any:
        if not chosen(payload):
            return spec.handle(conn, command, payload, prepared)
        return Accepted([build_event("StepFailed", {
            "step_id": payload.step_id, "plan_version": payload.plan_version, "error": INJECTED_FAILURE,
            "retryable": False, "attempts": 1,
        }, correlation_id=command.correlation_id, causation_id=command.command_id, producer=producer)])

    return replace(spec, prepare=prepare, handle=handle)


# (esquema del ejecutor, command_id, eventos de resultado, segundos): los añade a su outbox más tarde.
DeferResult = Callable[[str, uuid.UUID, list[EventEnvelope], float], None]


def delaying(spec: CommandSpec, schema: str, plan: FaultPlan, defer: DeferResult) -> CommandSpec:
    """`ExecuteStep` cuyo resultado llega tarde (spec synthetic-data, «Inyección de entrega retrasada por acción»):
    el efecto se ejecuta a su hora y en su transacción, pero sus eventos (`StepCompleted` o `StepFailed`) no van
    al outbox con ella: `defer` los añade `step_delay` segundos después."""

    def handle(conn: psycopg.Connection, command: CommandEnvelope, payload: Any, prepared: Any) -> Any:
        outcome = spec.handle(conn, command, payload, prepared)
        seconds = step_delay(plan.seed, payload.action, payload.idempotency_key,
                             plan.step_delay_seconds.get(payload.action, 0.0))
        if seconds <= 0 or not isinstance(outcome, Accepted) or not outcome.events:
            return outcome
        defer(schema, command.command_id, list(outcome.events), seconds)
        return replace(outcome, events=[])

    return replace(spec, handle=handle)


def crashing(spec: CommandSpec, crashes: EffectRecordCrashes) -> CommandSpec:
    """`ExecuteStep` que cae entre el efecto y el registro de su clave (spec synthetic-data, «Entrega concurrente de
    comandos»): si `prepare` acaba de llamar al servicio (`external_id`) y es el primer intento de la clave, cae antes
    de la transacción de `handle`. El trabajo se reintenta con el mismo `command_id` y la misma clave."""

    def prepare(command: CommandEnvelope, payload: Any) -> Any:
        prepared = spec.prepare(command, payload) if spec.prepare is not None else None
        if getattr(prepared, "external_id", None) is not None and crashes.should_crash(payload.action,
                                                                                       payload.idempotency_key):
            raise EffectRecordCrash(f"{payload.action} cae tras el efecto de {payload.idempotency_key}")
        return prepared

    return replace(spec, prepare=prepare)


def slowed(spec: CommandSpec, seconds: float) -> CommandSpec:
    """Comando largo cuyo `prepare` tarda `seconds` más (E14: el LLM o el servicio externo, lentos)."""
    if seconds <= 0 or not spec.deferred:
        return spec

    def prepare(command: CommandEnvelope, payload: Any) -> Any:
        time.sleep(seconds)
        return spec.prepare(command, payload) if spec.prepare is not None else None

    return replace(spec, prepare=prepare)


def open_jobs(conn: psycopg.Connection, schema: str, command_id: uuid.UUID) -> tuple[bool, int]:
    """Si el comando ya tiene trabajo en el `command_jobs` de `schema` y cuántos trabajos hay abiertos."""
    with conn.transaction():
        row = conn.execute(
            f"SELECT count(*) FILTER (WHERE command_id = %s) AS mine,"
            f" count(*) FILTER (WHERE status IN ('pending', 'running')) AS open"
            f" FROM {qualified(schema, 'command_jobs')}", (command_id,)).fetchone()
    return row["mine"] > 0, row["open"]


def counting_busy(spec: CommandSpec, count: Callable[[], None]) -> CommandSpec:
    """`ExecuteStep` que anota con `count` cada vez que su trabajo encuentra la clave `in_progress` (E22)."""

    def handle(conn: psycopg.Connection, command: CommandEnvelope, payload: Any, prepared: Any) -> Any:
        try:
            return spec.handle(conn, command, payload, prepared)
        except EffectInProgress:
            count()
            raise

    return replace(spec, handle=handle)


def _try(read: Callable[[], Any]) -> Any:
    """Una lectura del diagnóstico de atasco: su error, si falla, en lugar de su valor."""
    try:
        return read()
    except Exception as exc:  # noqa: BLE001 — diagnóstico: no debe tumbar el experimento
        return {"error": repr(exc)}


class Activity:
    """Trabajo en curso (`busy`) y hecho (`done`) en los hilos del arnés: decide si la ejecución está ociosa."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.busy = 0
        self.done = 0

    @contextlib.contextmanager
    def working(self) -> Iterator[None]:
        with self._lock:
            self.busy += 1
        try:
            yield
        finally:
            with self._lock:
                self.busy -= 1

    def progressed(self) -> None:
        with self._lock:
            self.done += 1


class TrackedConsumer:
    """Consumidor que cuenta como trabajo en curso mientras atiende un mensaje (p. ej. la llamada al LLM)."""

    def __init__(self, consumer: Any, activity: Activity) -> None:
        self._consumer = consumer
        self._activity = activity

    def handle_message(self, msg: Any) -> Any:
        with self._activity.working():
            outcome = self._consumer.handle_message(msg)
        self._activity.progressed()
        return outcome

    def __getattr__(self, name: str) -> Any:
        return getattr(self._consumer, name)


@dataclass(frozen=True)
class Target:
    """Un sistema que atiende comandos: su esquema, sus comandos y si usa su inbox."""

    schema: str
    specs: dict[str, CommandSpec]
    inbox: bool = True


@dataclass
class CommandDelivery:
    """Las entregas de un comando y la respuesta de cada una, `(status_code, body)` (E22)."""

    command_type: str
    command_id: uuid.UUID
    action: str | None
    idempotency_key: str | None
    responses: list[tuple[int, Any]]


def distinct_responses(deliveries: Iterable[CommandDelivery]) -> int:
    """El máximo de respuestas distintas que recibió un mismo `command_id`."""
    seen: dict[uuid.UUID, set[str]] = {}
    for d in deliveries:
        seen.setdefault(d.command_id, set()).update(f"{code}:{json.dumps(body, sort_keys=True, default=str)}"
                                                    for code, body in d.responses)
    return max((len(v) for v in seen.values()), default=0)


def deliver_concurrently(connect: Callable[[], Any], calls: list[Callable[[Any], Any]]) -> list[Any]:
    """Hace las llamadas a la vez, cada una en su hilo y con su conexión (`connect`), y devuelve sus resultados en
    orden. Una barrera las suelta juntas; un error de un hilo se relanza aquí."""
    barrier = threading.Barrier(len(calls))
    results: list[Any] = [None] * len(calls)
    errors: list[BaseException] = []

    def run(i: int) -> None:
        conn = None
        try:
            conn = connect()
            barrier.wait(timeout=JOIN_TIMEOUT_S)
            results[i] = calls[i](conn)
        except BaseException as exc:  # noqa: BLE001 — se relanza en el hilo que llama
            errors.append(exc)
        finally:
            if conn is not None:
                conn.close()

    threads = [threading.Thread(target=run, args=(i,), name=f"concurrent-{i}") for i in range(len(calls))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(JOIN_TIMEOUT_S)
    if errors:
        raise errors[0]
    return results


class InProcessCommands:
    """Sustituye a `command_client.invoke`: ejecuta el comando con el handler de su sistema, sin HTTP.

    `connect` abre la conexión de cada hilo en una entrega concurrente (E22) y al atender lo que un ejecutor
    colgado tenía aceptado (E12). Con `executor_faults` (E12), un sistema `down` rechaza la llamada y uno `hang` la
    acepta sin responder: se espera `timeout` con `sleep` y la petición queda en `backlog` hasta que se recupera.
    `hang_wait_s` suma lo que el que invoca esperó a cada sistema colgado. Con `executor_capacity` (E14), un ejecutor
    lleno responde 503 a un `ExecuteStep` nuevo; `unavailable_503` los cuenta por ejecutor. `jobs` dice si el comando
    ya tiene trabajo y cuántos hay abiertos (por defecto, `open_jobs` con la conexión de las entregas)."""

    def __init__(self, conn: psycopg.Connection, targets: dict[str, Target], plan: FaultPlan,
                 crashed: set[str], *, connect: Callable[[], Any] | None = None,
                 clock: Callable[[], float] = time.monotonic, sleep: Callable[[float], Any] = time.sleep,
                 jobs: Callable[[str, uuid.UUID], tuple[bool, int]] | None = None) -> None:
        self._conn = conn
        self._jobs = jobs or (lambda schema, command_id: open_jobs(self._conn, schema, command_id))
        self._targets = targets
        self._plan = plan
        self._crashed = crashed
        self._connect = connect
        self._sleep = sleep
        self._outage = ExecutorOutage(plan, clock=clock)
        self._handled: Counter[str] = Counter()
        self._lock = threading.Lock()
        self.crashes = 0
        self.deliveries: list[CommandDelivery] = []
        self.backlog: dict[str, list[CommandEnvelope]] = {}
        self.hang_wait_s: Counter[str] = Counter()
        self.unavailable_503: Counter[str] = Counter()

    def deliver(self, target: Target, command: CommandEnvelope, *, conn: psycopg.Connection | None = None,
                entry: bool = False) -> Any:
        """Entrega el comando a su sistema con los fallos del plan y devuelve la respuesta a `command`: `duplicates`
        veces seguidas, o desde `concurrent_commands` hilos a la vez; con `reinvoke_steps`, también a la vez con
        otro `command_id`. Devuelve la respuesta de la última entrega de `command`. Anota cada entrega en
        `deliveries`.

        `entry`: comando de entrada del arnés (datos de ejemplo, avisos): siempre con inbox y sin `duplicates`, como
        antes de E22; solo le afecta `concurrent_commands`."""
        inbox_enabled = True if entry else target.inbox
        spec = target.specs[command.command_type]
        payload = command.payload
        commands = [command]
        if command.command_type == "ExecuteStep" and payload.get("action") in self._plan.reinvoke_steps:
            commands.append(command.model_copy(update={"command_id": uuid.uuid4()}))
        threads = self._plan.concurrent_commands.get(command.command_type, 1)

        def run(c: CommandEnvelope) -> Callable[[Any], Any]:
            return lambda conn_: execute_command(conn_, target.schema, c, spec, inbox_enabled=inbox_enabled)

        if len(commands) == 1 and threads == 1:
            sent = [command] * (1 if entry else self._plan.duplicates)  # al menos una vez: el mismo command_id
            responses = [run(command)(conn or self._conn) for _ in sent]
        else:
            sent = [c for c in commands for _ in range(threads)]
            responses = deliver_concurrently(self._connect, [run(c) for c in sent])
        with self._lock:
            for c in commands:
                self.deliveries.append(CommandDelivery(
                    c.command_type, c.command_id, payload.get("action"), payload.get("idempotency_key"),
                    [(r.status_code, r.body) for s, r in zip(sent, responses, strict=True) if s is c]))
        # Como antes de E22, la respuesta de la última entrega de `command` (sin inbox puede ser un rechazo).
        return responses[len(sent) - 1 - sent[::-1].index(command)]

    def __call__(self, url: str, command_type: str, *, command_id: uuid.UUID, correlation_id: str,
                 causation_id: uuid.UUID | None, payload: dict[str, Any], timeout: float) -> CommandReply:
        command = CommandEnvelope(command_id=command_id, command_type=command_type, correlation_id=correlation_id,
                                  causation_id=causation_id, payload=payload)
        target = self._targets[url]
        self._suffer_outage(target, command, timeout)
        self._refuse_when_full(target, command)
        response = self.deliver(target, command)
        if command_type == "ExecuteStep" and response.status_code == 202:
            self._crash_after_commit(f"executor-{payload['action']}")
        return CommandReply(response.status_code, response.body, response.replayed)

    def _suffer_outage(self, target: Target, command: CommandEnvelope, timeout: float) -> None:
        """E12: el sistema destino está caído (falla al momento) o colgado (acepta la petición, la guarda y no
        responde en `timeout`)."""
        fault = self._outage.active(target.schema, command.payload.get("action"))
        if fault is None:
            return
        if fault.mode == "hang":
            with self._lock:
                kept = self.backlog.setdefault(target.schema, [])
                if all(c.command_id != command.command_id for c in kept):
                    kept.append(command)
                self.hang_wait_s[target.schema] += timeout
            self._sleep(timeout)
        raise CommandUnavailable(f"{target.schema} {fault.mode}")

    def _refuse_when_full(self, target: Target, command: CommandEnvelope) -> None:
        """E14: un ejecutor con tantos trabajos abiertos como su capacidad responde 503 a un `ExecuteStep` nuevo, sin
        crear el trabajo. Una reentrega (su trabajo ya existe) pasa: el command handler la trata como repetida."""
        capacity = self._plan.executor_capacity.get(target.schema)
        if capacity is None or command.command_type != "ExecuteStep":
            return
        known, open_ = self._jobs(target.schema, command.command_id)
        if known or open_ < capacity:
            return
        with self._lock:
            self.unavailable_503[target.schema] += 1
        raise CommandUnavailable(f"{target.schema}: HTTP 503 ({open_} trabajos abiertos, capacidad {capacity})")

    def awaiting_recovery(self) -> bool:
        """Algún ejecutor colgado tiene peticiones aceptadas y se recuperará: el arnés debe seguir hasta atenderlas."""
        with self._lock:
            pending = [schema for schema, kept in self.backlog.items() if kept]
        return any(self._outage.recovering(schema) or self._outage.ended(schema) for schema in pending)

    def flush_recovered(self, classify: Callable[[CommandEnvelope], str]) -> dict[str, int]:
        """Un ejecutor colgado que se recupera atiende una vez lo que tenía aceptado. `classify` dice, antes de
        atenderlo, si el resultado llegará obsoleto o a un paso vigente; devuelve el recuento."""
        with self._lock:
            ready = {schema: kept for schema, kept in self.backlog.items() if kept and self._outage.ended(schema)}
            for schema in ready:
                self.backlog[schema] = []
        counts: Counter[str] = Counter()
        targets = {t.schema: t for t in self._targets.values()}
        for schema, kept in ready.items():
            for command in kept:
                counts[classify(command)] += 1
                conn = self._connect()
                try:
                    target = targets[schema]
                    execute_command(conn, schema, command, target.specs[command.command_type],
                                    inbox_enabled=target.inbox)
                finally:
                    if conn is not None:
                        conn.close()
        return dict(counts)

    def _crash_after_commit(self, name: str) -> None:
        """E2: el ejecutor cae con la transacción confirmada y sin responder; la saga lo reinvoca."""
        limit = self._plan.crash_after.get(name)
        if limit is None or name in self._crashed:
            return
        self._handled[name] += 1
        if self._handled[name] >= limit:
            self._crashed.add(name)
            self.crashes += 1
            raise CommandUnavailable(f"{name} cae tras {self._handled[name]} pasos")


@dataclass(frozen=True)
class RunConfig:
    seed: int
    incidents: int
    faults: FaultPlan = field(default_factory=FaultPlan)
    idempotency: str = "full"  # full, no_effect_keys o none (supuesto A3)
    fmt: str = "text"  # formato de los avisos: text o api
    timeout_seconds: float = 300.0
    idle_seconds: float = 15.0  # sin ningún avance durante este tiempo, se da por terminada
    plan_timeout_seconds: float | None = None  # plazo de CreatePlan y RevisePlan; None: el de la configuración
    step_timeout_seconds: float | None = None  # plazo de ExecuteStep; None: el de la configuración
    pending_notice_timeout_seconds: float = PENDING_NOTICE_TIMEOUT_S  # plazo de un aviso aparcado
    seed_orders: bool = True  # False: los pedidos se siembran después, con `Harness.seed_orders` (E17)
    track_timeouts: bool = False  # anota los `ExecuteStep` cuyo plazo vence (`Harness.timed_out`, E11)
    job_workers: int = 1  # workers de trabajos por componente (E22: dos trabajos con la misma clave a la vez)
    command_timeout_seconds: float | None = None  # COMMAND_HTTP_TIMEOUT_S; None: el de la configuración (E12)
    dispatcher_thread: bool = False  # el dispatcher en su propio hilo, como en el orquestador (E12)
    # Modo multiproceso (E16): instancias de `eda-orchestrator` en procesos propios en lugar del orquestador en
    # proceso; los demás componentes siguen aquí y les atienden los comandos por REST (`processes.CommandGateway`).
    orchestrator_processes: int = 0
    session_timeout_ms: int | None = None  # KAFKA_SESSION_TIMEOUT_MS de esas instancias; None: el de librdkafka
    relay_flush_timeout_seconds: float = 30.0  # plazo de entrega de un lote del relay en cada turno (E9: corto)
    tolerate_relay_errors: bool = False  # E9: un lote sin confirmar se cuenta y el turno sigue, como `run_until`
    # E9: `session.timeout.ms` de los consumidores en proceso (intérprete y orquestador). Sin broker, un commit síncrono
    # espera un coordinador hasta este plazo, y un consumidor recreado espera a que caduque el miembro anterior.
    consumer_session_timeout_ms: int | None = None
    # Todos los componentes en procesos propios, los de `scripts/run-agents.sh` (E10): el harness solo siembra los
    # datos, envía los avisos por REST y mide. Ver `check_component_mode` para lo que no admite.
    component_processes: bool = False

    @property
    def external_orchestrator(self) -> bool:
        return self.orchestrator_processes > 0


def check_component_mode(config: RunConfig, poc_version: int, *, custom: bool) -> None:
    """ValueError si la ejecución pide, con `component_processes`, algo que solo hace el harness en proceso.
    `custom`: se pasó un `llm`, unos `data` o un `carrier` propios (los procesos usan el oráculo de `generate`)."""
    if not config.component_processes:
        return
    mode = "el modo con todos los componentes en procesos propios"
    if config.orchestrator_processes:
        raise ValueError(f"{mode} no se combina con orchestrator_processes")
    if config.idempotency != "full":
        raise ValueError(f"{mode} solo admite idempotency='full'")
    if config.faults != FaultPlan():
        raise ValueError(f"{mode} no admite los fallos del FaultPlan: los inyecta el harness en proceso")
    if poc_version != 1:
        raise ValueError(f"{mode} solo admite POC_VERSION=1")
    if custom:
        raise ValueError(f"{mode} usa el oráculo sobre generate(seed, incidents): no admite llm, data ni carrier "
                         "propios")


def saga_deadlines(config: RunConfig) -> dict[str, float]:
    """Los plazos de la saga que la ejecución cambia, listos para `replace` sobre `Settings`."""
    out = {}
    if config.plan_timeout_seconds is not None:
        out["saga_plan_timeout_s"] = config.plan_timeout_seconds
    if config.step_timeout_seconds is not None:
        out["saga_step_timeout_s"] = config.step_timeout_seconds
    if config.command_timeout_seconds is not None:
        out["command_http_timeout_s"] = config.command_timeout_seconds
    return out


@dataclass
class RunResult:
    config: RunConfig
    model: str
    closed: int
    open: int
    resolved: int
    escalated: int
    duplicates: dict[str, int]
    consumer_crashes: int
    poison_messages: int
    latencies_ms: list[float]
    llm_calls: int
    tokens_in: int
    tokens_out: int
    elapsed_seconds: float
    escalation_reasons: dict[str, int] = field(default_factory=dict)

    @property
    def repeated_steps(self) -> int:
        return sum(self.duplicates.values())


def seed_payload(data: Dataset, empty: tuple[str, ...] = ()) -> dict[str, Any]:
    """Pedidos y existencias del conjunto para `SeedSampleData`; los almacenes vacíos, con 0."""
    return {
        "orders": [{"order_id": o.order_id, "customer_ref": o.customer_ref, "sku": o.sku, "quantity": o.quantity,
                    "amount": str(Decimal(o.amount)), "carrier": o.carrier} for o in data.orders],
        "stock": [{"warehouse": s.warehouse, "sku": s.sku, "available": 0 if s.warehouse in empty else s.available}
                  for s in data.stock],
    }


class Harness:
    """Una ejecución aislada. Usar como contexto: crea y borra su base de datos y su topic."""

    def __init__(self, base: Settings, config: RunConfig, *, llm: LlmClient | None = None,
                 data: Dataset | None = None, carrier: CarrierConsultant | None = None,
                 mail_server: Callable[[MailStore], MCPServer] = build_mail_server) -> None:
        """`carrier`: un cliente A2A en proceso para el planificador. Sin él (por defecto) el planificador no
        consulta al transportista, como en la versión 1, aunque POC_VERSION=2: E5 y E6 siguen comparables.
        `mail_server`: construye el servidor de correo de la versión 2 sobre su buzón (E20, uno con fallos)."""
        self.config = config
        self._carrier = carrier
        self._mail_server = mail_server
        self._custom = llm is not None or data is not None or carrier is not None
        self.data = data or generate(config.seed, config.incidents)
        self._base = base
        self._llm = llm
        self.rules = BusinessRules()
        self.name = f"exp_{uuid.uuid4().hex[:10]}"
        self.settings: Settings | None = None
        self.conn: psycopg.Connection | None = None
        self.services = Services()
        self._deliveries: list[tuple[FaultyDelivery, Callable[[], EventConsumer]]] = []
        self._crashed: set[str] = set()
        self._failures: Counter[tuple[str, int, int]] = Counter()
        self._closables: list[Any] = []
        self.consumer_crashes = 0
        self.poison_messages = 0
        self.relay_errors: Counter[str] = Counter()  # lotes sin confirmar por componente (E9)
        self.commit_failures = 0  # caídas de un consumidor por un commit de offset fallido (E9)
        self.relay_max_seconds = 0.0  # lo que más tardó un relay en un turno con `tolerate_relay_errors` (E9)
        self.outcomes: Counter[tuple[str, str]] = Counter()  # (consumidor, resultado) de cada mensaje
        self._inbox_off: frozenset[str] = frozenset()  # consumidores sin inbox (control de E21)
        self.effect_crashes = EffectRecordCrashes(config.faults)  # caídas entre efecto y registro (E22)
        self.busy_claims = 0  # trabajos que encontraron su clave `in_progress` (E22)
        self.processes: list[OrchestratorProcess] = []  # modo multiproceso (E16)
        self._gateway: CommandGateway | None = None
        self._process_dir: tempfile.TemporaryDirectory | None = None
        # Todos los componentes en procesos propios (E10): los procesos, sus URL y sus códigos al pararlos.
        self.components: list[ComponentProcess] = []
        self.component_urls: dict[str, str] = {}
        self.exit_codes: dict[str, int | None] = {}
        self._observed_events = 0
        self._init_threads()

    def _init_threads(self) -> None:
        self._activity = Activity()
        self._stop = threading.Event()
        self._crash_lock = threading.Lock()
        self._projectors_paused = False
        self.delayed_results = 0  # resultados de pasos retrasados (E11)
        self.timed_out: list[dict[str, Any]] = []  # pasos cuyo plazo venció con el paso pedido (E11)
        self.recovered: Counter[str] = Counter()  # resultados de un ejecutor colgado que se recupera (E12)
        self._threads: list[threading.Thread] = []
        self._stuck: set[str] = set()  # hilos que siguen vivos tras la espera de `_stop_threads`
        self.stall: dict[str, Any] | None = None  # diagnóstico de la ejecución que llegó a `timeout_seconds`
        self._workers: list[JobWorker] = []
        self._threaded = set(THREADED_CONSUMERS)
        self._seen_done = 0

    # -- ciclo de vida ---------------------------------------------------------------------
    def __enter__(self) -> Self:
        if self.config.idempotency not in IDEMPOTENCY_MODES:
            raise ValueError(f"modo de idempotencia desconocido: {self.config.idempotency}")
        check_component_mode(self.config, self._base.poc_version, custom=self._custom)
        self._database = isolated_database(self._base, self.name)
        url = self._database.__enter__()
        try:
            self._enter(url)
        except BaseException:
            self.__exit__(None, None, None)  # sin procesos, base de datos ni topic que queden atrás
            raise
        return self

    def _enter(self, url: str) -> None:
        topic = f"eda.{self.name}"
        self._admin = AdminClient({"bootstrap.servers": self._base.kafka_bootstrap_servers})
        for fut in self._admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1)
                                              for t in (topic, f"{topic}.dlq")]).values():
            fut.result(timeout=30)
        self.settings = replace(self._base, database_url=url, events_topic=topic, dlq_topic=f"{topic}.dlq",
                                inbox_enabled=self.config.idempotency != "none",
                                step_backoff_base_seconds=0.01, step_backoff_max_seconds=0.05,
                                pending_notice_timeout_s=self.config.pending_notice_timeout_seconds,
                                **saga_deadlines(self.config))
        self.conn = connect(url)
        if self.config.component_processes:
            self._start_components()
        else:
            self._set_idempotency(self.config.idempotency)
            self._build()
            self._start_threads()
            if self.config.external_orchestrator:
                self._start_orchestrators()
        if self.config.seed_orders:
            self.seed_orders()

    def __exit__(self, *exc: object) -> None:
        with contextlib.suppress(Exception):
            self.stop_components()
        self._stop_orchestrators()
        self._stop_threads()
        self._close_deliveries()
        for closable in self._closables:
            with contextlib.suppress(Exception):
                closable.close()
        with contextlib.suppress(Exception):
            self.conn.close()
        self._database.__exit__(None, None, None)
        with contextlib.suppress(Exception):
            topic = f"eda.{self.name}"
            for fut in self._admin.delete_topics([topic, f"{topic}.dlq"]).values():
                fut.result(timeout=30)

    # -- modo multiproceso (spec experiments, «Modo multiproceso del harness»; E16) ------------------
    def _start_orchestrators(self) -> None:
        """El servidor de comandos con los receptores del harness y las instancias del orquestador, que lo invocan."""
        targets = {t: (v.schema, v.specs, v.inbox) for t, v in self.targets.items()}
        self._gateway = CommandGateway(targets, lambda: connect(self.settings.database_url), self._tracked_call).start()
        self._process_dir = tempfile.TemporaryDirectory(prefix=f"{self.name}-")
        start_orchestrators(self.config.orchestrator_processes, self.settings, self._gateway.urls(),
                            Path(self._process_dir.name), session_timeout_ms=self.config.session_timeout_ms,
                            processes=self.processes)

    def _tracked_call(self, call: Callable[[], Any]) -> Any:
        with self._activity.working():
            out = call()
        self._activity.progressed()
        return out

    def _stop_orchestrators(self) -> None:
        """Para las instancias (también las congeladas) antes que nada: siguen escribiendo en la base de datos."""
        for process in self.processes:
            with contextlib.suppress(Exception):
                process.stop()
        if self._gateway is not None:
            with contextlib.suppress(Exception):
                self._gateway.stop()
        if self._process_dir is not None:
            with contextlib.suppress(Exception):
                self._process_dir.cleanup()

    # -- todos los componentes en procesos propios (spec experiments; E10) ------------------------------
    def _start_components(self) -> None:
        """Lanza los procesos de `scripts/run-agents.sh`, cada uno con su puerto y su log, y espera a que estén
        listos. Los añade a `components` según arrancan: `__exit__` los para aunque falle uno."""
        targets = [c.target for c in COMPONENTS if c.target]
        self.component_urls = component_urls(dict(zip(targets, free_ports(len(targets)), strict=True)))
        env = component_env(self.settings, self.component_urls, extra={
            "LLM_CLIENT": "oracle", "ORACLE_SEED": str(self.config.seed),
            "ORACLE_INCIDENTS": str(self.config.incidents),
            "PENDING_NOTICE_TIMEOUT_S": str(self.settings.pending_notice_timeout_s),
            "EFFECT_KEYS_ENABLED": "true" if self.settings.effect_keys_enabled else "false",
        })
        self._process_dir = tempfile.TemporaryDirectory(prefix=f"{self.name}-")
        start_components(COMPONENTS, env, self.component_urls, Path(self._process_dir.name),
                         processes=self.components)
        wait_ready(self.components)

    def stop_components(self) -> dict[str, int | None]:
        """Para todos los procesos a la vez (una sola vez) y devuelve el código de salida de cada uno: 0 si se paró
        limpio. Con el sistema parado se pueden comprobar las invariantes (I5 reconstruye las vistas)."""
        if self.components and not self.exit_codes:
            self.exit_codes = stop_all(self.components)
        return self.exit_codes

    def process_deaths(self) -> dict[str, int | None]:
        """Los procesos que terminaron sin que el harness los parase, con su código de salida."""
        return {p.name: p.returncode for p in self.components if p.died()}

    def service_executions(self) -> Counter[tuple[str, str]]:
        """Ejecuciones de los servicios simulados de los ejecutores por (servicio, `idempotency_key`)."""
        return read_executions(p.executions_path for p in self.components if p.executions_path is not None)

    def _observe_components(self) -> int:
        """Un turno con los componentes en procesos: las filas nuevas de `event_log` desde el anterior."""
        row = self.conn.execute("SELECT count(*) AS n FROM event_log.events").fetchone()
        self.conn.commit()
        progressed, self._observed_events = row["n"] - self._observed_events, row["n"]
        if not progressed:
            time.sleep(PROCESS_POLL_S)
        return progressed

    def _catch_up_processes(self, timeout: float) -> bool:
        """El proceso `event-log` llega a las marcas de agua altas de ahora; False si no llega en `timeout`."""
        topic = self.settings.events_topic
        highs = self._process_highs()
        end = time.monotonic() + timeout
        while True:
            rows = self.conn.execute("SELECT kafka_partition, kafka_offset FROM event_log.offsets"
                                     " WHERE consumer = %s AND kafka_topic = %s",
                                     (event_log.CONSUMER, topic)).fetchall()
            self.conn.commit()
            if caught_up({r["kafka_partition"]: r["kafka_offset"] for r in rows}, highs):
                return True
            if time.monotonic() > end:
                log.warning("event_log no llegó a %s en %s s", highs, timeout)
                return False
            time.sleep(PROCESS_POLL_S)

    def _process_highs(self) -> dict[int, int]:
        """Marcas de agua altas del topic de la ejecución, con un consumidor de sondeo propio (ningún otro hilo lo usa)."""
        topic = self.settings.events_topic
        probe = make_consumer(self.settings.kafka_bootstrap_servers, group_id=f"{topic}.probe")
        try:
            parts = probe.list_topics(topic, timeout=10).topics[topic].partitions
            return {p: high_watermark(probe, TopicPartition(topic, p)) for p in parts}
        finally:
            probe.close()

    def _process_lags(self) -> dict[str, int]:
        """`lags` con los componentes en procesos: los grupos de Kafka de los procesos (su offset confirmado) y la
        posición guardada de `event_log` y de cada proyector en `event_log.offsets` y `views.offsets`."""
        highs = self._process_highs()
        topic = self.settings.events_topic
        lags = {c.group: group_lag(self._admin, c.group, topic, highs) for c in COMPONENTS if c.group}
        rows = self.conn.execute(
            "SELECT consumer, kafka_partition, kafka_offset FROM event_log.offsets WHERE kafka_topic = %s"
            " UNION ALL SELECT consumer, kafka_partition, kafka_offset FROM views.offsets WHERE kafka_topic = %s",
            (topic, topic)).fetchall()
        self.conn.commit()
        return lags | positioned_lags(rows, highs, [event_log.CONSUMER, *(p.name for p in build_all())])

    def reconnect(self, timeout: float = RECONNECT_TIMEOUT_S) -> bool:
        """La conexión del harness tras una caída de PostgreSQL (E10): si `SELECT 1` falla, la cierra y abre otra,
        reintentándolo cada `RECONNECT_RETRY_S` mientras la base no responda y hasta `timeout` (después, el error de
        conexión sube). True si tuvo que abrir otra."""
        try:
            self.conn.execute("SELECT 1").fetchone()
            self.conn.commit()
            return False
        except CONNECTION_ERRORS:
            with contextlib.suppress(Exception):
                self.conn.close()
        end = time.monotonic() + timeout
        while True:
            try:
                self.conn = connect(self.settings.database_url)
                self.conn.execute("SELECT 1").fetchone()
                self.conn.commit()
                return True
            except CONNECTION_ERRORS:
                if time.monotonic() >= end:
                    raise
                time.sleep(RECONNECT_RETRY_S)

    def _open_process_jobs(self) -> int:
        """Trabajos pendientes o en curso en el `command_jobs` de cada esquema que lo tenga."""
        rows = self.conn.execute("SELECT table_schema AS s FROM information_schema.tables"
                                 " WHERE table_name = 'command_jobs'").fetchall()
        self.conn.commit()
        if not rows:
            return 0
        counts = self.conn.execute(" UNION ALL ".join(
            f"SELECT count(*) AS n FROM {qualified(r['s'], 'command_jobs')} WHERE status IN ('pending', 'running')"
            for r in rows)).fetchall()
        self.conn.commit()
        return sum(r["n"] for r in counts)

    def _set_idempotency(self, mode: str) -> None:
        """Restricciones de unicidad de negocio y deduplicación de los servicios simulados según el modo."""
        if mode != "full":
            drop_business_uniques(self.conn)
        for service in (self.services.mail, self.services.claims, self.services.carrier, self.services.payments):
            service.idempotent = mode == "full"

    @property
    def llm(self) -> LlmClient:
        if self._llm is None:
            self._llm = Oracle(self.data).client()
        return self._llm

    def _connect(self) -> psycopg.Connection:
        conn = connect(self.settings.database_url)
        self._closables.append(conn)
        return conn

    def _targets(self, mode: str | None = None) -> dict[str, Target]:
        s, plan, mode = self.settings, self.config.faults, mode or self.config.idempotency
        effects = replace(s, effect_keys_enabled=mode == "full")  # ejecutores e inventory
        targets = {
            "intake": Target(intake.SCHEMA, intake.specs(), mode != "none"),
            "planner": Target(planner.SCHEMA, self._slowed(planner.SCHEMA, planner.specs(
                s, self.llm, self.rules, carrier=self._carrier)), mode != "none"),
            "replanner": Target(replanner.SCHEMA, self._slowed(replanner.SCHEMA,
                                                               replanner.specs(s, self.llm, self.rules)), mode != "none"),
            "inventory": Target(inventory.SCHEMA, dict(inventory.specs(effects), ExecuteStep=delaying(failing(
                counting_busy(inventory.specs(effects)["ExecuteStep"], self._count_busy), inventory.SCHEMA, plan),
                inventory.SCHEMA, plan, self._defer_result)), mode == "full"),
        }
        mail_server = mail_server_for(self.services, s.poc_version, self._mail_server)
        for action in EXECUTOR_ACTIONS:
            spec = step_executors.specs(effects, action, self.llm, self.rules,
                                        service=getattr(self.services, SERVICE_OF[action]),
                                        mail_server=mail_server)["ExecuteStep"]
            spec = counting_busy(crashing(spec, self.effect_crashes), self._count_busy)
            spec = slowed(spec, plan.prepare_delay_seconds.get(f"executor_{action}", 0.0))
            targets[f"executor_{action}"] = Target(f"executor_{action}",
                                                   {"ExecuteStep": delaying(failing(spec, f"executor-{action}", plan),
                                                                            f"executor_{action}", plan,
                                                                            self._defer_result)},
                                                   mode == "full")
        return targets

    def _slowed(self, schema: str, specs: dict[str, CommandSpec]) -> dict[str, CommandSpec]:
        seconds = self.config.faults.prepare_delay_seconds.get(schema, 0.0)
        return {name: slowed(spec, seconds) for name, spec in specs.items()}

    def _record_timeouts(self) -> None:
        """Los `ExecuteStep` que `expire_once` va a tratar como `timeout` en este turno: plazo vencido y sin
        resolver con el paso aún `requested`. El orquestador y los plazos van en el mismo hilo, así que nada los
        resuelve entre esta consulta y `expire_once`. El plazo no deja ningún evento en el log."""
        with self._timer_conn.transaction():
            rows = self._timer_conn.execute(
                """
                SELECT d.incident_id, d.step_id, (c.payload ->> 'plan_version')::int AS plan_version
                FROM orchestrator.deadlines d
                JOIN orchestrator.command_outbox c USING (command_id)
                JOIN orchestrator.plan_steps s ON s.incident_id = d.incident_id AND s.step_id = d.step_id
                WHERE d.kind = 'execute_step' AND d.resolved_at IS NULL AND d.expires_at <= now()
                  AND s.status = 'requested'
                """
            ).fetchall()
        self.timed_out += [dict(r) for r in rows]

    def _count_busy(self) -> None:
        with self._crash_lock:
            self.busy_claims += 1

    def _defer_result(self, schema: str, command_id: uuid.UUID, events: list[EventEnvelope], seconds: float) -> None:
        """Añade al outbox de `schema` los eventos de resultado de un paso `seconds` segundos después, cuando la
        transacción del comando ya está confirmada (su fila de inbox existe). Cuenta como trabajo en curso."""
        url = self.settings.database_url
        with self._crash_lock:
            self.delayed_results += 1

        def run() -> None:
            with self._activity.working():
                time.sleep(seconds)
                conn = connect(url)
                try:
                    end = time.monotonic() + JOIN_TIMEOUT_S
                    while time.monotonic() < end:
                        with conn.transaction():
                            if conn.execute(f"SELECT 1 FROM {qualified(schema, 'inbox')} WHERE message_id = %s",
                                            (command_id,)).fetchone() is not None:
                                outbox.append(conn, schema, events)
                                break
                        time.sleep(IDLE_SLEEP_S)
                    else:
                        log.warning("resultado retrasado de %s descartado: el comando %s no se confirmó",
                                    schema, command_id)
                finally:
                    conn.close()
            self._activity.progressed()

        threading.Thread(target=run, daemon=True, name=f"harness-delay-{schema}").start()

    def _build(self) -> None:
        s = self.settings
        self.targets = self._targets()
        self.commands = InProcessCommands(self._connect(), {target_url(s, t): v for t, v in self.targets.items()},
                                          self.config.faults, self._crashed,
                                          connect=lambda: connect(s.database_url),
                                          sleep=lambda seconds: self._stop.wait(seconds))
        self._seed_conn = self._connect()
        self._dispatcher_conn = self._connect()
        self._timer_conn = self._connect()
        self._dispatch_backoff = Backoff(0.01, 0.05, seed=self.config.seed)
        # En modo multiproceso, cada instancia del orquestador publica su outbox con su relay, como en producción.
        self._relays = [ComponentRelay.from_settings(s, schema) for schema in PUBLISHERS
                        if not (schema == orchestrator.SCHEMA and self.config.external_orchestrator)]
        # El lado de lectura no tiene outbox: su relay solo publica sus dead letters en la DLQ.
        self._relays += [ComponentRelay.from_settings(s, schema, outbox=False) for schema in READ_SIDE]
        self._projectors = build_all()
        self._readers = [event_log.build(s), *(consumer_for(p, s, self._connect()) for p in self._projectors)]
        self._closables += [*self._relays, *self._readers]
        self._build_deliveries()

    def _factories(self) -> dict[str, Callable[[], EventConsumer]]:
        """Consumidores de Kafka por `group.id`; los de `_inbox_off`, sin inbox."""
        s = self.settings

        def settings_for(group: str) -> Settings:
            return replace(s, inbox_enabled=False) if group in self._inbox_off else s

        factories: dict[str, Callable[[], EventConsumer]] = {
            interpreter.SCHEMA: lambda: interpreter.build_consumer(
                self._connect(), make_consumer(s.kafka_bootstrap_servers, interpreter.SCHEMA,
                                               session_timeout_ms=self.config.consumer_session_timeout_ms),
                settings_for(interpreter.SCHEMA), self.llm, self.rules),
        }
        if not self.config.external_orchestrator:  # en modo multiproceso, el grupo es de las instancias
            factories[orchestrator.SCHEMA] = lambda: orchestrator.build_consumer(
                self._connect(), make_consumer(s.kafka_bootstrap_servers, orchestrator.SCHEMA,
                                               session_timeout_ms=self.config.consumer_session_timeout_ms),
                settings_for(orchestrator.SCHEMA), self.rules)
        return factories

    def _build_deliveries(self) -> None:
        self._deliveries = []
        for name, factory in self._factories().items():
            if name in self._threaded:
                factory = self._tracked_factory(factory)
            delivery = FaultyDelivery(factory(), self.config.faults, crashed=self._crashed)
            self._deliveries.append((delivery, factory))

    def _close_deliveries(self) -> None:
        """Cierra los consumidores. Uno cuyo hilo sigue vivo se queda abierto: cerrarlo desde este hilo mientras el
        suyo está dentro de librdkafka se bloquea sin límite (lo vio el CI de E9); el hilo es daemon."""
        for delivery, _ in self._deliveries:
            if f"harness-{delivery.consumer.name}" in self._stuck:
                continue
            with contextlib.suppress(Exception):
                delivery.consumer.close()

    # -- hilos -------------------------------------------------------------------------------
    def _tracked(self, consumer: Any) -> TrackedConsumer:
        return TrackedConsumer(consumer, self._activity)

    def _tracked_factory(self, make: Callable[[], EventConsumer]) -> Callable[[], Any]:
        return lambda: self._tracked(make())

    def _tracked_job(self, *args: Any, **kwargs: Any) -> bool:
        with self._activity.working():
            did = run_job_once(*args, **kwargs)
        if did:
            self._activity.progressed()
        return did

    def _start_threads(self) -> None:
        """Un worker de trabajos por componente con comandos largos y un hilo por consumidor de `_threaded`."""
        s = self.settings
        for target in self.targets.values():
            if not any(spec.deferred for spec in target.specs.values()):
                continue
            for k in range(self.config.job_workers):
                worker = JobWorker(lambda: connect(s.database_url), target.schema, target.specs,
                                   lease_seconds=s.job_lease_seconds, poll_interval=JOB_POLL_S,
                                   max_attempts=s.job_max_attempts, backoff=Backoff(0.01, 0.05, seed=self.config.seed),
                                   run_once=self._tracked_job, name=f"harness-jobs-{target.schema}-{k}")
                worker.start()
                self._workers.append(worker)
        if self.config.dispatcher_thread and not self.config.external_orchestrator:
            thread = threading.Thread(target=self._dispatch, daemon=True, name="harness-dispatcher")
            thread.start()
            self._threads.append(thread)
        for i, (delivery, _) in enumerate(self._deliveries):
            if delivery.consumer.name in self._threaded:
                thread = threading.Thread(target=self._consume, args=(i,), daemon=True,
                                          name=f"harness-{delivery.consumer.name}")
                thread.start()
                self._threads.append(thread)

    def _consume(self, i: int) -> None:
        """Bucle de un consumidor en su hilo, con las mismas caídas y mensajes veneno que en los turnos."""
        delivery, factory = self._deliveries[i]
        while not self._stop.is_set():
            try:
                self._record(delivery, delivery.run_once(CONSUMER_POLL_S))
            except Exception as exc:  # noqa: BLE001 — una caída de un proceso, a propósito o no
                self._on_crash(i, delivery, factory, exc)

    def _stop_threads(self) -> None:
        self._stop.set()
        for worker in self._workers:
            worker.stop(JOIN_TIMEOUT_S)
        for thread in self._threads:
            thread.join(JOIN_TIMEOUT_S)
        self._stuck = {thread.name for thread in self._threads if thread.is_alive()}
        if self._stuck:
            log.warning("hilos que no terminan al parar: %s", ", ".join(sorted(self._stuck)))
        self._workers, self._threads = [], []

    def _command(self, target: str, command_type: str, correlation_id: str, payload: dict[str, Any], *,
                 check: bool = True, command_id: uuid.UUID | None = None) -> Any:
        """Comando de entrada (datos de ejemplo, avisos): directo a su handler, con las entregas del plan
        (`duplicates`, `concurrent_commands`) y sin más fallos inyectados. Con `check`, un rechazo es un error; sin
        él, devuelve la respuesta. `command_id`: el de un reintento (E10); por defecto, uno nuevo."""
        command = CommandEnvelope(command_id=command_id or uuid.uuid4(), command_type=command_type,
                                  correlation_id=correlation_id, causation_id=None, payload=payload)
        if self.config.component_processes:  # por REST al proceso del destino; un 5xx o sin respuesta, excepción
            response = invoke(self.component_urls[target], command_type, command_id=command.command_id,
                              correlation_id=correlation_id, causation_id=None, payload=payload,
                              timeout=self.settings.command_http_timeout_s)
        else:
            response = self.commands.deliver(self.targets[target], command, conn=self._seed_conn, entry=True)
        if check and response.status_code != 202:
            raise RuntimeError(f"{command_type} rechazado: {response.body}")
        return response

    def seed_orders(self, *, wait: bool = True) -> None:
        """Siembra pedidos y existencias. Con `wait`, espera a que `view_orders` los tenga (como un despliegue ya
        en marcha); sin él, solo publica sus `OrderRegistered` (E17, con los proyectores parados)."""
        self._command("inventory", "SeedSampleData", inventory.SAMPLE_DATA_CORRELATION,
                      seed_payload(self.data, tuple(self.config.faults.empty_warehouses)))
        deadline = time.monotonic() + self.config.idle_seconds
        while wait and self.projected_orders() < len(self.data.orders) and time.monotonic() < deadline:
            self.step()

    # -- parada de proyectores (spec synthetic-data, «Parada de proyectores»; E17) ---------------
    def pause_projectors(self) -> None:
        """Los proyectores dejan de consumir; `event_log` y el resto de componentes siguen."""
        self._projectors_paused = True

    def resume_projectors(self) -> None:
        self._projectors_paused = False

    def projected_orders(self) -> int:
        """Pedidos de la ejecución que ya están en `view_orders`."""
        row = self.conn.execute("SELECT count(*) AS n FROM views.view_orders WHERE order_id = ANY(%s)",
                                ([o.order_id for o in self.data.orders],)).fetchone()
        self.conn.commit()
        return row["n"]

    # -- ejecución ---------------------------------------------------------------------------
    def send_notices(self, indices: Iterable[int] | None = None) -> None:
        """Envía los avisos del conjunto (los de `indices`, si se dan)."""
        notices = self.data.notices if indices is None else [self.data.notices[i] for i in indices]
        for n in notices:
            self._send_notice(n)

    def _send_notice(self, n: Any, *, check: bool = True, command_id: uuid.UUID | None = None) -> Any:
        return self._command("intake", "RegisterCarrierNotice", incident_id_for_notice(n.carrier_notice_id),
                             {"carrier_notice_id": n.carrier_notice_id, "carrier": n.carrier,
                              "raw_text": n.raw_text(self.config.fmt)}, check=check, command_id=command_id)

    def resend_notice(self, index: int) -> Any:
        """El transportista reenvía el aviso `index`: devuelve la respuesta de intake (un rechazo no es error)."""
        return self._send_notice(self.data.notices[index], check=False)

    def resend_notice_concurrently(self, index: int, times: int) -> list[Any]:
        """El transportista reenvía el aviso `index` `times` veces a la vez, cada envío desde su hilo y con su
        conexión (spec synthetic-data, «Reenvío de avisos del transportista»); devuelve las respuestas de intake."""
        n, t = self.data.notices[index], self.targets["intake"]
        payload = {"carrier_notice_id": n.carrier_notice_id, "carrier": n.carrier,
                   "raw_text": n.raw_text(self.config.fmt)}

        def send(conn: psycopg.Connection) -> Any:
            command = CommandEnvelope(command_id=uuid.uuid4(), command_type="RegisterCarrierNotice",
                                      correlation_id=incident_id_for_notice(n.carrier_notice_id),
                                      causation_id=None, payload=payload)
            return execute_command(conn, t.schema, command, t.specs["RegisterCarrierNotice"])

        return deliver_concurrently(lambda: connect(self.settings.database_url), [send] * times)

    def step(self) -> int:
        """Un turno: relays, lado de lectura, consumidores, dispatcher y plazos. Devuelve cuánto avanzó. Con los
        componentes en procesos propios, solo mide lo que avanzaron."""
        if self.config.component_processes:
            return self._observe_components()
        progressed = sum(self._publish(r) for r in self._relays)
        progressed += sum(r.run_once(timeout=0) for r in self._active_readers())
        for i, (delivery, factory) in enumerate(self._deliveries):
            if delivery.consumer.name in self._threaded:
                continue  # tiene su propio hilo
            for _ in range(20):
                try:
                    outcome = delivery.run_once(0.01)
                    if outcome is None:
                        break
                    self._record(delivery, outcome)
                    progressed += 1
                except Exception as exc:  # noqa: BLE001 — una caída de un proceso, a propósito o no
                    progressed += 1
                    self._on_crash(i, delivery, factory, exc)
                    break
        external = self.config.external_orchestrator  # el dispatcher y los plazos son de las instancias
        if not (self.config.dispatcher_thread or external):  # con hilo propio, lo atiende `_dispatch`
            for _ in range(TURN_COMMANDS):
                if not dispatch_once(self._dispatcher_conn, self.settings, self.rules, invoke=self.commands,
                                     backoff=self._dispatch_backoff):
                    break
                progressed += 1
        recovered = self.commands.flush_recovered(self._result_freshness)
        self.recovered.update(recovered)
        progressed += sum(recovered.values())
        if self.config.track_timeouts and not external:
            self._record_timeouts()
        if not external:
            progressed += expire_once(self._timer_conn, self.settings, self.rules)
        progressed += interpreter.expire_parked(self._timer_conn)
        return progressed

    def _publish(self, relay: ComponentRelay) -> int:
        """Outbox y dead letters de un relay. Con `tolerate_relay_errors`, un lote que el broker no confirma (o un
        productor que no puede encolar) se cuenta y el turno sigue, como `ComponentRelay.run_until`."""
        timeout = self.config.relay_flush_timeout_seconds
        if not self.config.tolerate_relay_errors:
            return relay.run_once(flush_timeout=timeout) + relay.publish_dead_letters(flush_timeout=timeout)
        started = time.monotonic()
        try:
            return relay.run_once(flush_timeout=timeout) + relay.publish_dead_letters(flush_timeout=timeout)
        except (RelayDeliveryError, KafkaException, BufferError) as exc:
            self.relay_errors[relay._schema] += 1
            error = exc.args[0] if isinstance(exc, KafkaException) and exc.args else None
            if error is not None and getattr(error, "fatal", lambda: False)():  # el productor ya no se recupera
                log.error("relay de %s: error fatal del productor: %s", relay._schema, error)
            else:
                log.info("relay de %s sin confirmar: %s", relay._schema, type(exc).__name__)
            return 0
        finally:
            self.relay_max_seconds = max(self.relay_max_seconds, time.monotonic() - started)

    def _result_freshness(self, command: CommandEnvelope) -> str:
        """E12: el resultado del comando llegará `stale` si su plazo ya se resolvió (venció o llegó otro resultado),
        o `current` si el paso aún lo espera."""
        with self._timer_conn.transaction():
            row = self._timer_conn.execute("SELECT resolved_at FROM orchestrator.deadlines WHERE command_id = %s",
                                           (command.command_id,)).fetchone()
        return "stale" if row is None or row["resolved_at"] is not None else "current"

    def _dispatch(self) -> None:
        """El dispatcher en su propio hilo y con su conexión, como el hilo `command-dispatcher` del orquestador: un
        ejecutor colgado lo bloquea a él, no a los relays ni al lado de lectura. Cuenta como trabajo en curso."""
        conn = connect(self.settings.database_url)
        try:
            while not self._stop.is_set():
                with self._activity.working():
                    did = dispatch_once(conn, self.settings, self.rules, invoke=self.commands,
                                        backoff=self._dispatch_backoff)
                if did:
                    self._activity.progressed()
                else:
                    self._stop.wait(JOB_POLL_S)
        finally:
            conn.close()

    def _record(self, delivery: FaultyDelivery, outcome: Any) -> None:
        if outcome is None:
            return
        with self._crash_lock:  # también lo llaman los consumidores con hilo propio
            self.outcomes[(delivery.consumer.name, getattr(outcome, "value", str(outcome)))] += 1

    def _on_crash(self, i: int, delivery: FaultyDelivery, factory: Callable[[], EventConsumer], exc: Exception) -> None:
        with self._crash_lock:  # los consumidores con hilo propio caen a la vez que el resto
            self.consumer_crashes += 1
        msg = delivery.current
        key = (delivery.consumer.name, msg.partition(), msg.offset()) if msg is not None else None
        if isinstance(exc, KafkaException):  # commit de offset sin broker: el mensaje ya se procesó, no es veneno
            log.warning("%s cae sin confirmar el offset: %r", delivery.consumer.name, exc)
            with self._crash_lock:
                self.commit_failures += 1
        elif not isinstance(exc, ConsumerCrashed):
            log.warning("%s cae: %r", delivery.consumer.name, exc)
            if key is not None:
                with self._crash_lock:
                    self._failures[key] += 1
                    poison = self._failures[key] >= POISON_AFTER
                    self.poison_messages += poison
                if poison:
                    delivery.consumer.commit(msg)  # se salta el mensaje para no quedar atascado
        with contextlib.suppress(Exception):
            delivery.consumer.close()  # sin confirmar el offset: Kafka lo vuelve a entregar
        delivery.consumer = factory()

    def closed(self) -> int:
        row = self.conn.execute(
            "SELECT count(*) AS n FROM views.view_incident_status WHERE status IN ('resolved', 'escalated')"
        ).fetchone()
        self.conn.commit()
        return row["n"]

    def _open_jobs(self) -> int:
        """Trabajos pendientes o en curso de los componentes con comandos largos."""
        if self.config.component_processes:
            return self._open_process_jobs()
        schemas = [t.schema for t in self.targets.values() if any(s.deferred for s in t.specs.values())]
        if not schemas:
            return 0
        rows = self.conn.execute(" UNION ALL ".join(
            f"SELECT count(*) AS n FROM {qualified(schema, 'command_jobs')} WHERE status IN ('pending', 'running')"
            for schema in schemas)).fetchall()
        self.conn.commit()
        return sum(r["n"] for r in rows)

    def _working(self) -> bool:
        """Algún hilo atiende un mensaje o un trabajo, o quedan trabajos por hacer."""
        commands = getattr(self, "commands", None)  # sin montar (tests de los hilos) no hay comandos
        return (self._activity.busy > 0 or self._open_jobs() > 0
                or (commands is not None and commands.awaiting_recovery()))

    def _advance(self) -> bool:
        """Un turno; True si avanzó algo, en el turno o en los hilos, o si los hilos siguen trabajando."""
        progressed = self.step()
        done = self._activity.done
        moved = bool(progressed) or done != self._seen_done or self._working()
        self._seen_done = done
        if not progressed:
            time.sleep(IDLE_SLEEP_S)
        return moved

    def stall_snapshot(self) -> dict[str, Any]:
        """Qué tiene la ejecución por ocupada cuando vence `timeout_seconds` sin cerrar las incidencias (E9 y E17 se
        quedaban así): lo que cuenta como trabajo en curso, la pila de cada hilo (leer sus marcos no toca Kafka ni la
        base de ese hilo) y las sesiones de PostgreSQL que esperan un bloqueo o tienen una transacción abierta."""
        frames = sys._current_frames()
        current = threading.get_ident()
        threads = {t.name: traceback.format_stack(frames[t.ident])[-STALL_FRAMES:]
                   for t in threading.enumerate() if t.ident in frames and t.ident != current}
        commands = getattr(self, "commands", None)
        return {"busy": self._activity.busy, "open_jobs": _try(self._open_jobs),
                "awaiting_recovery": _try(commands.awaiting_recovery) if commands is not None else None,
                "threads": threads, "database": self._database_waits()}

    def _database_waits(self) -> dict[str, Any]:
        conn = getattr(self, "conn", None)
        if conn is None:
            return {"error": "sin conexión"}
        try:
            with conn.transaction():
                sessions = conn.execute(
                    "SELECT pid, pg_blocking_pids(pid) AS blocked_by, state, wait_event_type, wait_event,"
                    " now() - xact_start AS xact_age, left(query, 300) AS query FROM pg_stat_activity"
                    " WHERE datname = current_database() AND pid <> pg_backend_pid()"
                    " AND (wait_event_type = 'Lock' OR state LIKE 'idle in transaction%%')"
                ).fetchall()
                parked = conn.execute("SELECT count(*) AS n FROM interpreter.pending_notices").fetchone()["n"]
        except Exception as exc:  # noqa: BLE001 — diagnóstico: no debe tumbar el experimento
            return {"error": repr(exc)}
        return {"sessions": [{k: str(v) for k, v in row.items()} for row in sessions], "pending_notices": parked}

    @staticmethod
    def _dump_stall(stall: dict[str, Any]) -> None:
        """Al log y a `sys.__stderr__` (en consola al ejecutar el experimento; pytest lo captura, así que los tests de
        integración lo sacan del informe)."""
        summary = {k: v for k, v in stall.items() if k != "threads"}
        log.warning("plazo vencido sin cerrar las incidencias: %s", json.dumps(summary, default=str))
        stream = STALL_STREAM or sys.__stderr__
        stream.write("== harness: plazo vencido sin cerrar las incidencias ==\n"
                     + json.dumps(summary, indent=1, default=str, ensure_ascii=False) + "\n")
        for name, stack in stall["threads"].items():
            stream.write(f"-- hilo {name}\n{''.join(stack)}")
        stream.flush()

    def _settle(self) -> None:
        """Último repaso, hasta `idle_seconds`: turnos mientras algo avance o un hilo trabaje, y, cuando todo parece
        quieto, uno más. Un hilo puede confirmar una fila de outbox (un resultado retrasado) justo después del último
        turno y antes de dejar de contar como trabajo: el turno extra la publica. No espera a ningún lag: lo que
        `event_log` aún no haya leído lo lee `catch_up_log`, una sola vez, antes de comprobar las invariantes."""
        end = time.monotonic() + self.config.idle_seconds
        while time.monotonic() < end:
            if not (self.step() or self._working() or self.step()):
                return

    def run_while(self, keep_going: Callable[[], bool], timeout: float, *,
                  on_turn: Callable[[], Any] = lambda: None) -> float:
        """Turnos mientras `keep_going()` y durante `timeout` como mucho; devuelve el tiempo que pasó."""
        start = time.monotonic()
        while keep_going() and time.monotonic() - start < timeout:
            self._advance()
            on_turn()
        return time.monotonic() - start

    def catch_up_log(self, timeout: float = LOG_CATCH_UP_S) -> bool:
        """`event_log` lee hasta las marcas de agua altas de ahora (I4 busca ahí cada fila publicada). False si no
        llega en `timeout`."""
        if self.config.component_processes:  # con los procesos ya parados, `event-log` no avanza: una sola lectura
            return self._catch_up_processes(0.0 if self.exit_codes else timeout)
        try:
            self._readers[0].catch_up(timeout)
        except TimeoutError as exc:
            log.warning("%s", exc)
            return False
        return True

    def run_until_closed(self, *, on_turn: Callable[[], Any] = lambda: None, upto: int | None = None) -> float:
        """Turnos hasta cerrar todas las incidencias, o hasta `idle_seconds` sin avance (con los hilos ociosos
        y sin trabajos pendientes) o `timeout_seconds` en total. `on_turn` se llama tras cada turno (E16 muestrea
        el estado de la saga). `upto`: incidencias cerradas que bastan (por defecto, todas las de la ejecución; E19 cierra
        primero una parte)."""
        start = last = time.monotonic()
        target = self.config.incidents if upto is None else upto
        while self.closed() < target:
            now = time.monotonic()
            if now - start > self.config.timeout_seconds:
                self.stall = self.stall_snapshot()
                self._dump_stall(self.stall)
                break
            if self._advance():
                last = now
            elif now - last > self.config.idle_seconds:
                break
            on_turn()
        # Un último repaso para que las vistas y el log reflejen todo lo publicado.
        self._settle()
        return time.monotonic() - start

    # -- reproducción del log (E21) ------------------------------------------------------------
    def consumers(self) -> list[tuple[str, str]]:
        """Nombre y esquema (su inbox) de cada consumidor de Kafka con inbox."""
        return [(d.consumer.name, d.consumer.schema) for d, _ in self._deliveries]

    def messages(self) -> int:
        """Mensajes que hay ahora en el topic de la ejecución."""
        return sum(self._readers[0].high_watermarks().values())

    def _active_readers(self) -> list[Any]:
        """Consumidores del lado de lectura en marcha: `event_log` ([0]) y, si no están parados, los proyectores."""
        return self._readers[:1] if self._projectors_paused else self._readers

    def lag(self) -> int:
        """Lo que les falta por leer a los grupos de Kafka y al lado de lectura."""
        return sum(self.lags().values())

    def lags(self) -> dict[str, int]:
        """Lag de cada grupo de Kafka y de cada consumidor posicionado del lado de lectura (E18; E10, con los
        componentes en procesos)."""
        if self.config.component_processes:
            return self._process_lags()
        highs = self._readers[0].high_watermarks()
        topic = self.settings.events_topic
        lags = {group: group_lag(self._admin, group, topic, highs) for group in self._factories()}
        return lags | {reader.consumer: reader.lag() for reader in self._readers}

    def consumer_state(self) -> dict[str, dict[str, Any]]:
        """Diagnóstico de cada consumidor de Kafka (E9). Uno que se atiende en los turnos: particiones asignadas con su
        posición (un error de Kafka queda en `error`). Uno con hilo propio: solo si el hilo sigue vivo; consultar su
        asignación desde este hilo mientras el suyo está en `poll` se bloquea sin límite (lo vio el CI de E9)."""
        alive = {t.name: t.is_alive() for t in self._threads} | dict.fromkeys(self._stuck, True)
        state: dict[str, dict[str, Any]] = {}
        for delivery, _ in self._deliveries:
            consumer = delivery.consumer
            entry: dict[str, Any] = {}
            if consumer.name in self._threaded:  # también tras `_stop_threads`, que vacía `_threads`
                entry["thread_alive"] = alive.get(f"harness-{consumer.name}", False)
            else:
                try:
                    entry["positions"] = consumer.positions()
                except Exception as exc:  # noqa: BLE001 — diagnóstico: no debe tumbar el experimento
                    entry["error"] = repr(exc)
            state[consumer.name] = entry
        return state

    # -- mensajes veneno (E18) -----------------------------------------------------------------
    def dead_letter_consumers(self) -> list[tuple[str, str, Callable[[str | None], bool]]]:
        """(consumidor, esquema de su `dead_letters`, si atiende un `event_type`) de cada consumidor del
        topic: los de Kafka, `event_log` (atiende todos los tipos) y cada proyector (spec resilience)."""
        kafka = [(d.consumer.name, d.consumer.schema, d.consumer.attends) for d, _ in self._deliveries]
        return [*kafka, (self._readers[0].consumer, READ_SIDE[0], lambda _type: True),
                *((p.name, READ_SIDE[1], p.attends) for p in self._projectors)]

    def partitions(self) -> list[int]:
        """Particiones del topic de eventos de la ejecución."""
        return self._readers[0].partitions()

    def dlq_messages(self) -> int:
        """Mensajes que hay ahora en el topic DLQ de la ejecución (marcas de agua altas)."""
        probe = make_consumer(self.settings.kafka_bootstrap_servers, group_id=f"{self.settings.dlq_topic}.probe")
        try:
            topic = self.settings.dlq_topic
            parts = probe.list_topics(topic, timeout=10).topics[topic].partitions
            return sum(high_watermark(probe, TopicPartition(topic, p)) for p in parts)
        finally:
            probe.close()

    def retries(self, partition: int, offset: int) -> dict[str, int]:
        """Caídas de cada consumidor de Kafka con el mensaje en (`partition`, `offset`) antes de saltarlo."""
        with self._crash_lock:
            return {name: n for (name, p, o), n in self._failures.items() if (p, o) == (partition, offset)}

    def replay(self, inbox_off: Iterable[str] = ()) -> float:
        """Con el sistema parado, lleva al principio los offsets de los grupos de Kafka (spec synthetic-data,
        «Reinicio de offsets de un grupo») y rebobina el lado de lectura, que guarda su posición en PostgreSQL;
        arranca otra vez hasta que el lag sea 0. `inbox_off`: grupos que se arrancan sin inbox (control)."""
        self._stop_threads()
        self._close_deliveries()
        self._inbox_off = frozenset(inbox_off)
        reset_group_offsets(self._admin, self._factories(), self.settings.events_topic,
                            self._readers[0].partitions())
        for reader in self._readers:
            reader.rewind()
        self._init_threads()
        self._build_deliveries()
        self._start_threads()
        return self.run_until_caught_up()

    # -- pérdida del topic (E19) ----------------------------------------------------------------
    def lose_topic(self, mode: str) -> dict[int, int]:
        """Con el sistema parado, pierde el contenido de `eda.events` (spec synthetic-data, «Pérdida del topic»):
        `truncated` lo vacía con `delete_records` (los offsets siguen) y `recreated` lo borra y lo crea de nuevo (los
        offsets vuelven a 0), con productores y lado de lectura nuevos, los grupos en 0 y la posición del lado de
        lectura en -1. Deja el sistema parado; devuelve las marcas de agua altas de antes de la pérdida."""
        if mode not in TOPIC_LOSS_MODES:
            raise ValueError(f"modo de pérdida del topic desconocido: {mode}")
        self._stop_threads()
        self._close_deliveries()
        topic = self.settings.events_topic
        highs = self._readers[0].high_watermarks()
        if mode == "truncated":
            for fut in self._admin.delete_records([TopicPartition(topic, p, high) for p, high in highs.items()]
                                                  ).values():
                fut.result(timeout=30)
            return highs
        for fut in self._admin.delete_topics([topic]).values():
            fut.result(timeout=30)
        self._recreate_topic(topic, len(highs))
        for closable in [*self._relays, *self._readers]:
            with contextlib.suppress(Exception):
                closable.close()
        s = self.settings
        self._relays = [ComponentRelay.from_settings(s, schema) for schema in PUBLISHERS
                        if not (schema == orchestrator.SCHEMA and self.config.external_orchestrator)]
        self._relays += [ComponentRelay.from_settings(s, schema, outbox=False) for schema in READ_SIDE]
        with self.conn.transaction():
            for schema in READ_SIDE:
                self.conn.execute(f"UPDATE {qualified(schema, 'offsets')} SET kafka_offset = -1 WHERE kafka_topic = %s",
                                  (topic,))
        self._readers = [event_log.build(s), *(consumer_for(p, s, self._connect()) for p in self._projectors)]
        self._closables += [*self._relays, *self._readers]
        reset_group_offsets(self._admin, self._factories(), topic, sorted(highs))
        return highs

    def _recreate_topic(self, topic: str, partitions: int) -> None:
        deadline = time.monotonic() + TOPIC_RECREATE_TIMEOUT_S
        while True:
            try:
                for fut in self._admin.create_topics([NewTopic(topic, num_partitions=partitions,
                                                               replication_factor=1)]).values():
                    fut.result(timeout=30)
                return
            except Exception:  # el broker aún está borrando el topic anterior
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.5)

    def republish(self) -> dict[str, int]:
        """Con el sistema parado, republica a la vez el outbox de todos los componentes, mezclado por la posición que
        cada fila tenía en el topic perdido (`outbox.republish_in_order`), y arranca el sistema otra vez; devuelve las
        filas republicadas por componente."""
        schemas = [relay._schema for relay in self._relays if relay._has_outbox]
        conn = connect(self.settings.database_url)
        try:
            rows = outbox.republish_in_order(conn, make_producer(self.settings.kafka_bootstrap_servers), schemas,
                                             self.settings.events_topic)
        finally:
            conn.close()
        self._init_threads()
        self._build_deliveries()
        self._start_threads()
        return rows

    def run_until_caught_up(self) -> float:
        """Turnos hasta que nadie avance y el lag sea 0, o `timeout_seconds`; después, un último repaso."""
        start = time.monotonic()
        while time.monotonic() - start < self.config.timeout_seconds:
            if not self._advance() and self.lag() == 0:
                break
        self._settle()
        return time.monotonic() - start

    def redeliver(self, command_type: str = "ExecuteStep", *, idempotency: str | None = None) -> list[CommandReply]:
        """Entrega otra vez, con su `command_id` original, los comandos que la saga ya entregó, y espera a que sus
        trabajos diferidos terminen. `idempotency`: modo de los sistemas que los atienden (por defecto, el de la
        ejecución). Otro modo es definitivo para el resto de la ejecución: cambia los sistemas, sus workers de
        trabajos y el dispatcher, y quita las restricciones de unicidad de negocio (control de E21)."""
        if idempotency is not None and idempotency != self.config.idempotency:
            if idempotency not in IDEMPOTENCY_MODES:
                raise ValueError(f"modo de idempotencia desconocido: {idempotency}")
            self._stop_threads()
            self._set_idempotency(idempotency)
            self.targets = self._targets(idempotency)
            self.commands = InProcessCommands(self._connect(), {target_url(self.settings, t): v for t, v in
                                                                self.targets.items()}, self.config.faults, self._crashed,
                                              connect=lambda: connect(self.settings.database_url),
                                              sleep=lambda seconds: self._stop.wait(seconds))
            self._init_threads()
            self._start_threads()
        rows = self.conn.execute(
            "SELECT command_id, incident_id, command_type, target, causation_id, payload"
            " FROM orchestrator.command_outbox WHERE command_type = %s AND status IN ('delivered', 'rejected')"
            " ORDER BY command_id", (command_type,)).fetchall()
        self.conn.commit()
        replies = [self.commands(target_url(self.settings, r["target"]), r["command_type"],
                                 command_id=r["command_id"], correlation_id=r["incident_id"],
                                 causation_id=r["causation_id"], payload=r["payload"],
                                 timeout=self.settings.command_http_timeout_s) for r in rows]
        self.run_until_caught_up()
        return replies

    # -- métricas ----------------------------------------------------------------------------
    @property
    def crashes(self) -> int:
        commands = getattr(self, "commands", None)  # con los componentes en procesos no hay comandos en proceso
        return self.consumer_crashes + (commands.crashes if commands is not None else 0)

    def duplicates(self) -> dict[str, int]:
        self.conn.commit()  # cierra la transacción implícita de las lecturas anteriores
        if self.config.component_processes:  # las ejecuciones las anotaron los procesos ejecutores
            for attr in set(SERVICE_OF.values()):
                getattr(self.services, attr).executions.clear()
            for (service, key), n in self.service_executions().items():
                getattr(self.services, service).executions[key] = n
        return effect_duplicates(self.conn, self.services)

    def result(self, elapsed: float) -> RunResult:
        c = self.conn
        statuses = Counter(r["status"] for r in c.execute("SELECT status FROM views.view_incident_status").fetchall())
        latencies = [r["ms"] for r in c.execute(
            """
            SELECT extract(epoch FROM max(e.occurred_at) - min(e.occurred_at)) * 1000 AS ms
            FROM event_log.events e
            WHERE e.event_type IN ('CarrierNoticeReceived', 'IncidentResolved', 'IncidentEscalated')
            GROUP BY e.correlation_id
            HAVING count(*) FILTER (WHERE e.event_type <> 'CarrierNoticeReceived') > 0
            """
        ).fetchall()]
        cost = c.execute(
            "SELECT coalesce(sum(llm_calls), 0) AS calls, coalesce(sum(tokens_in), 0) AS tin, "
            "coalesce(sum(tokens_out), 0) AS tout FROM views.view_cost_per_incident"
        ).fetchone()
        reasons = {r["reason"]: r["n"] for r in c.execute(
            "SELECT coalesce(escalation_reason, 'unknown') AS reason, count(*) AS n FROM views.view_incident_status"
            " WHERE status = 'escalated' GROUP BY 1 ORDER BY 1"
        ).fetchall()}
        c.commit()
        closed = statuses["resolved"] + statuses["escalated"]
        return RunResult(
            config=self.config, model=getattr(self.llm, "model", "unknown"), closed=closed,
            open=self.config.incidents - closed, resolved=statuses["resolved"], escalated=statuses["escalated"],
            duplicates=self.duplicates(), consumer_crashes=self.crashes,
            poison_messages=self.poison_messages, latencies_ms=[float(v) for v in latencies],
            llm_calls=int(cost["calls"]), tokens_in=int(cost["tin"]), tokens_out=int(cost["tout"]),
            elapsed_seconds=elapsed, escalation_reasons=reasons,
        )


@contextlib.contextmanager
def started(base: Settings, config: RunConfig, **kw: Any) -> Iterator[Harness]:
    """Arnés con los avisos enviados y el sistema ejecutado hasta cerrar las incidencias."""
    with Harness(base, config, **kw) as h:
        h.send_notices()
        h.elapsed = h.run_until_closed()
        yield h


def run(base: Settings, config: RunConfig, **kw: Any) -> RunResult:
    with started(base, config, **kw) as h:
        return h.result(h.elapsed)
