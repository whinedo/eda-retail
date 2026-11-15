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

Modos de idempotencia (E1, supuesto A3):
- `full`: inbox en los consumidores y en los comandos, `effect_keys` y servicios con clave.
- `no_effect_keys`: los ejecutores e `inventory` pierden su inbox de comandos, `effect_keys` y la
  deduplicación de los servicios; el resto conserva su inbox. Es lo que mide H1.
- `none`: todo desactivado.
Un duplicado es una ejecución externa repetida del mismo paso o una reserva de más.
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from decimal import Decimal
from typing import Any, Self

import psycopg
from confluent_kafka.admin import AdminClient, NewTopic

from eda_retail.config import Settings
from eda_retail.event_log import cli as event_log
from eda_retail.events.catalog import build_event
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.experiments.oracle import Oracle
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.command_client import CommandReply, CommandUnavailable
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec, execute_command
from eda_retail.infra.consumer import EventConsumer
from eda_retail.infra.db import connect
from eda_retail.infra.jobs import JobWorker, run_job_once
from eda_retail.infra.kafka import make_consumer
from eda_retail.infra.migrations import upgrade
from eda_retail.infra.outbox import ComponentRelay
from eda_retail.infra.schemas import qualified
from eda_retail.intake import handler as intake
from eda_retail.interpreter import consumer as interpreter
from eda_retail.inventory import handler as inventory
from eda_retail.llm.generation import LlmClient
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
from eda_retail.step_executors.services import Services
from eda_retail.synthetic.faults import (
    INJECTED_FAILURE,
    ConsumerCrashed,
    FaultPlan,
    FaultyDelivery,
    should_fail,
)
from eda_retail.synthetic.generator import Dataset, generate

log = logging.getLogger(__name__)

IDEMPOTENCY_MODES = ("full", "no_effect_keys", "none")
POISON_AFTER = 3  # caídas con el mismo mensaje antes de saltarlo
TURN_COMMANDS = 200  # comandos que el dispatcher invoca como mucho en un turno
# Un aviso cuyo pedido aún no está en view_orders se aparca (design.md §4.3); en el arnés el plazo es corto
# para que una carrera entre particiones no deje la incidencia abierta hasta el plazo del despliegue.
PENDING_NOTICE_TIMEOUT_S = 2.0
SERVICE_OF = {"notify_customer": "mail", "file_claim": "claims", "reissue_shipment": "carrier",
              "issue_refund": "payments"}
JOB_POLL_S = 0.05  # sondeo de un worker de trabajos sin trabajo
CONSUMER_POLL_S = 0.1  # sondeo de Kafka de un consumidor en su hilo
JOIN_TIMEOUT_S = 30.0  # espera al parar un hilo (puede estar dentro de una llamada al LLM)
IDLE_SLEEP_S = 0.01  # pausa de un turno sin avance mientras los hilos trabajan
THREADED_CONSUMERS = frozenset({interpreter.SCHEMA})  # consumidores con su propio hilo
PUBLISHERS = ("intake", "interpreter", "planner", "replanner", "orchestrator", "inventory",
              *(f"executor_{a}" for a in EXECUTOR_ACTIONS))


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


def service_duplicates(services: Services) -> Counter[str]:
    """Ejecuciones externas repetidas del mismo paso, por servicio simulado."""
    out: Counter[str] = Counter()
    for action, attr in SERVICE_OF.items():
        out[action] += sum(n - 1 for n in getattr(services, attr).executions.values() if n > 1)
    return out


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


class InProcessCommands:
    """Sustituye a `command_client.invoke`: ejecuta el comando con el handler de su sistema, sin HTTP."""

    def __init__(self, conn: psycopg.Connection, targets: dict[str, Target], plan: FaultPlan,
                 crashed: set[str]) -> None:
        self._conn = conn
        self._targets = targets
        self._plan = plan
        self._crashed = crashed
        self._handled: Counter[str] = Counter()
        self.crashes = 0

    def __call__(self, url: str, command_type: str, *, command_id: uuid.UUID, correlation_id: str,
                 causation_id: uuid.UUID | None, payload: dict[str, Any], timeout: float) -> CommandReply:
        target = self._targets[url]
        command = CommandEnvelope(command_id=command_id, command_type=command_type, correlation_id=correlation_id,
                                  causation_id=causation_id, payload=payload)
        response = None
        for _ in range(self._plan.duplicates):  # entrega al menos una vez: el mismo command_id
            response = execute_command(self._conn, target.schema, command, target.specs[command_type],
                                       inbox_enabled=target.inbox)
        if command_type == "ExecuteStep" and response.status_code == 202:
            self._crash_after_commit(f"executor-{payload['action']}")
        return CommandReply(response.status_code, response.body, response.replayed)

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
                 data: Dataset | None = None, carrier: CarrierConsultant | None = None) -> None:
        """`carrier`: un cliente A2A en proceso para el planificador. Sin él (por defecto) el planificador no
        consulta al transportista, como en la versión 1, aunque POC_VERSION=2: E5 y E6 siguen comparables."""
        self.config = config
        self._carrier = carrier
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
        self._init_threads()

    def _init_threads(self) -> None:
        self._activity = Activity()
        self._stop = threading.Event()
        self._crash_lock = threading.Lock()
        self._threads: list[threading.Thread] = []
        self._workers: list[JobWorker] = []
        self._threaded = set(THREADED_CONSUMERS)
        self._seen_done = 0

    # -- ciclo de vida ---------------------------------------------------------------------
    def __enter__(self) -> Self:
        if self.config.idempotency not in IDEMPOTENCY_MODES:
            raise ValueError(f"modo de idempotencia desconocido: {self.config.idempotency}")
        self._database = isolated_database(self._base, self.name)
        url = self._database.__enter__()
        topic = f"eda.{self.name}"
        self._admin = AdminClient({"bootstrap.servers": self._base.kafka_bootstrap_servers})
        for fut in self._admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1)
                                              for t in (topic, f"{topic}.dlq")]).values():
            fut.result(timeout=30)
        self.settings = replace(self._base, database_url=url, events_topic=topic, dlq_topic=f"{topic}.dlq",
                                inbox_enabled=self.config.idempotency != "none",
                                step_backoff_base_seconds=0.01, step_backoff_max_seconds=0.05,
                                pending_notice_timeout_s=PENDING_NOTICE_TIMEOUT_S)
        self.conn = connect(url)
        for service in (self.services.mail, self.services.claims, self.services.carrier, self.services.payments):
            service.idempotent = self.config.idempotency == "full"
        self._build()
        self._start_threads()
        self._seed()
        return self

    def __exit__(self, *exc: object) -> None:
        self._stop_threads()
        for delivery, _ in self._deliveries:
            with contextlib.suppress(Exception):
                delivery.consumer.close()
        for closable in self._closables:
            with contextlib.suppress(Exception):
                closable.close()
        with contextlib.suppress(Exception):
            self.conn.close()
        self._database.__exit__(None, None, None)
        with contextlib.suppress(Exception):
            for fut in self._admin.delete_topics([self.settings.events_topic, self.settings.dlq_topic]).values():
                fut.result(timeout=30)

    @property
    def llm(self) -> LlmClient:
        if self._llm is None:
            self._llm = Oracle(self.data).client()
        return self._llm

    def _connect(self) -> psycopg.Connection:
        conn = connect(self.settings.database_url)
        self._closables.append(conn)
        return conn

    def _targets(self) -> dict[str, Target]:
        s, plan, mode = self.settings, self.config.faults, self.config.idempotency
        effects = replace(s, effect_keys_enabled=mode == "full")  # ejecutores e inventory
        targets = {
            "intake": Target(intake.SCHEMA, intake.specs(), mode != "none"),
            "planner": Target(planner.SCHEMA, planner.specs(s, self.llm, self.rules, carrier=self._carrier),
                              mode != "none"),
            "replanner": Target(replanner.SCHEMA, replanner.specs(s, self.llm, self.rules), mode != "none"),
            "inventory": Target(inventory.SCHEMA, dict(inventory.specs(effects), ExecuteStep=failing(
                inventory.specs(effects)["ExecuteStep"], inventory.SCHEMA, plan)), mode == "full"),
        }
        for action in EXECUTOR_ACTIONS:
            spec = step_executors.specs(effects, action, self.llm, self.rules,
                                        service=getattr(self.services, SERVICE_OF[action]))["ExecuteStep"]
            targets[f"executor_{action}"] = Target(f"executor_{action}",
                                                   {"ExecuteStep": failing(spec, f"executor-{action}", plan)},
                                                   mode == "full")
        return targets

    def _build(self) -> None:
        s = self.settings
        self.targets = self._targets()
        self.commands = InProcessCommands(self._connect(), {target_url(s, t): v for t, v in self.targets.items()},
                                          self.config.faults, self._crashed)
        self._seed_conn = self._connect()
        self._dispatcher_conn = self._connect()
        self._timer_conn = self._connect()
        self._dispatch_backoff = Backoff(0.01, 0.05, seed=self.config.seed)
        self._relays = [ComponentRelay.from_settings(s, schema) for schema in PUBLISHERS]
        self._readers = [event_log.build(s), *(consumer_for(p, s, self._connect()) for p in build_all())]
        self._closables += [*self._relays, *self._readers]
        factories: dict[str, Callable[[], EventConsumer]] = {
            interpreter.SCHEMA: lambda: interpreter.build_consumer(
                self._connect(), make_consumer(s.kafka_bootstrap_servers, interpreter.SCHEMA), s, self.llm,
                self.rules),
            orchestrator.SCHEMA: lambda: orchestrator.build_consumer(
                self._connect(), make_consumer(s.kafka_bootstrap_servers, orchestrator.SCHEMA), s, self.rules),
        }
        for name, factory in factories.items():
            if name in self._threaded:
                factory = self._tracked_factory(factory)
            delivery = FaultyDelivery(factory(), self.config.faults, crashed=self._crashed)
            self._deliveries.append((delivery, factory))

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
            worker = JobWorker(lambda: connect(s.database_url), target.schema, target.specs,
                               lease_seconds=s.job_lease_seconds, poll_interval=JOB_POLL_S, max_attempts=s.job_max_attempts,
                               backoff=Backoff(0.01, 0.05, seed=self.config.seed), run_once=self._tracked_job,
                               name=f"harness-jobs-{target.schema}")
            worker.start()
            self._workers.append(worker)
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
                delivery.run_once(CONSUMER_POLL_S)
            except Exception as exc:  # noqa: BLE001 — una caída de un proceso, a propósito o no
                self._on_crash(i, delivery, factory, exc)

    def _stop_threads(self) -> None:
        self._stop.set()
        for worker in self._workers:
            worker.stop(JOIN_TIMEOUT_S)
        for thread in self._threads:
            thread.join(JOIN_TIMEOUT_S)
        self._workers, self._threads = [], []

    def _command(self, target: str, command_type: str, correlation_id: str, payload: dict[str, Any]) -> None:
        """Comando de entrada (datos de ejemplo, avisos): directo a su handler, sin fallos inyectados."""
        t = self.targets[target]
        command = CommandEnvelope(command_id=uuid.uuid4(), command_type=command_type, correlation_id=correlation_id,
                                  causation_id=None, payload=payload)
        response = execute_command(self._seed_conn, t.schema, command, t.specs[command_type])
        if response.status_code != 202:
            raise RuntimeError(f"{command_type} rechazado: {response.body}")

    def _seed(self) -> None:
        """Siembra pedidos y existencias y espera a que `view_orders` los tenga (como un despliegue ya en marcha)."""
        self._command("inventory", "SeedSampleData", inventory.SAMPLE_DATA_CORRELATION,
                      seed_payload(self.data, tuple(self.config.faults.empty_warehouses)))
        deadline = time.monotonic() + self.config.idle_seconds
        while self._projected_orders() < len(self.data.orders) and time.monotonic() < deadline:
            self.step()

    def _projected_orders(self) -> int:
        row = self.conn.execute("SELECT count(*) AS n FROM views.view_orders WHERE order_id = ANY(%s)",
                                ([o.order_id for o in self.data.orders],)).fetchone()
        self.conn.commit()
        return row["n"]

    # -- ejecución ---------------------------------------------------------------------------
    def send_notices(self) -> None:
        for n in self.data.notices:
            self._command("intake", "RegisterCarrierNotice", incident_id_for_notice(n.carrier_notice_id),
                          {"carrier_notice_id": n.carrier_notice_id, "carrier": n.carrier,
                           "raw_text": n.raw_text(self.config.fmt)})

    def step(self) -> int:
        """Un turno: relays, lado de lectura, consumidores, dispatcher y plazos. Devuelve cuánto avanzó."""
        progressed = sum(r.run_once() + r.publish_dead_letters() for r in self._relays)
        progressed += sum(r.run_once(timeout=0) for r in self._readers)
        for i, (delivery, factory) in enumerate(self._deliveries):
            if delivery.consumer.name in self._threaded:
                continue  # tiene su propio hilo
            for _ in range(20):
                try:
                    if delivery.run_once(0.01) is None:
                        break
                    progressed += 1
                except Exception as exc:  # noqa: BLE001 — una caída de un proceso, a propósito o no
                    progressed += 1
                    self._on_crash(i, delivery, factory, exc)
                    break
        for _ in range(TURN_COMMANDS):
            if not dispatch_once(self._dispatcher_conn, self.settings, self.rules, invoke=self.commands,
                                 backoff=self._dispatch_backoff):
                break
            progressed += 1
        progressed += expire_once(self._timer_conn, self.settings, self.rules)
        progressed += interpreter.expire_parked(self._timer_conn)
        return progressed

    def _on_crash(self, i: int, delivery: FaultyDelivery, factory: Callable[[], EventConsumer], exc: Exception) -> None:
        with self._crash_lock:  # los consumidores con hilo propio caen a la vez que el resto
            self.consumer_crashes += 1
        msg = delivery.current
        key = (delivery.consumer.name, msg.partition(), msg.offset()) if msg is not None else None
        if not isinstance(exc, ConsumerCrashed):
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
        return self._activity.busy > 0 or self._open_jobs() > 0

    def _advance(self) -> bool:
        """Un turno; True si avanzó algo, en el turno o en los hilos, o si los hilos siguen trabajando."""
        progressed = self.step()
        done = self._activity.done
        moved = bool(progressed) or done != self._seen_done or self._working()
        self._seen_done = done
        if not progressed:
            time.sleep(IDLE_SLEEP_S)
        return moved

    def run_until_closed(self) -> float:
        """Turnos hasta cerrar todas las incidencias, o hasta `idle_seconds` sin avance (con los hilos ociosos
        y sin trabajos pendientes) o `timeout_seconds` en total."""
        start = last = time.monotonic()
        while self.closed() < self.config.incidents:
            now = time.monotonic()
            if now - start > self.config.timeout_seconds:
                break
            if self._advance():
                last = now
            elif now - last > self.config.idle_seconds:
                break
        # Un último repaso para que las vistas y el log reflejen todo lo publicado.
        end = time.monotonic() + self.config.idle_seconds
        while (self.step() or self._working()) and time.monotonic() < end:
            pass
        return time.monotonic() - start

    # -- métricas ----------------------------------------------------------------------------
    @property
    def crashes(self) -> int:
        return self.consumer_crashes + self.commands.crashes

    def duplicates(self) -> dict[str, int]:
        self.conn.commit()  # cierra la transacción implícita de las lecturas anteriores
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
