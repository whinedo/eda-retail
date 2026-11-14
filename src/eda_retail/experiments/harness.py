"""Arnés de los experimentos (decisión D1 de odd/tasks/experiments.md).

Monta el sistema completo en un solo proceso: relay, proyectores, intérprete, planificador,
replanner, orquestador y los seis ejecutores, con los mismos componentes que despliega
`scripts/run-agents.sh`, atendidos por turnos contra PostgreSQL y Kafka reales. Cada
ejecución usa una base de datos y un topic propios, así no ve datos de otras.

Los consumidores van envueltos en `FaultyDelivery` (duplicados, retraso y caída) y los
ejecutores en `FailingAction` (fallos de paso con semilla). Una caída cierra el consumidor
sin confirmar el offset y crea otro en el mismo grupo, como al reiniciar un proceso; un
mensaje que hace caer al consumidor tres veces se salta (mensaje envenenado).

Modos de idempotencia (E1, supuesto A3):
- `full`: inbox en todos los consumidores, `effect_keys` y servicios con clave.
- `no_effect_keys`: los ejecutores pierden su inbox, `effect_keys` y la deduplicación de
  los servicios; agentes y orquestador conservan su inbox. Es lo que mide H1.
- `none`: todo desactivado. Sin inbox, un evento repetido llega dos veces a los agentes con
  LLM y la incidencia puede escalarse antes de ejecutar ningún paso.
Un duplicado es una ejecución externa repetida del mismo paso o una reserva de más.
"""

from __future__ import annotations

import contextlib
import logging
import time
import uuid
from collections import Counter
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field, replace
from datetime import timedelta
from typing import Any, Self

import psycopg
from confluent_kafka.admin import AdminClient, NewTopic

from eda_retail.agents.cli import build_handlers
from eda_retail.command.escalation import escalation_hook
from eda_retail.command.handler import CommandHandler
from eda_retail.config import Settings
from eda_retail.events.catalog import Action
from eda_retail.executors.actions import build_actions
from eda_retail.executors.base import Executor, ExecutorSettings, dead_letter_on_exhaustion
from eda_retail.executors.retry import Backoff
from eda_retail.executors.services import Services
from eda_retail.experiments.oracle import Oracle
from eda_retail.infra.consumer import EventConsumer
from eda_retail.infra.db import connect
from eda_retail.infra.migrations import upgrade
from eda_retail.infra.relay import OutboxRelay
from eda_retail.llm.generation import LlmClient
from eda_retail.orchestrator.runner import Orchestrator
from eda_retail.projectors.registry import build_all
from eda_retail.synthetic.faults import ConsumerCrashed, FailingAction, FaultPlan, FaultyDelivery, empty_warehouses
from eda_retail.synthetic.generator import Dataset, generate, load
from eda_retail.synthetic.sample import send_notice

log = logging.getLogger(__name__)

IDEMPOTENCY_MODES = ("full", "no_effect_keys", "none")
POISON_AFTER = 3  # caídas con el mismo mensaje antes de saltarlo
_SERVICE_EFFECTS = {"mail": "customer_messages", "claims": "claims", "carrier": "shipments", "payments": "refunds"}


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

    @property
    def repeated_steps(self) -> int:
        return sum(self.duplicates.values())


class Harness:
    """Una ejecución aislada. Usar como contexto: crea y borra su base de datos y su topic."""

    def __init__(self, base: Settings, config: RunConfig, *, llm: LlmClient | None = None,
                 data: Dataset | None = None) -> None:
        self.config = config
        self.data = data or generate(config.seed, config.incidents)
        self._base = base
        self._llm = llm
        self.name = f"exp_{uuid.uuid4().hex[:10]}"
        self.settings: Settings | None = None
        self.conn: psycopg.Connection | None = None
        self.services = Services()
        self._deliveries: list[tuple[FaultyDelivery, Callable[[], EventConsumer]]] = []
        self._crashed: set[str] = set()
        self._failures: Counter[tuple[str, int, int]] = Counter()
        self.consumer_crashes = 0
        self.poison_messages = 0

    # -- ciclo de vida ---------------------------------------------------------------------
    def __enter__(self) -> Self:
        base_url, _, _ = self._base.database_url.rpartition("/")
        with psycopg.connect(f"{base_url}/postgres", autocommit=True) as admin:
            admin.execute(f"CREATE DATABASE {self.name}")
        url = f"{base_url}/{self.name}"
        upgrade(url)
        topic = f"eda.{self.name}"
        self._admin = AdminClient({"bootstrap.servers": self._base.kafka_bootstrap_servers})
        for fut in self._admin.create_topics([NewTopic(t, num_partitions=3, replication_factor=1)
                                              for t in (topic, f"{topic}.dlq")]).values():
            fut.result(timeout=30)
        if self.config.idempotency not in IDEMPOTENCY_MODES:
            raise ValueError(f"modo de idempotencia desconocido: {self.config.idempotency}")
        idem = self.config.idempotency == "full"
        self.settings = replace(self._base, database_url=url, events_topic=topic, dlq_topic=f"{topic}.dlq",
                                inbox_enabled=self.config.idempotency != "none", effect_keys_enabled=idem)
        self.conn = connect(url)
        load(self.conn, self.data)
        if self.config.faults.empty_warehouses:
            empty_warehouses(self.conn, self.config.faults.empty_warehouses)
        for service in (self.services.mail, self.services.claims, self.services.carrier, self.services.payments):
            service.idempotent = idem
        self._build()
        return self

    def __exit__(self, *exc: object) -> None:
        for delivery, _ in self._deliveries:
            with contextlib.suppress(Exception):
                delivery.consumer.close()
        for closable in (getattr(self, "_relay", None),):
            with contextlib.suppress(Exception):
                closable.close()
        for c in (self.conn, getattr(self, "_projector_conn", None)):
            with contextlib.suppress(Exception):
                c.close()
        base_url, _, _ = self._base.database_url.rpartition("/")
        with contextlib.suppress(Exception), psycopg.connect(f"{base_url}/postgres", autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {self.name} WITH (FORCE)")
        with contextlib.suppress(Exception):
            for fut in self._admin.delete_topics([self.settings.events_topic, self.settings.dlq_topic]).values():
                fut.result(timeout=30)

    @property
    def llm(self) -> LlmClient:
        if self._llm is None:
            self._llm = Oracle(self.data).client()
        return self._llm

    def _build(self) -> None:
        s = self.settings
        handler = CommandHandler()
        self._relay = OutboxRelay.from_settings(s)
        self._projector_conn = connect(s.database_url)
        self._projectors = build_all(gap_timeout=timedelta(0))
        components: dict[str, dict[str, Any]] = {
            name: build_handlers(name, self.llm, handler, inbox_enabled=s.inbox_enabled)
            for name in ("interpreter", "planner", "replanner")
        }
        components["orchestrator"] = Orchestrator(handler, inbox_enabled=s.inbox_enabled).handlers
        actions = build_actions(self.llm, handler, self.services)
        # Los ejecutores pierden su inbox salvo en `full` (la protección de los efectos).
        executor_settings = replace(s, inbox_enabled=self.config.idempotency == "full")
        for action in (a.value for a in Action):
            name = f"executor-{action}"
            step = actions[action]
            if self.config.faults.fail_probability.get(action):
                step = FailingAction(step, self.config.faults)
            executor = Executor(step, handler, settings=ExecutorSettings.from_settings(executor_settings),
                                backoff=Backoff(0.01, 0.05), on_exhausted=dead_letter_on_exhaustion(name))
            components[name] = executor.handlers
        for name, handlers in components.items():
            own = executor_settings if name.startswith("executor-") else s
            def factory(name: str = name, handlers: dict = handlers, own: Settings = own) -> EventConsumer:
                return EventConsumer.from_settings(
                    own, name=name, handlers=handlers, on_dead_letter=escalation_hook(handler, producer=name))
            delivery = FaultyDelivery(factory(), self.config.faults, crashed=self._crashed)
            self._deliveries.append((delivery, factory))

    # -- ejecución ---------------------------------------------------------------------------
    def send_notices(self) -> None:
        for n in self.data.notices:
            send_notice(self.conn, n.raw_text(self.config.fmt), carrier=n.carrier,
                        carrier_notice_id=n.carrier_notice_id)

    def step(self) -> int:
        """Un turno: relay, proyectores y cada consumidor. Devuelve cuánto avanzó."""
        progressed = self._relay.run_once() + self._relay.publish_dead_letters()
        progressed += sum(p.run_once(self._projector_conn) for p in self._projectors)
        for i, (delivery, factory) in enumerate(self._deliveries):
            for _ in range(20):
                try:
                    if delivery.run_once(0.01) is None:
                        break
                    progressed += 1
                except Exception as exc:  # noqa: BLE001 — una caída de un proceso, a propósito o no
                    progressed += 1
                    self._on_crash(i, delivery, factory, exc)
                    break
        return progressed

    def _on_crash(self, i: int, delivery: FaultyDelivery, factory: Callable[[], EventConsumer], exc: Exception) -> None:
        self.consumer_crashes += 1
        msg = delivery.current
        key = (delivery.consumer.name, msg.partition(), msg.offset()) if msg is not None else None
        if not isinstance(exc, ConsumerCrashed):
            log.warning("%s cae: %r", delivery.consumer.name, exc)
            if key is not None:
                self._failures[key] += 1
                if self._failures[key] >= POISON_AFTER:
                    self.poison_messages += 1
                    delivery.consumer.commit(msg)  # se salta el mensaje para no quedar atascado
        with contextlib.suppress(Exception):
            delivery.consumer.close()  # sin confirmar el offset: Kafka lo vuelve a entregar
        delivery.consumer = factory()

    def closed(self) -> int:
        row = self.conn.execute(
            "SELECT count(*) AS n FROM view_incident_status WHERE status IN ('resolved', 'escalated')"
        ).fetchone()
        self.conn.commit()
        return row["n"]

    def run_until_closed(self) -> float:
        start = last = time.monotonic()
        while self.closed() < self.config.incidents:
            now = time.monotonic()
            if self.step():
                last = now
            elif now - last > self.config.idle_seconds or now - start > self.config.timeout_seconds:
                break
        # Un último repaso para que las vistas reflejen todo lo escrito.
        while self.step():
            pass
        return time.monotonic() - start

    # -- métricas ----------------------------------------------------------------------------
    def duplicates(self) -> dict[str, int]:
        out: Counter[str] = Counter()
        for attr, table in _SERVICE_EFFECTS.items():
            out[table] += sum(n - 1 for n in getattr(self.services, attr).executions.values() if n > 1)
        rows = self.conn.execute(
            "SELECT count(*) - count(DISTINCT order_id) AS extra FROM reservations"
        ).fetchone()
        self.conn.commit()
        out["reservations"] += rows["extra"]
        return {k: v for k, v in out.items() if v}

    def result(self, elapsed: float) -> RunResult:
        c = self.conn
        statuses = Counter(r["status"] for r in c.execute("SELECT status FROM view_incident_status").fetchall())
        latencies = [r["ms"] for r in c.execute(
            """
            SELECT extract(epoch FROM max(e.occurred_at) - min(e.occurred_at)) * 1000 AS ms
            FROM events e
            WHERE e.event_type IN ('CarrierNoticeReceived', 'IncidentResolved', 'IncidentEscalated')
            GROUP BY e.correlation_id
            HAVING count(*) FILTER (WHERE e.event_type <> 'CarrierNoticeReceived') > 0
            """
        ).fetchall()]
        cost = c.execute(
            "SELECT coalesce(sum(llm_calls), 0) AS calls, coalesce(sum(tokens_in), 0) AS tin, "
            "coalesce(sum(tokens_out), 0) AS tout FROM view_cost_per_incident"
        ).fetchone()
        c.commit()
        closed = statuses["resolved"] + statuses["escalated"]
        return RunResult(
            config=self.config, model=getattr(self.llm, "model", "unknown"), closed=closed,
            open=self.config.incidents - closed, resolved=statuses["resolved"], escalated=statuses["escalated"],
            duplicates=self.duplicates(), consumer_crashes=self.consumer_crashes,
            poison_messages=self.poison_messages, latencies_ms=[float(v) for v in latencies],
            llm_calls=int(cost["calls"]), tokens_in=int(cost["tin"]), tokens_out=int(cost["tout"]),
            elapsed_seconds=elapsed,
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
