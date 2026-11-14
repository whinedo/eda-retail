"""Base síncrona de referencia (spec experiments, «Base síncrona de referencia»; decisión D5).

La misma lógica de negocio que la versión de eventos, con llamadas directas: cada aviso se
atiende de principio a fin en la misma llamada. Reutiliza los prompts y las comprobaciones
del intérprete, el planificador y el replanner, las reglas del plan (`validate_plan`), las
acciones de los ejecutores con sus servicios simulados, la misma `idempotency_key` y las
mismas compensaciones derivadas en código. No hay `events`, outbox, inbox, vistas ni Kafka.

Supuesto A8 (odd/tasks/experiments.md): el estado de negocio vive en PostgreSQL, en una base
de datos propia por ejecución (`orders`, `stock`, `reservations`, tablas de efectos e
`incidents`), así E6 compara las dos versiones con la misma base de datos. Los pasos listos
se ejecutan uno a uno en orden (versión de origen, `step_id`). De los fallos inyectados se
aplican los de negocio (`fail_probability`, almacenes vacíos) y los duplicados, como el mismo
aviso recibido varias veces; un aviso repetido devuelve la incidencia ya creada.
"""

from __future__ import annotations

import contextlib
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Self

import psycopg

from eda_retail.agents import interpreter, planner, replanner
from eda_retail.agents.prompts import format_orders
from eda_retail.baseline_sync.saga import FINAL, Step, apply_revision, ready_steps
from eda_retail.command.handler import CommandHandler, Rejected, incident_id_for_notice
from eda_retail.command.plans import validate_plan
from eda_retail.command.rules import BusinessRules
from eda_retail.config import Settings
from eda_retail.executors.actions import build_actions
from eda_retail.executors.base import EffectError, StepRequest
from eda_retail.executors.retry import Backoff
from eda_retail.executors.services import Services
from eda_retail.experiments.harness import RunConfig, effect_duplicates, isolated_database
from eda_retail.experiments.oracle import Oracle
from eda_retail.infra.db import connect
from eda_retail.infra.effect_keys import compute_idempotency_key
from eda_retail.llm.generation import Budget, LlmCall, LlmClient, LlmRequest, generate
from eda_retail.llm.schemas import PlanProposal, strict_schema
from eda_retail.queries.incident_status import IncidentStatus
from eda_retail.synthetic.faults import INJECTED_FAILURE, FailingAction, empty_warehouses
from eda_retail.synthetic.generator import Dataset, SyntheticNotice, load
from eda_retail.synthetic.generator import generate as generate_data


class Escalate(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class Incident:
    incident_id: str
    budget: Budget
    plan_version: int = 0
    steps: dict[str, Step] = field(default_factory=dict)
    calls: list[LlmCall] = field(default_factory=list)


class SyncSystem:
    def __init__(self, conn: psycopg.Connection, llm: LlmClient, services: Services, *, faults: Any,
                 rules: BusinessRules | None = None, max_attempts: int = 3, backoff: Backoff | None = None) -> None:
        self.conn, self.llm, self.rules = conn, llm, rules or BusinessRules()
        self._ctx = SimpleNamespace(rules=self.rules)  # validate_plan solo lee las reglas
        self._max_attempts = max_attempts
        self._backoff = backoff or Backoff(0.01, 0.05)
        actions = build_actions(llm, CommandHandler(self.rules), services)
        self.actions = {name: FailingAction(a, faults) if faults.fail_probability.get(name) else a
                        for name, a in actions.items()}
        self.received: list[str] = []
        self.injected_failures: set[tuple[str, str]] = set()
        self.calls: list[LlmCall] = []
        self.latencies_ms: list[float] = []

    # -- llamadas al LLM -----------------------------------------------------------------------
    def _ask(self, inc: Incident, request: LlmRequest, output: type, check: Any) -> Any:
        gen = generate(self.llm, request, output, max_generations=self.rules.max_generations,
                       budget=inc.budget, check=check)
        self._charge(inc, gen.calls)
        if gen.value is None:
            raise Escalate(gen.reason)
        return gen.value

    def _charge(self, inc: Incident, calls: list[LlmCall]) -> None:
        for c in calls:
            inc.budget = inc.budget.charge(c)
        inc.calls += calls
        self.calls += calls
        with self.conn.transaction():  # el redactor lee el presupuesto de `incidents`
            self.conn.execute("UPDATE incidents SET tokens_used = %s, llm_calls = %s WHERE incident_id = %s",
                              (inc.budget.tokens_used, inc.budget.llm_calls, inc.incident_id))

    def _rejection(self, steps: list[dict], **kw: Any) -> str | None:
        try:
            validate_plan(self._ctx, steps, **kw)
        except Rejected as exc:
            return f"{exc.reason} ({exc.detail})" if exc.detail else exc.reason
        return None

    # -- una incidencia --------------------------------------------------------------------------
    def handle(self, notice: SyntheticNotice, fmt: str = "text") -> str:
        self.received.append(notice.carrier_notice_id)
        incident_id = incident_id_for_notice(notice.carrier_notice_id)
        with self.conn.transaction():
            row = self.conn.execute("SELECT status FROM incidents WHERE incident_id = %s", (incident_id,)).fetchone()
        if row is not None:
            return row["status"]  # aviso repetido: la incidencia ya existe
        start = time.perf_counter()
        inc = Incident(incident_id, Budget(0, 0, self.rules.max_tokens_per_incident,
                                           self.rules.max_llm_calls_per_incident))
        with self.conn.transaction():
            self.conn.execute("INSERT INTO incidents (incident_id, status, severity, cause) "
                              "VALUES (%s, 'open', 'unknown', 'unknown')", (incident_id,))
        try:
            outcome = self._process(inc, notice, fmt)
            status, reason = "resolved", outcome
        except Escalate as exc:
            status, reason = "escalated", exc.reason
        with self.conn.transaction():
            self.conn.execute("UPDATE incidents SET status = %s, plan_version = %s, closed_at = now() "
                              "WHERE incident_id = %s", (status, inc.plan_version, incident_id))
        self.latencies_ms.append((time.perf_counter() - start) * 1000)
        return status if status == "escalated" else reason

    def _process(self, inc: Incident, notice: SyntheticNotice, fmt: str) -> str:
        v = self._ask(inc, LlmRequest(
            agent="interpreter", incident_id=inc.incident_id, system=interpreter.SYSTEM,
            prompt=f"Transportista: {notice.carrier}\n<aviso>\n{notice.raw_text(fmt)}\n</aviso>",
            schema=strict_schema(interpreter.NoticeInterpretation), max_tokens=2_000,
        ), interpreter.NoticeInterpretation, interpreter.check_interpretation)
        with self.conn.transaction():
            known = {r["order_id"] for r in self.conn.execute(
                "SELECT order_id FROM orders WHERE order_id = ANY(%s)", (v.order_ids,))}
            self.conn.execute("UPDATE incidents SET severity = %s, cause = %s WHERE incident_id = %s",
                              (v.severity.value, v.cause.value, inc.incident_id))
            orders = [o for o in v.order_ids if o in known]
            text = format_orders(self.conn, orders) if orders else ""
        if not orders:
            raise Escalate("no_valid_orders")
        if v.severity.value == "low":
            steps = planner.low_severity_plan(orders)
        else:
            request = planner.plan_request(inc.incident_id, v.cause.value, v.severity.value,
                                           v.estimated_delay_hours, text)
            steps = self._ask(inc, request, PlanProposal,
                              lambda p: self._rejection(p.command_steps(), external_ids=set())).command_steps()
        inc.plan_version = 1
        inc.steps = {s["step_id"]: Step(s["step_id"], s["action"], s["params"], list(s["depends_on"]), 1)
                     for s in steps}
        return self._run_plan(inc)

    # -- el plan y la saga -----------------------------------------------------------------------
    def _run_plan(self, inc: Incident) -> str:
        while ready := ready_steps(inc.steps):
            step = ready[0]
            if step.compensates:
                inc.steps[step.compensates].status = "compensating"
            error = self._execute(inc, step)
            if error is None:
                step.status = "completed"
                if step.compensates:
                    inc.steps[step.compensates].status = "compensated"
                continue
            step.status, step.error = "failed", error.error
            if error.error == INJECTED_FAILURE:
                self.injected_failures.add((inc.incident_id, step.step_id))
            if step.compensates:
                raise Escalate("compensation_failed")
            if getattr(error, "escalate", None):
                raise Escalate(error.escalate)
            if error.retryable:
                raise Escalate("retries_exhausted")
            self._replan(inc, step)
        if not all(s.status in FINAL for s in inc.steps.values()):
            raise Escalate("plan_blocked")
        refunded = any(s.action == "issue_refund" and s.status == "completed" for s in inc.steps.values())
        return "refunded" if refunded else "resolved"

    def _execute(self, inc: Incident, step: Step) -> EffectError | None:
        key = compute_idempotency_key(inc.incident_id, step.step_id, origin_plan_version=step.origin)
        request = StepRequest(None, inc.incident_id, inc.plan_version, step.step_id, step.action,  # type: ignore[arg-type]
                              step.params, key)
        action = self.actions[step.action]
        for attempt in range(1, self._max_attempts + 1):
            try:
                prepared = action.prepare(self.conn, request)
                self._charge(inc, prepared.calls)
                if prepared.error is not None:
                    raise prepared.error
                with self.conn.transaction():
                    step.result = action.execute(self.conn, request, prepared).result
                return None
            except EffectError as exc:
                if not (exc.retryable and attempt < self._max_attempts):
                    return exc
                time.sleep(self._backoff.delay(attempt))
        return None  # pragma: no cover

    def _replan(self, inc: Incident, failed: Step) -> None:
        if inc.plan_version - 1 >= self.rules.max_plan_revisions:
            raise Escalate(replanner.MAX_REVISIONS)
        status = IncidentStatus(inc.incident_id, "open", inc.plan_version, [s.view() for s in inc.steps.values()],
                                None, {})
        completed = [s for s in inc.steps.values() if s.status == "completed"]

        def check(plan: PlanProposal) -> str | None:
            steps = plan.command_steps()
            for s in steps:
                if s["step_id"] in {c.step_id for c in completed}:
                    return f"completed_step_in_plan (paso {s['step_id']})"
                old = inc.steps.get(s["step_id"])
                if old is not None and old.action != s["action"]:
                    return f"invalid_plan ({s['step_id']} cambia de acción)"
            return replanner.check_step_ids(status, plan) or self._rejection(
                steps, external_ids={c.step_id for c in completed},
                completed=[{"action": c.action, "params": c.params} for c in completed])

        proposal = self._ask(inc, LlmRequest(
            agent="replanner", incident_id=inc.incident_id, system=replanner.SYSTEM,
            schema=strict_schema(PlanProposal),
            prompt=(f"Plan vigente (versión {inc.plan_version}):\n{replanner.render_plan(status)}\n\n"
                    f"Fallo: el paso {failed.step_id} terminó con el error {failed.error} y no se puede reintentar."),
        ), PlanProposal, check)
        inc.plan_version += 1
        apply_revision(inc.steps, proposal.command_steps(), inc.plan_version)



# -- una ejecución aislada, como el arnés de la versión de eventos -------------------------------
@dataclass
class SyncResult:
    config: RunConfig
    model: str
    closed: int
    open: int
    resolved: int
    refunded: int
    escalated: int
    duplicates: dict[str, int]
    latencies_ms: list[float]
    llm_calls: int
    tokens_in: int
    tokens_out: int
    elapsed_seconds: float
    received: list[str]
    injected_failures: set[tuple[str, str]]
    log_rows: int  # filas en events, outbox e inbox: 0 en la base síncrona


class SyncRun:
    def __init__(self, base: Settings, config: RunConfig, *, llm: LlmClient | None = None,
                 data: Dataset | None = None) -> None:
        self.config = config
        self.data = data or generate_data(config.seed, config.incidents)
        self.llm = llm or Oracle(self.data).client()
        self.services = Services()
        self._base = base
        self.elapsed = 0.0

    def __enter__(self) -> Self:
        self._database = isolated_database(self._base, f"sync_{uuid.uuid4().hex[:10]}")
        self.conn = connect(self._database.__enter__())
        load(self.conn, self.data)
        if self.config.faults.empty_warehouses:
            empty_warehouses(self.conn, self.config.faults.empty_warehouses)
        self.conn.commit()
        self.system = SyncSystem(self.conn, self.llm, self.services, faults=self.config.faults,
                                 max_attempts=self._base.step_max_attempts)
        return self

    def __exit__(self, *exc: object) -> None:
        with contextlib.suppress(Exception):
            self.conn.close()
        self._database.__exit__(None, None, None)

    def run(self) -> None:
        start = time.monotonic()
        for notice in self.data.notices:
            for _ in range(self.config.faults.duplicates):
                self.system.handle(notice, self.config.fmt)
        self.elapsed = time.monotonic() - start

    def result(self) -> SyncResult:
        c = self.conn
        with c.transaction():
            rows = c.execute("SELECT status FROM incidents").fetchall()
            refunded = c.execute("SELECT count(DISTINCT incident_id) AS n FROM refunds").fetchone()["n"]
            log_rows = c.execute("SELECT (SELECT count(*) FROM events) + (SELECT count(*) FROM outbox) "
                                 "+ (SELECT count(*) FROM inbox) AS n").fetchone()["n"]
        statuses = [r["status"] for r in rows]
        resolved, escalated = statuses.count("resolved"), statuses.count("escalated")
        calls = self.system.calls
        return SyncResult(
            config=self.config, model=getattr(self.llm, "model", "unknown"), closed=resolved + escalated,
            open=self.config.incidents - resolved - escalated, resolved=resolved, refunded=refunded,
            escalated=escalated, duplicates=effect_duplicates(c, self.services),
            latencies_ms=list(self.system.latencies_ms), llm_calls=len(calls),
            tokens_in=sum(x.tokens_in for x in calls), tokens_out=sum(x.tokens_out for x in calls),
            elapsed_seconds=self.elapsed, received=list(self.system.received),
            injected_failures=set(self.system.injected_failures), log_rows=log_rows,
        )


@contextlib.contextmanager
def sync_started(base: Settings, config: RunConfig, **kw: Any) -> Iterator[SyncRun]:
    """Base síncrona con todos los avisos ya atendidos."""
    with SyncRun(base, config, **kw) as run:
        run.run()
        yield run


def run_sync(base: Settings, config: RunConfig, **kw: Any) -> SyncResult:
    with sync_started(base, config, **kw) as run:
        return run.result()
