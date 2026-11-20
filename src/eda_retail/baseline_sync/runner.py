"""Base síncrona de referencia (spec experiments, «Base síncrona de referencia»; decisión D5).

La misma lógica de negocio que la versión de eventos, con llamadas directas: cada aviso se atiende de
principio a fin en la misma llamada. Reutiliza lo mismo que los componentes: los prompts y las
comprobaciones del intérprete, del planificador (`check_plan`) y del replanificador
(`check_revision`), el redactor, los servicios simulados, la misma `idempotency_key` y las mismas
compensaciones derivadas en código. Los prompts son los mismos que en la versión de eventos (los
pedidos, con su importe; el redactor, sin SKU ni cantidad), así E6 compara las dos con las mismas
entradas al LLM. No hay log, outbox, inbox, vistas ni Kafka.

Supuesto A8 (odd/tasks/experiments.md): el estado de negocio vive en PostgreSQL, en el esquema
`baseline_sync` de una base de datos propia por ejecución. Los pasos listos se ejecutan uno a uno en
orden (versión de origen, `step_id`). De los fallos inyectados se aplican los de negocio
(`fail_probability`, almacenes vacíos) y los duplicados, como el mismo aviso recibido varias veces; un
aviso repetido devuelve la incidencia ya creada.
"""

from __future__ import annotations

import contextlib
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any, Self

import psycopg

from eda_retail.baseline_sync.saga import FINAL, Step, apply_revision, ready_steps
from eda_retail.config import Settings
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.experiments.harness import RunConfig, isolated_database, seed_payload, service_duplicates
from eda_retail.experiments.oracle import Oracle
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.db import connect
from eda_retail.infra.effect_keys import compute_idempotency_key
from eda_retail.llm.generation import BUDGET_EXHAUSTED, Budget, LlmCall, LlmClient, LlmRequest, generate
from eda_retail.llm.prompts import interpreter, planner, replanner
from eda_retail.llm.prompts.writer import INVALID_DRAFT, DraftRequest, Writer
from eda_retail.llm.schemas import PlanProposal
from eda_retail.planner.handler import check_plan, order_lines
from eda_retail.replanner.handler import check_revision
from eda_retail.rules import BusinessRules
from eda_retail.step_executors.errors import REFUND_LIMIT, BusinessError, EffectError, Escalating
from eda_retail.step_executors.services import FakeService, Services
from eda_retail.synthetic.faults import INJECTED_FAILURE, should_fail
from eda_retail.synthetic.generator import Dataset, SyntheticNotice
from eda_retail.synthetic.generator import generate as generate_data

OUT_OF_STOCK = "out_of_stock"
NO_VALID_ORDERS = "no_valid_orders"
_DRAFTS = {"notify_customer": "customer_notice", "file_claim": "claim"}
_SERVICES = {"notify_customer": "mail", "file_claim": "claims", "reissue_shipment": "carrier",
             "issue_refund": "payments"}
# Fila del efecto de cada acción con servicio (SQL literal: la comprobación de propiedad lo lee).
_COLUMNS = " (%s, incident_id, order_id, step_id) VALUES (%%s, %%s, %%s, %%s)"
_EFFECT_ROWS = {
    "notify_customer": ("message_id", "INSERT INTO baseline_sync.customer_messages" + _COLUMNS % "message_id"),
    "file_claim": ("claim_id", "INSERT INTO baseline_sync.claims" + _COLUMNS % "claim_id"),
    "reissue_shipment": ("new_shipment_id", "INSERT INTO baseline_sync.shipments" + _COLUMNS % "shipment_id"),
}


class Escalate(Exception):
    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


@dataclass
class Incident:
    incident_id: str
    budget: Budget
    cause: str = "unknown"
    delay_hours: int = 0
    plan_version: int = 0
    steps: dict[str, Step] = field(default_factory=dict)
    calls: list[LlmCall] = field(default_factory=list)


class SyncSystem:
    def __init__(self, conn: psycopg.Connection, llm: LlmClient, services: Services, *, faults: Any,
                 rules: BusinessRules | None = None, max_attempts: int = 3, backoff: Backoff | None = None) -> None:
        self.conn, self.llm, self.rules = conn, llm, rules or BusinessRules()
        self.services = services
        self.writer = Writer(llm, max_generations=self.rules.max_generations)
        self._faults = faults
        self._max_attempts = max_attempts
        self._backoff = backoff or Backoff(0.01, 0.05)
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

    def _amounts(self, order_ids: list[str]) -> dict[str, Decimal]:
        with self.conn.transaction():
            rows = self.conn.execute("SELECT order_id, amount FROM baseline_sync.orders WHERE order_id = ANY(%s)",
                                     (order_ids,)).fetchall()
        found = {r["order_id"]: r["amount"] for r in rows}
        return {o: found[o] for o in order_ids if o in found}

    # -- una incidencia --------------------------------------------------------------------------
    def handle(self, notice: SyntheticNotice, fmt: str = "text") -> str:
        self.received.append(notice.carrier_notice_id)
        incident_id = incident_id_for_notice(notice.carrier_notice_id)
        with self.conn.transaction():
            row = self.conn.execute("SELECT status FROM baseline_sync.incidents WHERE incident_id = %s",
                                    (incident_id,)).fetchone()
        if row is not None:
            return row["status"]  # aviso repetido: la incidencia ya existe
        start = time.perf_counter()
        inc = Incident(incident_id, Budget(0, 0, self.rules.max_tokens_per_incident,
                                           self.rules.max_llm_calls_per_incident))
        with self.conn.transaction():
            self.conn.execute("INSERT INTO baseline_sync.incidents (incident_id, status) VALUES (%s, 'open')",
                              (incident_id,))
        try:
            outcome = self._process(inc, notice, fmt)
            status, reason = "resolved", outcome
        except Escalate as exc:
            status, reason = "escalated", exc.reason
        with self.conn.transaction():
            self.conn.execute(
                "UPDATE baseline_sync.incidents SET status = %s, plan_version = %s, reason = %s, closed_at = now()"
                " WHERE incident_id = %s", (status, inc.plan_version, reason, incident_id))
        self.latencies_ms.append((time.perf_counter() - start) * 1000)
        return status if status == "escalated" else reason

    def _process(self, inc: Incident, notice: SyntheticNotice, fmt: str) -> str:
        v = self._ask(inc, interpreter.notice_request(inc.incident_id, notice.carrier, notice.raw_text(fmt)),
                      interpreter.NoticeInterpretation, interpreter.check_interpretation)
        inc.cause, inc.delay_hours = v.cause.value, v.estimated_delay_hours
        amounts = self._amounts(v.order_ids)
        with self.conn.transaction():
            self.conn.execute("UPDATE baseline_sync.incidents SET severity = %s, cause = %s WHERE incident_id = %s",
                              (v.severity.value, v.cause.value, inc.incident_id))
        if not amounts:
            raise Escalate(NO_VALID_ORDERS)
        orders = list(amounts)
        if v.severity.value == "low":
            steps = planner.low_severity_plan(orders)
        else:
            request = planner.plan_request(inc.incident_id, v.cause.value, v.severity.value,
                                           v.estimated_delay_hours, order_lines(amounts))

            def check(plan: PlanProposal) -> str | None:
                result = check_plan(plan.command_steps(), self.rules, amounts)
                return result if isinstance(result, str) else None

            steps = self._ask(inc, request, PlanProposal, check).command_steps()
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
            if error.escalate:
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
        if should_fail(self._faults.seed, step.action, key, self._faults.fail_probability.get(step.action, 0.0)):
            return BusinessError(INJECTED_FAILURE)
        try:
            text = self._draft(inc, step)
            for attempt in range(1, self._max_attempts + 1):
                try:
                    with self.conn.transaction():
                        step.result = self._effect(inc, step, key, text)
                    return None
                except EffectError as exc:
                    if not (exc.retryable and attempt < self._max_attempts):
                        return exc
                    time.sleep(self._backoff.delay(attempt))
        except EffectError as exc:
            return exc
        return None  # pragma: no cover

    def _order(self, order_id: str) -> dict[str, Any]:
        row = self.conn.execute("SELECT * FROM baseline_sync.orders WHERE order_id = %s", (order_id,)).fetchone()
        if row is None:
            raise BusinessError("order_not_found")
        return row

    def _draft(self, inc: Incident, step: Step) -> str | None:
        """El borrador del redactor para las acciones redactadas (las mismas entradas que su ejecutor)."""
        kind = _DRAFTS.get(step.action)
        if kind is None:
            return None
        draft = self.writer.draft(DraftRequest(kind=kind, incident_id=inc.incident_id,  # type: ignore[arg-type]
                                               order_id=str(step.params.get("order_id")), sku=None, quantity=None,
                                               cause=inc.cause, estimated_delay_hours=inc.delay_hours), inc.budget)
        self._charge(inc, draft.calls)
        if draft.text is None:
            raise EffectError(draft.error or INVALID_DRAFT, retryable=draft.error != BUDGET_EXHAUSTED)
        return draft.text

    def _service(self, action: str) -> FakeService:
        return getattr(self.services, _SERVICES[action])

    def _effect(self, inc: Incident, step: Step, key: str, text: str | None) -> dict[str, Any]:
        c, order_id = self.conn, str(step.params.get("order_id"))
        if step.action == "reserve_stock":
            return self._reserve(inc, step, self._order(order_id))
        if step.action == "release_stock":
            return self._release(inc, step)
        if step.action == "issue_refund":
            amount = Decimal(str(step.params["amount"]))
            if amount > self.rules.max_refund_amount:
                raise Escalating(REFUND_LIMIT)
            self._order(order_id)
            refund_id = self._service(step.action).call(key)
            c.execute("INSERT INTO baseline_sync.refunds (refund_id, incident_id, order_id, step_id, amount)"
                      " VALUES (%s, %s, %s, %s, %s)", (refund_id, inc.incident_id, order_id, step.step_id, amount))
            return {"refund_id": refund_id}
        result_key, insert = _EFFECT_ROWS[step.action]
        self._order(order_id)
        effect_id = self._service(step.action).call(key)
        c.execute(insert, (effect_id, inc.incident_id, order_id, step.step_id))
        return {result_key: effect_id}

    def _reserve(self, inc: Incident, step: Step, order: dict[str, Any]) -> dict[str, Any]:
        row = self.conn.execute(
            """
            UPDATE baseline_sync.stock SET available = available - %s
            WHERE (warehouse, sku) = (
                SELECT warehouse, sku FROM baseline_sync.stock WHERE sku = %s AND available >= %s
                ORDER BY warehouse LIMIT 1 FOR UPDATE
            )
            RETURNING warehouse
            """,
            (order["quantity"], order["sku"], order["quantity"]),
        ).fetchone()
        if row is None:
            raise BusinessError(OUT_OF_STOCK)
        reservation_id = f"R-{uuid.uuid4().hex[:8]}"
        self.conn.execute(
            "INSERT INTO baseline_sync.reservations (reservation_id, incident_id, step_id, order_id, warehouse, sku,"
            " quantity, status) VALUES (%s, %s, %s, %s, %s, %s, %s, 'active')",
            (reservation_id, inc.incident_id, step.step_id, order["order_id"], row["warehouse"], order["sku"],
             order["quantity"]))
        return {"reservation_id": reservation_id, "warehouse": row["warehouse"]}

    def _release(self, inc: Incident, step: Step) -> dict[str, Any]:
        row = self.conn.execute(
            "SELECT * FROM baseline_sync.reservations WHERE reservation_id = %s FOR UPDATE",
            (step.params.get("reservation_id"),)).fetchone()
        if row is None or row["status"] != "active":
            return {"released": False}
        self.conn.execute("UPDATE baseline_sync.reservations SET status = 'released' WHERE reservation_id = %s",
                          (row["reservation_id"],))
        self.conn.execute("UPDATE baseline_sync.stock SET available = available + %s WHERE warehouse = %s AND sku = %s",
                          (row["quantity"], row["warehouse"], row["sku"]))
        return {"released": True, "reservation_id": row["reservation_id"]}

    def _replan(self, inc: Incident, failed: Step) -> None:
        if inc.plan_version - 1 >= self.rules.max_plan_revisions:
            raise Escalate(replanner.MAX_REVISIONS)
        current = [s.view() for s in inc.steps.values()]
        amounts = self._amounts(sorted({str(s.params["order_id"]) for s in inc.steps.values()
                                        if s.params.get("order_id")}))

        def check(plan: PlanProposal) -> str | None:
            result = check_revision(current, plan.command_steps(), self.rules, amounts)
            return result if isinstance(result, str) else None

        proposal = self._ask(inc, replanner.revise_request(
            inc.incident_id, inc.plan_version, current, failed.step_id, failed.error or "",
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
    log_rows: int  # tablas de log, outbox o inbox en su esquema: 0 en la base síncrona
    escalation_reasons: dict[str, int] = field(default_factory=dict)


def seed(conn: psycopg.Connection, data: Dataset, empty: tuple[str, ...] = ()) -> None:
    """Pedidos y existencias del conjunto en `baseline_sync` (los almacenes vacíos, a 0)."""
    payload = seed_payload(data, empty)
    with conn.transaction():
        for o in payload["orders"]:
            conn.execute("INSERT INTO baseline_sync.orders (order_id, customer_ref, sku, quantity, amount, carrier)"
                         " VALUES (%(order_id)s, %(customer_ref)s, %(sku)s, %(quantity)s, %(amount)s, %(carrier)s)"
                         " ON CONFLICT DO NOTHING", o)
        for s in payload["stock"]:
            conn.execute("INSERT INTO baseline_sync.stock (warehouse, sku, available)"
                         " VALUES (%(warehouse)s, %(sku)s, %(available)s) ON CONFLICT DO NOTHING", s)


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
        seed(self.conn, self.data, tuple(self.config.faults.empty_warehouses))
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

    def duplicates(self) -> dict[str, int]:
        out = service_duplicates(self.services)
        with self.conn.transaction():
            row = self.conn.execute(
                "SELECT count(*) - count(DISTINCT order_id) AS extra FROM baseline_sync.reservations").fetchone()
        out["reserve_stock"] += row["extra"]
        return {k: v for k, v in out.items() if v}

    def result(self) -> SyncResult:
        c = self.conn
        with c.transaction():
            rows = c.execute("SELECT status, reason FROM baseline_sync.incidents").fetchall()
            refunded = c.execute("SELECT count(DISTINCT incident_id) AS n FROM baseline_sync.refunds").fetchone()["n"]
            log_rows = c.execute(
                "SELECT count(*) AS n FROM information_schema.tables WHERE table_schema = 'baseline_sync'"
                " AND table_name IN ('events', 'outbox', 'inbox')").fetchone()["n"]
        statuses = [r["status"] for r in rows]
        resolved, escalated = statuses.count("resolved"), statuses.count("escalated")
        reasons = Counter(r["reason"] or "unknown" for r in rows if r["status"] == "escalated")
        calls = self.system.calls
        return SyncResult(
            config=self.config, model=getattr(self.llm, "model", "unknown"), closed=resolved + escalated,
            open=self.config.incidents - resolved - escalated, resolved=resolved, refunded=refunded,
            escalated=escalated, duplicates=self.duplicates(),
            latencies_ms=list(self.system.latencies_ms), llm_calls=len(calls),
            tokens_in=sum(x.tokens_in for x in calls), tokens_out=sum(x.tokens_out for x in calls),
            elapsed_seconds=self.elapsed, received=list(self.system.received),
            injected_failures=set(self.system.injected_failures), log_rows=log_rows,
            escalation_reasons=dict(sorted(reasons.items())),
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
