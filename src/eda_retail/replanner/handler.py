"""Command handler del replanificador: `RevisePlan`, que invoca la saga por REST (design.md §3).

El comando lleva el plan vigente (cada paso con su estado y su versión de origen), el paso fallido, su
error y las revisiones hechas, así que el replanificador no lee el estado del orquestador ni la vista
`incident_status`. `prepare` (fuera de la transacción) consulta el lado de lectura (el presupuesto en
`cost_per_incident` y los importes de los pedidos del plan en `view_orders`) y propone la revisión:

- revisiones agotadas (`MAX_PLAN_REVISIONS`): escalado con `max_revisions`, sin LLM;
- en otro caso el LLM, con cada propuesta comprobada por `check_revision`; un rechazo se explica al LLM
  en la siguiente generación, dentro de los límites de llm-boundaries.

`RevisePlan` es un comando con trabajo largo (design.md §4.4): la petición solo lo acepta (`accept` rechaza
con `revision_exists` una versión del plan ya revisada o con un `RevisePlan` en curso) y responde 202; el
worker del replanificador hace `prepare` (el LLM) y `handle` después.

`handle` (en la transacción del trabajo) comprueba la revisión otra vez con las mismas reglas,
escribe `replanner.revisions` y `replanner.llm_calls` y publica `LlmCallRecorded` por llamada y
`PlanRevised` con `plan_version` + 1, o `IncidentEscalated` con el motivo cuando no hay plan válido. Si otro
`RevisePlan` de la misma versión se adelantó, el trabajo termina rechazado con `revision_exists`, sin eventos.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from decimal import Decimal
from types import SimpleNamespace
from typing import Any

import psycopg
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field

from eda_retail.config import Settings
from eda_retail.events.catalog import PlanStep, build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec, Rejected
from eda_retail.infra.db import connect
from eda_retail.llm.generation import Budget, LlmCall, LlmClient, LlmRequest, generate
from eda_retail.llm.prompts.replanner import MAX_REVISIONS, REPLACEABLE, SYSTEM, check_step_ids, revise_prompt
from eda_retail.llm.schemas import PlanProposal, strict_schema
from eda_retail.plan_validation import PlanRejected, validate_plan
from eda_retail.queries.cost_per_incident import Prices, budget_from_cost, get_incident_cost
from eda_retail.queries.orders import get_orders
from eda_retail.rules import BusinessRules

SCHEMA = "replanner"
REVISION_EXISTS = "revision_exists"


class CurrentStep(BaseModel):
    """Un paso del plan vigente tal como lo conoce la saga."""

    model_config = ConfigDict(extra="forbid")

    step_id: str
    action: str
    params: dict[str, Any]
    depends_on: list[str]
    status: str
    origin_plan_version: int = Field(ge=1)


class RevisePlan(BaseModel):
    """El plan vigente, el paso fallido y las revisiones hechas (design.md §3)."""

    model_config = ConfigDict(extra="forbid")

    plan_version: int = Field(ge=1)
    revisions: int = Field(ge=0)
    failed_step_id: str
    error: str
    steps: list[CurrentStep] = Field(min_length=1)

    def current(self) -> list[dict[str, Any]]:
        return [s.model_dump() for s in self.steps]

    def order_ids(self) -> list[str]:
        return sorted({str(s.params["order_id"]) for s in self.steps if s.params.get("order_id")})


@dataclass(frozen=True)
class RevisionContext:
    """Lo que el replanificador consulta antes de decidir: su propia tabla y el lado de lectura."""

    revised: bool
    budget: Budget
    amounts: dict[str, Decimal]  # importe de cada pedido del plan en view_orders


@dataclass(frozen=True)
class Prepared:
    revised: bool
    steps: list[dict[str, Any]] | None = None
    reason: str | None = None
    calls: list[LlmCall] = field(default_factory=list)
    amounts: dict[str, Decimal] = field(default_factory=dict)


def check_revision(current: list[dict[str, Any]], proposed: list[dict[str, Any]], rules: BusinessRules,
                   amounts: dict[str, Decimal]) -> list[PlanStep] | str:
    """Los pasos de la revisión validados, o el motivo del rechazo para explicárselo al LLM.

    Reglas en código (spec incident-planning): las de cualquier plan (`validate_plan`, con los pasos
    completados como dependencias posibles y para `refund_and_reissue_conflict`); un paso con efecto no
    vuelve (`completed_step_in_plan`); un paso mantenido no cambia de acción; un paso nuevo no repite uno
    abierto con otro `step_id` (`step_id_changed`)."""
    by_id = {s["step_id"]: s for s in current}
    completed = [s for s in current if s["status"] == "completed"]
    try:
        steps = validate_plan(SimpleNamespace(rules=rules), proposed, external_ids={s["step_id"] for s in completed},
                              completed=completed, order_amounts=amounts)
    except PlanRejected as exc:
        return f"{exc.reason} ({exc.detail})" if exc.detail else exc.reason
    for s in steps:
        old = by_id.get(s.step_id)
        if old is None:
            continue
        if old["status"] not in REPLACEABLE:
            return f"completed_step_in_plan (paso {s.step_id}, {old['status']})"
        if old["action"] != s.action:
            return f"invalid_plan ({s.step_id} cambia de acción: {old['action']} -> {s.action})"
    return check_step_ids(current, [s.model_dump(mode="json") for s in steps]) or steps


class RevisionPreparer:
    """Lo que se hace antes de abrir la transacción: consultas al lado de lectura y LLM."""

    def __init__(self, llm: LlmClient, rules: BusinessRules, *,
                 context: Callable[[str, int, list[str]], RevisionContext]) -> None:
        self._llm = llm
        self._rules = rules
        self._context = context

    def __call__(self, command: CommandEnvelope, payload: RevisePlan) -> Prepared:
        incident_id = command.correlation_id
        ctx = self._context(incident_id, payload.plan_version, payload.order_ids())
        if ctx.revised:
            return Prepared(revised=True)  # no se paga otra vez el LLM
        if payload.revisions >= self._rules.max_plan_revisions:
            return Prepared(False, reason=MAX_REVISIONS, amounts=ctx.amounts)
        current = payload.current()

        def check(plan: PlanProposal) -> str | None:
            result = check_revision(current, plan.command_steps(), self._rules, ctx.amounts)
            return result if isinstance(result, str) else None

        request = LlmRequest(
            agent=SCHEMA, incident_id=incident_id, system=SYSTEM, schema=strict_schema(PlanProposal),
            prompt=revise_prompt(payload.plan_version, current, payload.failed_step_id, payload.error),
        )
        gen = generate(self._llm, request, PlanProposal, max_generations=self._rules.max_generations,
                       budget=ctx.budget, check=check)
        steps = gen.value.command_steps() if gen.value is not None else None
        return Prepared(False, steps, gen.reason, gen.calls, ctx.amounts)


def _event(command: CommandEnvelope, event_type: str, payload: dict[str, Any]) -> EventEnvelope:
    return build_event(event_type, payload, correlation_id=command.correlation_id,
                       causation_id=command.command_id, producer=SCHEMA)


def _record_calls(conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> list[EventEnvelope]:
    events = []
    for call in calls:
        conn.execute(
            "INSERT INTO replanner.llm_calls (incident_id, command_id, model, tokens_in, tokens_out, latency_ms,"
            " validation_passed, attempt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (command.correlation_id, command.command_id, call.model, call.tokens_in, call.tokens_out,
             call.latency_ms, call.validation_passed, call.attempt),
        )
        events.append(_event(command, "LlmCallRecorded", {
            "incident_id": command.correlation_id, "agent": call.agent, "model": call.model,
            "tokens_in": call.tokens_in, "tokens_out": call.tokens_out, "latency_ms": call.latency_ms,
            "validation_passed": call.validation_passed, "attempt": call.attempt,
        }))
    return events


def revise_plan(rules: BusinessRules):
    def handle(conn: psycopg.Connection, command: CommandEnvelope, payload: RevisePlan,
               prepared: Prepared) -> Accepted | Rejected:
        if prepared.revised:
            return Rejected(REVISION_EXISTS)
        incident_id = command.correlation_id
        steps, reason = None, prepared.reason
        if prepared.steps is not None:
            checked = check_revision(payload.current(), prepared.steps, rules, prepared.amounts)
            steps, reason = (None, checked) if isinstance(checked, str) else (checked, None)
        payload_steps = [s.model_dump(mode="json") for s in steps] if steps is not None else None
        new_version = payload.plan_version + 1 if steps is not None else None
        inserted = conn.execute(
            "INSERT INTO replanner.revisions (incident_id, base_plan_version, command_id, plan_version,"
            " failed_step_id, error, steps, status, reason) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"
            " ON CONFLICT DO NOTHING",
            (incident_id, payload.plan_version, command.command_id, new_version, payload.failed_step_id,
             payload.error, Jsonb(payload_steps) if payload_steps is not None else None,
             "revised" if steps is not None else "escalated", reason),
        ).rowcount
        if inserted == 0:  # otro RevisePlan de la misma versión se adelantó
            return Rejected(REVISION_EXISTS)
        events = _record_calls(conn, command, prepared.calls)
        if payload_steps is not None:
            events.append(_event(command, "PlanRevised", {"incident_id": incident_id, "plan_version": new_version,
                                                          "steps": payload_steps, "reason": payload.error}))
            return Accepted(events, {"incident_id": incident_id, "outcome": "revised", "plan_version": new_version})
        events.append(_event(command, "IncidentEscalated", {"incident_id": incident_id, "reason": reason}))
        return Accepted(events, {"incident_id": incident_id, "outcome": "escalated", "reason": reason})

    return handle


def accept_revise_plan(conn: psycopg.Connection, command: CommandEnvelope, payload: RevisePlan) -> Rejected | None:
    """Aceptación: esa versión del plan no se revisó ni tiene un `RevisePlan` pendiente o en curso."""
    busy = conn.execute(
        "SELECT 1 AS found FROM replanner.revisions WHERE incident_id = %s AND base_plan_version = %s"
        " UNION ALL SELECT 1 FROM replanner.command_jobs WHERE correlation_id = %s AND command_type = 'RevisePlan'"
        " AND (payload->>'plan_version')::int = %s AND status IN ('pending', 'running') LIMIT 1",
        (command.correlation_id, payload.plan_version, command.correlation_id, payload.plan_version),
    ).fetchone()
    return Rejected(REVISION_EXISTS) if busy is not None else None


def read_context(settings: Settings, rules: BusinessRules) -> Callable[[str, int, list[str]], RevisionContext]:
    """Consultas de `prepare` con una conexión propia: su tabla `revisions` y el lado de lectura."""
    prices = Prices.from_settings(settings)

    def context(incident_id: str, base_version: int, order_ids: list[str]) -> RevisionContext:
        conn = connect(settings.database_url)
        try:
            with conn.transaction():
                revised = conn.execute(
                    "SELECT 1 FROM replanner.revisions WHERE incident_id = %s AND base_plan_version = %s",
                    (incident_id, base_version),
                ).fetchone() is not None
            budget = budget_from_cost(get_incident_cost(conn, incident_id, prices), rules)
            found = get_orders(conn, order_ids).found  # view_orders
            amounts = {o: found[o].amount for o in order_ids if o in found}
        finally:
            conn.close()
        return RevisionContext(revised, budget, amounts)

    return context


def specs(settings: Settings, llm: LlmClient, rules: BusinessRules) -> dict[str, CommandSpec]:
    prepare = RevisionPreparer(llm, rules, context=read_context(settings, rules))
    return {"RevisePlan": CommandSpec(RevisePlan, revise_plan(rules), prepare=prepare, deferred=True,
                                      accept=accept_revise_plan)}
