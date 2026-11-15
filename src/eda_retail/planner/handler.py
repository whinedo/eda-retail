"""Command handler del planificador: `CreatePlan`, que invoca la saga por REST (design.md §3).

El comando lleva la incidencia (pedidos, causa, retraso y gravedad), así que el planificador no lee
el estado del orquestador. `prepare` (fuera de la transacción) consulta el lado de lectura (el
presupuesto en `cost_per_incident` y los importes en `view_orders`) y propone el plan:

- `severity` `low`: el plan fijo de `agents.planner`, un `notify_customer` por pedido, sin LLM;
- en otro caso, con `POC_VERSION=2`, primero consulta al agente del transportista por A2A
  (`planner.carrier`, spec a2a-carrier) y, si la respuesta supera los guardrails, la pasa al prompt como
  contenido no confiable; con la versión 1, o si la consulta falla, el prompt es el de siempre;
- después el LLM, con cada propuesta comprobada por `validate_plan` (acciones permitidas,
  compensaciones, dependencias, tope de reembolso y el importe del pedido); un rechazo se explica
  al LLM en la siguiente generación, dentro de los límites de llm-boundaries.

`CreatePlan` es un comando con trabajo largo (design.md §4.4): la petición solo lo acepta (`accept`
rechaza con `plan_exists` una incidencia que ya tiene plan o un `CreatePlan` en curso) y responde 202;
el worker del planificador hace `prepare` (el LLM) y `handle` después.

`handle` (en la transacción del trabajo) valida el plan otra vez con las mismas reglas,
escribe `planner.plans` y `planner.llm_calls` y publica `LlmCallRecorded` por llamada y
`PlanCreated`, o `IncidentEscalated` con el motivo cuando no hay plan válido; si hubo consulta A2A, antes
publica un `CarrierConsulted` con su resultado (sin `notes`). Si otro `CreatePlan` de
la misma incidencia se adelantó, el trabajo termina rechazado con `plan_exists`, sin eventos.
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
from eda_retail.events.catalog import Cause, PlanStep, Severity, build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec, Rejected
from eda_retail.infra.db import connect
from eda_retail.llm.generation import Budget, LlmCall, LlmClient, generate
from eda_retail.llm.prompts.planner import low_severity_plan, plan_request
from eda_retail.llm.schemas import PlanProposal
from eda_retail.plan_validation import PlanRejected, validate_plan
from eda_retail.planner.carrier import CarrierConsultant, CarrierConsultation, render_options
from eda_retail.queries.cost_per_incident import Prices, budget_from_cost, get_incident_cost
from eda_retail.queries.orders import get_orders
from eda_retail.rules import BusinessRules

SCHEMA = "planner"
PLAN_EXISTS = "plan_exists"


class CreatePlan(BaseModel):
    """La incidencia que hay que planificar, tal como la conoce la saga."""

    model_config = ConfigDict(extra="forbid")

    order_ids: list[str] = Field(min_length=1)
    cause: Cause
    estimated_delay_hours: int = Field(ge=0)
    severity: Severity


@dataclass(frozen=True)
class PlanContext:
    """Lo que el planificador consulta antes de decidir: su propia tabla y el lado de lectura."""

    planned: bool
    budget: Budget
    amounts: dict[str, Decimal]  # importe de cada pedido en view_orders


@dataclass(frozen=True)
class Prepared:
    planned: bool
    steps: list[dict[str, Any]] | None = None
    reason: str | None = None
    calls: list[LlmCall] = field(default_factory=list)
    amounts: dict[str, Decimal] = field(default_factory=dict)
    carrier: CarrierConsultation | None = None  # la consulta A2A, si la hubo (POC_VERSION=2)


def check_plan(steps: list[dict[str, Any]], rules: BusinessRules,
               amounts: dict[str, Decimal]) -> list[PlanStep] | str:
    """Los pasos validados, o el motivo del rechazo (con su detalle) para explicárselo al LLM."""
    try:
        return validate_plan(SimpleNamespace(rules=rules), steps, external_ids=set(), order_amounts=amounts)
    except PlanRejected as exc:
        return f"{exc.reason} ({exc.detail})" if exc.detail else exc.reason


def order_lines(amounts: dict[str, Decimal]) -> str:
    """Una línea por pedido con su importe (view_orders no tiene SKU ni cantidad, ni datos personales)."""
    return "\n".join(f"- {order}: importe {amount}" for order, amount in amounts.items())


class PlanPreparer:
    """Lo que se hace antes de abrir la transacción: consultas al lado de lectura y LLM."""

    def __init__(self, llm: LlmClient, rules: BusinessRules, *,
                 context: Callable[[str, list[str]], PlanContext], carrier: CarrierConsultant | None = None) -> None:
        self._llm = llm
        self._rules = rules
        self._context = context
        self._carrier = carrier  # None: versión 1, no se consulta al transportista

    def __call__(self, command: CommandEnvelope, payload: CreatePlan) -> Prepared:
        incident_id = command.correlation_id
        ctx = self._context(incident_id, payload.order_ids)
        if ctx.planned:
            return Prepared(planned=True)  # no se paga otra vez el LLM
        if payload.severity == Severity.LOW:
            return Prepared(False, low_severity_plan(payload.order_ids), amounts=ctx.amounts)

        def check(plan: PlanProposal) -> str | None:
            result = check_plan(plan.command_steps(), self._rules, ctx.amounts)
            return result if isinstance(result, str) else None

        consultation = None
        if self._carrier is not None:  # A2A antes del LLM, fuera de la transacción (regla 6)
            consultation = self._carrier.consult(payload.order_ids[0], payload.cause.value, incident_id=incident_id)
        options = render_options(consultation.options) if consultation and consultation.options else None
        request = plan_request(incident_id, payload.cause.value, payload.severity.value,
                               payload.estimated_delay_hours, order_lines(ctx.amounts), carrier_options=options)
        gen = generate(self._llm, request, PlanProposal, max_generations=self._rules.max_generations,
                       budget=ctx.budget, check=check)
        steps = gen.value.command_steps() if gen.value is not None else None
        return Prepared(False, steps, gen.reason, gen.calls, ctx.amounts, consultation)


def _event(command: CommandEnvelope, event_type: str, payload: dict[str, Any]) -> EventEnvelope:
    return build_event(event_type, payload, correlation_id=command.correlation_id,
                       causation_id=command.command_id, producer=SCHEMA)


def _record_calls(conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> list[EventEnvelope]:
    events = []
    for call in calls:
        conn.execute(
            "INSERT INTO planner.llm_calls (incident_id, command_id, model, tokens_in, tokens_out, latency_ms,"
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


def create_plan(rules: BusinessRules):
    def handle(conn: psycopg.Connection, command: CommandEnvelope, payload: CreatePlan,
               prepared: Prepared) -> Accepted | Rejected:
        if prepared.planned:
            return Rejected(PLAN_EXISTS)
        incident_id = command.correlation_id
        steps, reason = None, prepared.reason
        if prepared.steps is not None:
            checked = check_plan(prepared.steps, rules, prepared.amounts)
            steps, reason = (None, checked) if isinstance(checked, str) else (checked, None)
        payload_steps = [s.model_dump(mode="json") for s in steps] if steps is not None else None
        inserted = conn.execute(
            "INSERT INTO planner.plans (incident_id, command_id, plan_version, steps, status, reason)"
            " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
            (incident_id, command.command_id, 1 if steps is not None else None,
             Jsonb(payload_steps) if payload_steps is not None else None,
             "created" if steps is not None else "escalated", reason),
        ).rowcount
        if inserted == 0:  # otro CreatePlan de la misma incidencia se adelantó
            return Rejected(PLAN_EXISTS)
        events = []
        if prepared.carrier is not None:
            events.append(_event(command, "CarrierConsulted", prepared.carrier.event_payload(incident_id)))
        events += _record_calls(conn, command, prepared.calls)
        if payload_steps is not None:
            events.append(_event(command, "PlanCreated", {"incident_id": incident_id, "plan_version": 1,
                                                          "steps": payload_steps}))
            return Accepted(events, {"incident_id": incident_id, "outcome": "created", "plan_version": 1})
        events.append(_event(command, "IncidentEscalated", {"incident_id": incident_id, "reason": reason}))
        return Accepted(events, {"incident_id": incident_id, "outcome": "escalated", "reason": reason})

    return handle


def accept_create_plan(conn: psycopg.Connection, command: CommandEnvelope, payload: CreatePlan) -> Rejected | None:
    """Aceptación: la incidencia no tiene plan ni un `CreatePlan` pendiente o en curso."""
    busy = conn.execute(
        "SELECT 1 AS found FROM planner.plans WHERE incident_id = %s"
        " UNION ALL SELECT 1 FROM planner.command_jobs WHERE correlation_id = %s AND command_type = 'CreatePlan'"
        " AND status IN ('pending', 'running') LIMIT 1",
        (command.correlation_id, command.correlation_id),
    ).fetchone()
    return Rejected(PLAN_EXISTS) if busy is not None else None


def read_context(settings: Settings, rules: BusinessRules) -> Callable[[str, list[str]], PlanContext]:
    """Consultas de `prepare` con una conexión propia: su tabla `plans` y el lado de lectura."""
    prices = Prices.from_settings(settings)

    def context(incident_id: str, order_ids: list[str]) -> PlanContext:
        conn = connect(settings.database_url)
        try:
            with conn.transaction():
                planned = conn.execute("SELECT 1 FROM planner.plans WHERE incident_id = %s",
                                       (incident_id,)).fetchone() is not None
            budget = budget_from_cost(get_incident_cost(conn, incident_id, prices), rules)
            found = get_orders(conn, order_ids).found  # view_orders
            amounts = {o: found[o].amount for o in order_ids if o in found}
        finally:
            conn.close()
        return PlanContext(planned, budget, amounts)

    return context


def specs(settings: Settings, llm: LlmClient, rules: BusinessRules, *,
          carrier: CarrierConsultant | None = None) -> dict[str, CommandSpec]:
    """Sin `carrier` (versión 1, el harness de experimentos) el planificador no consulta al transportista."""
    prepare = PlanPreparer(llm, rules, context=read_context(settings, rules), carrier=carrier)
    return {"CreatePlan": CommandSpec(CreatePlan, create_plan(rules), prepare=prepare, deferred=True,
                                      accept=accept_create_plan)}
