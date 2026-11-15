"""Command handler del intérprete: `OpenIncident`, que deriva de `CarrierNoticeReceived` (design.md §3).

`prepare` (fuera de la transacción) llama al LLM y hace las consultas al lado de lectura: el
presupuesto en `cost_per_incident` y los pedidos en `view_orders`. `handle` (en la transacción de
`execute_command`) escribe el estado del intérprete y sus eventos:

- cada llamada al LLM en `interpreter.llm_calls` y como `LlmCallRecorded`;
- con pedidos en `view_orders`, `ShipmentDelayed` con esos pedidos (spec incident-interpretation,
  "Pedidos conocidos");
- sin ninguno, aparca el aviso en `interpreter.pending_notices` con un plazo (spec cqrs-views,
  "Proyección atrasada"): lo reanuda el `OrderRegistered` del pedido o lo resuelve su plazo;
- sin interpretación válida, `IncidentEscalated` con el motivo de la generación.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import psycopg
from pydantic import BaseModel, ConfigDict, Field

from eda_retail.config import Settings
from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.commands import Accepted, CommandEnvelope, Rejected
from eda_retail.llm.generation import Budget, LlmCall, LlmClient, generate
from eda_retail.llm.prompts.interpreter import NoticeInterpretation, check_interpretation, notice_request
from eda_retail.rules import BusinessRules

SCHEMA = "interpreter"
DUPLICATE_NOTICE = "duplicate_notice"
NO_VALID_ORDERS = "no_valid_orders"


class OpenIncident(BaseModel):
    """El aviso original, tal como lo publicó `intake`."""

    model_config = ConfigDict(extra="forbid")

    carrier_notice_id: str = Field(min_length=1)
    carrier: str = Field(min_length=1)
    raw_text: str = Field(min_length=1)


@dataclass(frozen=True)
class Prepared:
    duplicate: bool
    interpretation: NoticeInterpretation | None = None
    reason: str | None = None
    calls: list[LlmCall] = field(default_factory=list)
    known_orders: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Decision:
    kind: str  # delayed | parked | escalated
    order_ids: list[str]
    reason: str | None


def decide(value: NoticeInterpretation | None, reason: str | None, *, known_orders: list[str]) -> Decision:
    if value is None:
        return Decision("escalated", [], reason)
    known = [o for o in value.order_ids if o in known_orders]
    if known:
        return Decision("delayed", known, None)
    return Decision("parked", list(value.order_ids), None)


class NoticePreparer:
    """Lo que se hace antes de abrir la transacción: LLM y consultas a las proyecciones."""

    def __init__(
        self,
        llm: LlmClient,
        rules: BusinessRules,
        *,
        interpreted: Callable[[str], bool],
        budget: Callable[[str], Budget],
        known_orders: Callable[[list[str]], list[str]],
    ) -> None:
        self._llm = llm
        self._rules = rules
        self._interpreted = interpreted
        self._budget = budget
        self._known_orders = known_orders

    def __call__(self, command: CommandEnvelope, payload: OpenIncident) -> Prepared:
        if self._interpreted(payload.carrier_notice_id):
            return Prepared(duplicate=True)  # no se paga otra vez el LLM
        incident_id = command.correlation_id
        gen = generate(
            self._llm, notice_request(incident_id, payload.carrier, payload.raw_text), NoticeInterpretation,
            max_generations=self._rules.max_generations, budget=self._budget(incident_id),
            check=check_interpretation,
        )
        known = self._known_orders(gen.value.order_ids) if gen.value is not None else []
        return Prepared(False, gen.value, gen.reason, gen.calls, known)


def event(command: CommandEnvelope, event_type: str, payload: dict[str, Any]) -> EventEnvelope:
    return build_event(event_type, payload, correlation_id=command.correlation_id,
                       causation_id=command.command_id, producer=SCHEMA)


def shipment_delayed(incident_id: str, order_ids: list[str], row: dict[str, Any]) -> dict[str, Any]:
    return {"incident_id": incident_id, "order_ids": order_ids, "cause": row["cause"],
            "estimated_delay_hours": row["estimated_delay_hours"], "severity": row["severity"]}


def _record_calls(conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> list[EventEnvelope]:
    events = []
    for call in calls:
        conn.execute(
            "INSERT INTO interpreter.llm_calls (incident_id, command_id, model, tokens_in, tokens_out, latency_ms,"
            " validation_passed, attempt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (command.correlation_id, command.command_id, call.model, call.tokens_in, call.tokens_out,
             call.latency_ms, call.validation_passed, call.attempt),
        )
        events.append(event(command, "LlmCallRecorded", {
            "incident_id": command.correlation_id, "agent": call.agent, "model": call.model,
            "tokens_in": call.tokens_in, "tokens_out": call.tokens_out, "latency_ms": call.latency_ms,
            "validation_passed": call.validation_passed, "attempt": call.attempt,
        }))
    return events


def open_incident(settings: Settings):
    def handle(conn: psycopg.Connection, command: CommandEnvelope, payload: OpenIncident,
               prepared: Prepared) -> Accepted | Rejected:
        if prepared.duplicate:
            return Rejected(DUPLICATE_NOTICE)
        incident_id = command.correlation_id
        value = prepared.interpretation
        decision = decide(value, prepared.reason, known_orders=prepared.known_orders)
        status = {"delayed": "interpreted", "parked": "parked", "escalated": "escalated"}[decision.kind]
        row = {
            "cause": value.cause.value if value else None,
            "estimated_delay_hours": value.estimated_delay_hours if value else None,
            "severity": value.severity.value if value else None,
        }
        inserted = conn.execute(
            "INSERT INTO interpreter.interpretations (carrier_notice_id, incident_id, command_id, order_ids, cause,"
            " estimated_delay_hours, severity, status, reason, resolved_at)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, CASE WHEN %s::text = 'parked' THEN NULL ELSE now() END)"
            " ON CONFLICT DO NOTHING",
            (payload.carrier_notice_id, incident_id, command.command_id, decision.order_ids, row["cause"],
             row["estimated_delay_hours"], row["severity"], status, decision.reason, status),
        ).rowcount
        if inserted == 0:  # otro CarrierNoticeReceived del mismo aviso se adelantó
            return Rejected(DUPLICATE_NOTICE)
        events = _record_calls(conn, command, prepared.calls)
        if decision.kind == "delayed":
            events.append(event(command, "ShipmentDelayed", shipment_delayed(incident_id, decision.order_ids, row)))
        elif decision.kind == "parked":
            conn.execute(
                "INSERT INTO interpreter.pending_notices (incident_id, order_ids, deadline_at)"
                " VALUES (%s, %s, now() + make_interval(secs => %s))",
                (incident_id, decision.order_ids, settings.pending_notice_timeout_s),
            )
        else:
            events.append(event(command, "IncidentEscalated", {"incident_id": incident_id,
                                                               "reason": decision.reason}))
        return Accepted(events, {"incident_id": incident_id, "outcome": decision.kind})

    return handle
