"""Agente planificador (spec incident-planning, "Plan según la gravedad").

- `severity` `low`: plan fijo en código, sin LLM (supuesto A3): un `notify_customer` por
  pedido afectado.
- En otro caso el LLM propone un plan y el código lo comprueba con un ensayo del propio
  command handler (`validate`), que aplica las mismas reglas que la escritura real
  (acciones permitidas, compensaciones, dependencias, importe máximo). Si se rechaza, se
  pide un plan nuevo explicando el motivo, dentro de los límites de llm-boundaries.
- Agotadas las generaciones o el presupuesto: `EscalateIncident` con el motivo.
"""

from __future__ import annotations

import psycopg

from eda_retail.agents.prompts import format_orders
from eda_retail.agents.runtime import Decision
from eda_retail.command.handler import CommandHandler
from eda_retail.command.models import CreatePlan, EscalateIncident
from eda_retail.command.rules import BusinessRules
from eda_retail.events.catalog import Severity
from eda_retail.events.envelope import EventEnvelope
from eda_retail.llm.generation import LlmClient, generate
from eda_retail.llm.prompts.planner import (  # noqa: F401
    ACTIONS,
    POLICY,
    SYSTEM,
    low_severity_plan,
    plan_request,
    policy_actions,
)
from eda_retail.llm.schemas import PlanProposal


def rejection(result) -> str | None:
    if result.accepted:
        return None
    return f"{result.reason} ({result.detail})" if result.detail else result.reason


class Planner:
    name = "planner"
    TOOLS: tuple = ()  # sin herramientas con efectos (spec llm-boundaries)

    def __init__(self, llm: LlmClient, handler: CommandHandler, rules: BusinessRules | None = None) -> None:
        self._llm = llm
        self._handler = handler
        self._rules = rules or handler.rules

    def decide(self, conn: psycopg.Connection, event: EventEnvelope) -> Decision:
        p = event.payload
        incident_id = p["incident_id"]
        with conn.transaction():
            version = self._handler.current_version(conn, incident_id) or 0
            budget = self._handler.budget(conn, incident_id)
            orders = format_orders(conn, p["order_ids"])
        common = {"command_id": event.event_id, "expected_version": version, "producer": self.name,
                  "incident_id": incident_id}
        if p["severity"] == Severity.LOW:
            return Decision(incident_id, [CreatePlan(**common, steps=low_severity_plan(p["order_ids"]))])

        def check(plan: PlanProposal) -> str | None:
            return rejection(self._handler.validate(conn, CreatePlan(**common, steps=plan.command_steps())))

        request = plan_request(incident_id, p["cause"], p["severity"], p["estimated_delay_hours"], orders)
        gen = generate(self._llm, request, PlanProposal, max_generations=self._rules.max_generations,
                       budget=budget, check=check)
        if gen.value is None:
            return Decision(incident_id, [EscalateIncident(**common, reason=gen.reason)], gen.calls)
        return Decision(incident_id, [CreatePlan(**common, steps=gen.value.command_steps())], gen.calls)
