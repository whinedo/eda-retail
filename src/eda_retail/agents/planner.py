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

from typing import Any

import psycopg

from eda_retail.agents.prompts import ACTIONS, format_orders
from eda_retail.agents.runtime import Decision
from eda_retail.command.handler import CommandHandler
from eda_retail.command.models import CreatePlan, EscalateIncident
from eda_retail.command.rules import BusinessRules
from eda_retail.events.catalog import Severity
from eda_retail.events.envelope import EventEnvelope
from eda_retail.llm.generation import LlmClient, LlmRequest, generate
from eda_retail.llm.schemas import PlanProposal, strict_schema

# Política de planes (supuesto A1 de odd/tasks/experiments.md): qué acciones pide cada causa y
# gravedad. Va escrita en el prompt; E5 mide si el planificador la sigue.
POLICY: tuple[tuple[str, tuple[str, ...]], ...] = (
    ("cualquier causa con gravedad low", ("notify_customer",)),
    ("delay con gravedad medium", ("notify_customer",)),
    ("delay con gravedad high", ("notify_customer", "file_claim")),
    ("loss o damage", ("notify_customer", "file_claim", "reserve_stock", "reissue_shipment")),
    ("wrong_address", ("notify_customer", "reserve_stock", "reissue_shipment")),
    ("strike o unknown", ("notify_customer",)),
)


def policy_actions(cause: str, severity: str) -> tuple[str, ...]:
    """Las acciones que la política pide para una causa y una gravedad."""
    if severity == "low":
        return POLICY[0][1]
    if cause == "delay":
        return POLICY[2][1] if severity == "high" else POLICY[1][1]
    if cause in ("loss", "damage"):
        return POLICY[3][1]
    if cause == "wrong_address":
        return POLICY[4][1]
    return POLICY[5][1]


_POLICY_TEXT = "\n".join(f"- {when}: {', '.join(actions)}." for when, actions in POLICY)

SYSTEM = f"""Eres el planificador de incidencias logísticas de una tienda online.
Propón el plan de pasos que resuelve la incidencia.

{ACTIONS}

Política de la tienda: las acciones de cada pedido afectado según la causa y la gravedad.
{_POLICY_TEXT}

Reglas:
- Cada paso tiene step_id (S1, S2...), action, params (order_id y, solo en issue_refund,
  amount; en el resto amount es null) y depends_on (lista de step_id, vacía si no depende).
- No incluyas release_stock ni pasos de compensación: los deriva el sistema.
- depends_on solo lista los pasos que deben terminar antes; los pasos independientes no
  dependen entre sí, para poder ejecutarse en paralelo. La única dependencia obligatoria es
  la de reissue_shipment respecto al reserve_stock del mismo pedido.
- Ejemplo para un paquete perdido del pedido P1:
  S1 notify_customer (depends_on []), S2 file_claim (depends_on []),
  S3 reserve_stock (depends_on []), S4 reissue_shipment (depends_on ["S3"]).
- Si el pedido se puede reenviar (reserve_stock y reissue_shipment), no lo reembolses: el
  reembolso es para cuando el reenvío no es posible.
- Un reembolso no supera el importe del pedido.
Responde solo con el JSON del esquema."""


def low_severity_plan(order_ids: list[str]) -> list[dict[str, Any]]:
    return [
        {"step_id": f"S{i}", "action": "notify_customer", "params": {"order_id": o}, "depends_on": []}
        for i, o in enumerate(order_ids, start=1)
    ]


def rejection(result) -> str | None:
    if result.accepted:
        return None
    return f"{result.reason} ({result.detail})" if result.detail else result.reason


def plan_request(incident_id: str, cause: str, severity: str, delay_hours: int, orders: str) -> LlmRequest:
    """La petición al LLM del planificador (también la usan la base síncrona y E5)."""
    return LlmRequest(
        agent="planner", incident_id=incident_id, system=SYSTEM, schema=strict_schema(PlanProposal),
        prompt=(f"Incidencia: causa {cause}, gravedad {severity}, "
                f"retraso estimado {delay_hours} h.\nPedidos afectados:\n{orders}"),
    )


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
