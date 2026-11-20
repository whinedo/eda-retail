"""Prompt y reglas de estabilidad de los `step_id` del replanner (spec incident-planning).

Lo usan el componente `replanner`, el agente anterior y la base síncrona.
"""

from __future__ import annotations

from typing import Any

from eda_retail.llm.generation import LlmRequest
from eda_retail.llm.harness import AgentSpec
from eda_retail.llm.prompts.planner import ACTIONS
from eda_retail.llm.schemas import PlanProposal

MAX_REVISIONS = "max_revisions"
REPLACEABLE = ("pending", "requested", "failed", "cancelled")  # estados sin efecto

SYSTEM = f"""Eres el replanner de incidencias logísticas de una tienda online.
Un paso del plan ha fallado sin posibilidad de reintento. Propón el plan revisado.

{ACTIONS}

Reglas:
- No incluyas los pasos ya completados; los demás pasos pueden depender de ellos.
- Si mantienes un paso del plan vigente, conserva su step_id. Los pasos nuevos usan
  step_id que no existan todavía.
- Cada paso tiene step_id, action, params (order_id y, solo en issue_refund, amount; en el
  resto amount es null) y depends_on.
- No incluyas release_stock ni pasos de compensación: los deriva el sistema.
- Un reembolso no supera el importe del pedido.
Responde solo con el JSON del esquema."""

TEMPLATE = ("Plan vigente (versión {plan_version}):\n{plan}\n\n"
            "Fallo: el paso {failed_step_id} terminó con el error {error} y no se puede reintentar.")
STEP_TEMPLATE = "- {step_id}: {action} {params} depends_on {depends_on} -> {status}{error}"
ERROR_TEMPLATE = ", error {error}"

SPEC = AgentSpec(agent="replanner", prompt_version="1", system=SYSTEM,
                 templates=(TEMPLATE, STEP_TEMPLATE, ERROR_TEMPLATE), output=PlanProposal)


def check_step_ids(steps: list[dict[str, Any]], proposed: list[dict[str, Any]]) -> str | None:
    """Un paso nuevo con la acción y el pedido de un paso abierto que el plan deja fuera: `step_id_changed`.

    `steps` es el plan vigente con el estado de cada paso y `proposed`, los pasos propuestos."""
    proposed_ids = {s["step_id"] for s in proposed}
    current = {s["step_id"] for s in steps}
    open_steps = [s for s in steps if s["status"] in REPLACEABLE and s["step_id"] not in proposed_ids]
    for s in proposed:
        if s["step_id"] in current:
            continue
        for old in open_steps:
            if old["action"] == s["action"] and old["params"].get("order_id") == s["params"].get("order_id"):
                return f"step_id_changed: reutiliza {old['step_id']} para {s['action']} en lugar de {s['step_id']}"
    return None


def render_plan(steps: list[dict[str, Any]]) -> str:
    lines = []
    for s in steps:
        error = ERROR_TEMPLATE.format(error=s["error"]) if s.get("error") else ""
        lines.append(STEP_TEMPLATE.format(step_id=s["step_id"], action=s["action"], params=s["params"],
                                          depends_on=s["depends_on"], status=s["status"], error=error))
    return "\n".join(lines)


def revise_prompt(plan_version: int, steps: list[dict[str, Any]], failed_step_id: str, error: str) -> str:
    """El prompt del replanner: el plan vigente con el estado de cada paso y el fallo."""
    return TEMPLATE.format(plan_version=plan_version, plan=render_plan(steps), failed_step_id=failed_step_id,
                           error=error)


def revise_request(incident_id: str, plan_version: int, steps: list[dict[str, Any]], failed_step_id: str,
                   error: str) -> LlmRequest:
    return SPEC.request(incident_id, revise_prompt(plan_version, steps, failed_step_id, error))
