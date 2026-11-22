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
- No incluyas los pasos ya completados (completed); los demás pasos pueden depender de ellos.
- Un paso del plan vigente que no está completado y sigue haciendo falta va en el plan y
  conserva su step_id, aunque cambies su depends_on; un paso que dejas fuera se cancela. No
  crees un paso nuevo con la acción y el pedido de un paso que puedes mantener.
- Los pasos nuevos usan step_id que no existan todavía. Un step_id del plan vigente conserva su
  acción: nunca uses el step_id de un paso para otra acción.
- depends_on solo lista pasos del plan revisado o pasos ya completados: nunca del paso que ha
  fallado ni de un paso que dejas fuera.
- Cada paso tiene step_id, action, params (order_id y, solo en issue_refund, amount; en el
  resto amount es null) y depends_on.
- Un issue_refund lleva siempre amount: el importe del pedido que aparece en «Pedidos de la
  incidencia», nunca más.
- No incluyas release_stock ni pasos de compensación: los deriva el sistema.
- Ejemplo: notify_customer y file_claim completed, reserve_stock failed y reissue_shipment
  pending que depende de él; pedido P1 de importe 40.00. El reenvío ya no es posible: el plan
  revisado tiene solo un issue_refund nuevo, con amount 40.00, depends_on [] y un step_id que no
  existe en el plan vigente. No lleva notify_customer ni file_claim porque están completados; si
  file_claim siguiera pending, iría también, con su mismo step_id.
Responde solo con el JSON del esquema."""

TEMPLATE = ("Plan vigente (versión {plan_version}):\n{plan}\n\n"
            "Pedidos de la incidencia:\n{orders}\n\n"
            "Fallo: el paso {failed_step_id} terminó con el error {error} y no se puede reintentar.")
STEP_TEMPLATE = "- {step_id}: {action} {params} depends_on {depends_on} -> {status}{error}"
ERROR_TEMPLATE = ", error {error}"

SPEC = AgentSpec(agent="replanner", prompt_version="3", system=SYSTEM,
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


def revise_prompt(plan_version: int, steps: list[dict[str, Any]], failed_step_id: str, error: str,
                  orders: str) -> str:
    """El prompt del replanner: el plan vigente con el estado de cada paso, los pedidos con su importe
    (`planner.handler.order_lines`, para que un reembolso lleve un `amount` válido) y el fallo."""
    return TEMPLATE.format(plan_version=plan_version, plan=render_plan(steps), orders=orders,
                           failed_step_id=failed_step_id, error=error)


def revise_request(incident_id: str, plan_version: int, steps: list[dict[str, Any]], failed_step_id: str,
                   error: str, *, orders: str) -> LlmRequest:
    return SPEC.request(incident_id, revise_prompt(plan_version, steps, failed_step_id, error, orders))
