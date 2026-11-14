"""Replanner (spec incident-planning, "Revisión del plan ante un fallo").

Consume `StepFailed` no reintentables y propone un `RevisePlan`. Lee el plan solo de la
vista `incident_status` (spec cqrs-views); si la vista todavía no refleja el fallo, espera
un tiempo acotado y, si no llega, lanza `ViewNotCaughtUp` sin registrar el evento en el
inbox, para que se vuelva a procesar.

Reglas en código, no en el LLM:
- agotadas las revisiones (`MAX_PLAN_REVISIONS`), `EscalateIncident` `max_revisions` sin
  llamar al LLM;
- estabilidad de los `step_id`: un paso nuevo con la misma acción y pedido que un paso no
  completado del plan vigente se rechaza (`step_id_changed`), hay que reutilizar su id;
- el resto (pasos completados, compensaciones, acciones, importes) lo comprueba el ensayo
  del command handler.
Los fallos de compensaciones (`-comp`) no se replanifican: los gestiona la orquestación.
"""

from __future__ import annotations

import psycopg

from eda_retail.agents.planner import rejection
from eda_retail.agents.prompts import ACTIONS
from eda_retail.agents.runtime import Decision
from eda_retail.command.handler import CommandHandler
from eda_retail.command.models import EscalateIncident, RevisePlan
from eda_retail.command.plans import COMPENSATION_SUFFIX
from eda_retail.command.rules import BusinessRules
from eda_retail.events.envelope import EventEnvelope
from eda_retail.llm.generation import LlmClient, LlmRequest, generate
from eda_retail.llm.schemas import PlanProposal, strict_schema
from eda_retail.queries.incident_status import IncidentStatus, wait_for_incident_status

MAX_REVISIONS = "max_revisions"
_REPLACEABLE = ("pending", "requested", "failed", "cancelled")

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


class ViewNotCaughtUp(RuntimeError):
    """`incident_status` aún no refleja el fallo; el evento se reprocesará."""


def check_step_ids(status: IncidentStatus, plan: PlanProposal) -> str | None:
    proposed = {s.step_id for s in plan.steps}
    current = {s["step_id"] for s in status.steps}
    open_steps = [s for s in status.steps if s["status"] in _REPLACEABLE and s["step_id"] not in proposed]
    for s in plan.steps:
        if s.step_id in current:
            continue
        for old in open_steps:
            if old["action"] == s.action and old["params"].get("order_id") == s.params.order_id:
                return f"step_id_changed: reutiliza {old['step_id']} para {s.action} en lugar de {s.step_id}"
    return None


def render_plan(status: IncidentStatus) -> str:
    lines = []
    for s in status.steps:
        error = f", error {s['error']}" if s.get("error") else ""
        lines.append(f"- {s['step_id']}: {s['action']} {s['params']} depends_on {s['depends_on']} "
                     f"-> {s['status']}{error}")
    return "\n".join(lines)


class Replanner:
    name = "replanner"
    TOOLS: tuple = ()  # sin herramientas con efectos (spec llm-boundaries)

    def __init__(
        self, llm: LlmClient, handler: CommandHandler, rules: BusinessRules | None = None, *, view_timeout: float = 5.0
    ) -> None:
        self._llm = llm
        self._handler = handler
        self._rules = rules or handler.rules
        self._view_timeout = view_timeout

    def decide(self, conn: psycopg.Connection, event: EventEnvelope) -> Decision:
        incident_id = event.correlation_id
        p = event.payload
        if p["retryable"] or p["step_id"].endswith(COMPENSATION_SUFFIX):
            return Decision(incident_id)

        def reflects_failure(status: IncidentStatus) -> bool:
            step = status.step(p["step_id"])
            return status.status != "open" or (step is not None and step["status"] == "failed")

        status = wait_for_incident_status(conn, incident_id, reflects_failure, timeout=self._view_timeout)
        if status is None:
            raise ViewNotCaughtUp(f"{incident_id}: la vista no refleja aún el fallo de {p['step_id']}")
        if status.status != "open":
            return Decision(incident_id)
        with conn.transaction():
            version = self._handler.current_version(conn, incident_id) or 0
            budget = self._handler.budget(conn, incident_id)
        common = {"command_id": event.event_id, "expected_version": version, "producer": self.name,
                  "incident_id": incident_id}
        if status.plan_version - 1 >= self._rules.max_plan_revisions:
            return Decision(incident_id, [EscalateIncident(**common, reason=MAX_REVISIONS)])

        def check(plan: PlanProposal) -> str | None:
            return check_step_ids(status, plan) or rejection(
                self._handler.validate(conn, RevisePlan(**common, steps=plan.command_steps(), reason=p["error"]))
            )

        request = LlmRequest(
            agent=self.name, incident_id=incident_id, system=SYSTEM, schema=strict_schema(PlanProposal),
            prompt=(f"Plan vigente (versión {status.plan_version}):\n{render_plan(status)}\n\n"
                    f"Fallo: el paso {p['step_id']} terminó con el error {p['error']} y no se puede reintentar."),
        )
        gen = generate(self._llm, request, PlanProposal, max_generations=self._rules.max_generations,
                       budget=budget, check=check)
        if gen.value is None:
            return Decision(incident_id, [EscalateIncident(**common, reason=gen.reason)], gen.calls)
        command = RevisePlan(**common, steps=gen.value.command_steps(), reason=p["error"])
        return Decision(incident_id, [command], gen.calls)
