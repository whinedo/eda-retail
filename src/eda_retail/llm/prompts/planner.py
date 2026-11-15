"""Prompt, política y plan fijo del planificador (spec incident-planning, "Plan según la gravedad").

Lo usan el componente `planner`, el agente anterior, la base síncrona y E5.
"""

from __future__ import annotations

from typing import Any

from eda_retail.llm.generation import LlmRequest
from eda_retail.llm.schemas import PlanProposal, strict_schema

ACTIONS = """Acciones permitidas:
- notify_customer: avisa al cliente del problema.
- file_claim: abre una reclamación al transportista.
- reserve_stock: reserva stock para un reenvío.
- reissue_shipment: reenvía el pedido (necesita una reserva de stock).
- issue_refund: reembolsa el pedido; es irreversible y va al final.
Para un mismo pedido, reenvío (reissue_shipment) y reembolso (issue_refund) son excluyentes:
elige uno de los dos, nunca ambos."""


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


def plan_request(incident_id: str, cause: str, severity: str, delay_hours: int, orders: str) -> LlmRequest:
    """La petición al LLM del planificador (también la usan la base síncrona y E5)."""
    return LlmRequest(
        agent="planner", incident_id=incident_id, system=SYSTEM, schema=strict_schema(PlanProposal),
        prompt=(f"Incidencia: causa {cause}, gravedad {severity}, "
                f"retraso estimado {delay_hours} h.\nPedidos afectados:\n{orders}"),
    )
