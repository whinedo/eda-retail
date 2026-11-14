"""Vista `incident_status`: estado de la incidencia, versión del plan y estado de cada paso.

Refleja las transiciones del command handler (`command/plans.py`, `command/steps.py`):
- Un plan revisado conserva el estado de los pasos mantenidos (los `failed` y `cancelled`
  vuelven a `pending`); los pasos que salen del plan siguen en la lista como estaban.
- Al completarse una compensación (`<step_id>-comp`), el paso compensado pasa de
  `compensating` a `compensated`.
Cada handler fija valores absolutos, así que aplicar dos veces un evento no cambia la vista.
"""

from __future__ import annotations

from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from eda_retail.events.envelope import EventEnvelope
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "incident_status"
COMPENSATION_SUFFIX = "-comp"


def _insert(conn: psycopg.Connection, event: EventEnvelope, status: str, reason: str | None = None) -> None:
    conn.execute(
        """
        INSERT INTO view_incident_status (incident_id, status, escalation_reason, last_applied_event)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (incident_id) DO NOTHING
        """,
        (event.correlation_id, status, reason, event.event_id),
    )


def _update(conn: psycopg.Connection, event: EventEnvelope, **fields: Any) -> None:
    sets = ", ".join(f"{k} = %s" for k in fields)
    values = [Jsonb(v) if k == "steps" else v for k, v in fields.items()]
    conn.execute(
        f"UPDATE view_incident_status SET {sets}, last_applied_event = %s, updated_at = now() WHERE incident_id = %s",
        (*values, event.event_id, event.correlation_id),
    )


def _steps(conn: psycopg.Connection, incident_id: str) -> list[dict[str, Any]]:
    row = conn.execute("SELECT steps FROM view_incident_status WHERE incident_id = %s", (incident_id,)).fetchone()
    return list(row["steps"]) if row else []


def _set_step(conn: psycopg.Connection, event: EventEnvelope, step_id: str, **fields: Any) -> None:
    steps = _steps(conn, event.correlation_id)
    for s in steps:
        if s["step_id"] == step_id:
            s.update(fields)
    if step_id.endswith(COMPENSATION_SUFFIX) and fields.get("status") == "completed":
        original = step_id.removesuffix(COMPENSATION_SUFFIX)
        for s in steps:
            if s["step_id"] == original and s["status"] == "compensating":
                s["status"] = "compensated"
    _update(conn, event, steps=steps)


def _view_step(step: dict[str, Any], status: str) -> dict[str, Any]:
    return {**step, "status": status}


def _shipment_delayed(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _insert(conn, event, "open")


def _plan_created(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    steps = [_view_step(s, "pending") for s in event.payload["steps"]]
    _update(conn, event, plan_version=event.payload["plan_version"], steps=steps)


def _plan_revised(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    previous = {s["step_id"]: s for s in _steps(conn, event.correlation_id)}
    revised: list[dict[str, Any]] = []
    for s in event.payload["steps"]:
        old = previous.pop(s["step_id"], None)
        if old is None or old["status"] in ("failed", "cancelled"):
            revised.append(_view_step(s, "pending"))
        else:  # paso mantenido en curso: conserva estado y resultado
            carried = {k: old[k] for k in ("result", "error") if k in old}
            revised.append(_view_step(s, old["status"]) | carried)
    _update(conn, event, plan_version=event.payload["plan_version"], steps=revised + list(previous.values()))


def _step_requested(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _set_step(conn, event, event.payload["step_id"], status="requested")


def _step_completed(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _set_step(conn, event, event.payload["step_id"], status="completed", result=event.payload["result"])


def _step_failed(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _set_step(conn, event, event.payload["step_id"], status="failed", error=event.payload["error"])


def _resolved(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _update(conn, event, status="resolved")


def _escalated(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    # Puede escalarse antes de existir en la vista (supuesto A5 o sin pedidos válidos).
    _insert(conn, event, "escalated", event.payload["reason"])
    _update(conn, event, status="escalated", escalation_reason=event.payload["reason"])


HANDLERS = {
    "ShipmentDelayed": _shipment_delayed,
    "PlanCreated": _plan_created,
    "PlanRevised": _plan_revised,
    "StepRequested": _step_requested,
    "StepCompleted": _step_completed,
    "StepFailed": _step_failed,
    "IncidentResolved": _resolved,
    "IncidentEscalated": _escalated,
}


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
