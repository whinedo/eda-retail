"""Vista `audit_trail` (spec observability-audit, «Auditoría de decisiones»).

Una fila por evento de decisión, con `seq` como clave: aplicar dos veces el mismo evento no
cambia la vista (`ON CONFLICT DO NOTHING`), y la vista se reconstruye solo desde el log.

- `actor`: el `producer` del evento (qué componente lo decidió).
- `decision`: el tipo de evento; en `StepRequested`, también la acción pedida.
- `plan_version`: la del evento, si la lleva.
- `reason`: el motivo que lleva el evento (`PlanRevised.reason`, `StepFailed.error`,
  `IncidentEscalated.reason`) o `validation_failed` en un `LlmCallRecorded` cuya salida no
  pasó la validación del código.

Un comando rechazado no escribe eventos: la auditoría solo ve los rechazos que dejan rastro
en el log. El texto de los avisos (`raw_text`) es contenido no confiable y no se guarda.
"""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "audit_trail"

AUDITED = (
    "CarrierNoticeReceived", "ShipmentDelayed", "PlanCreated", "PlanRevised", "StepRequested",
    "StepCompleted", "StepFailed", "StepCancelled", "CustomerNotified", "ClaimFiled", "StockReserved",
    "StockReleased", "ShipmentReissued", "RefundIssued", "IncidentResolved", "IncidentEscalated",
    "LlmCallRecorded",
)

_REASON_FIELD = {"PlanRevised": "reason", "StepFailed": "error", "IncidentEscalated": "reason"}


def _decision(event: EventEnvelope) -> str:
    if event.event_type == "StepRequested":
        return f"StepRequested:{event.payload['action']}"
    return event.event_type


def _reason(event: EventEnvelope) -> str | None:
    if event.event_type == "LlmCallRecorded":
        return None if event.payload["validation_passed"] else "validation_failed"
    field = _REASON_FIELD.get(event.event_type)
    return event.payload[field] if field else None


def _record(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    conn.execute(
        """
        INSERT INTO view_audit_trail (seq, incident_id, plan_version, actor, decision, reason, occurred_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (seq) DO NOTHING
        """,
        (seq, event.correlation_id, event.payload.get("plan_version"), event.producer,
         _decision(event), _reason(event), event.occurred_at),
    )


HANDLERS = dict.fromkeys(AUDITED, _record)


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
