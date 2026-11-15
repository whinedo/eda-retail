"""Vista `audit_trail` (spec observability-audit, «Auditoría de decisiones»).

Una fila por evento de decisión, con `event_id` como clave y la partición y el offset del
mensaje: los eventos de una incidencia viajan por la misma partición, así que ordenar por offset
es su orden causal. Aplicar dos veces el mismo evento no cambia la vista (`ON CONFLICT DO NOTHING`),
y la vista se reconstruye volviendo a consumir el topic.

- `actor`: el `producer` del evento (qué componente lo decidió).
- `decision`: el tipo de evento; en `StepRequested`, también la acción pedida.
- `plan_version`: la del evento, si la lleva.
- `reason`: el motivo que lleva el evento (`PlanRevised.reason`, `StepFailed.error`,
  `IncidentEscalated.reason`), el resultado de la consulta A2A (`CarrierConsulted.outcome`) o
  `validation_failed` en un `LlmCallRecorded` cuya salida no pasó la validación del código.

Un comando rechazado no escribe eventos: la auditoría solo ve los rechazos que dejan rastro
en el log. El texto de los avisos (`raw_text`) es contenido no confiable y no se guarda.
"""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.positions import Position
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "audit_trail"

AUDITED = (
    "CarrierNoticeReceived", "ShipmentDelayed", "PlanCreated", "PlanRevised", "StepRequested",
    "StepCompleted", "StepFailed", "StepCancelled", "CustomerNotified", "ClaimFiled", "StockReserved",
    "StockReleased", "ShipmentReissued", "RefundIssued", "IncidentResolved", "IncidentEscalated",
    "LlmCallRecorded", "CarrierConsulted",
)

_REASON_FIELD = {"PlanRevised": "reason", "StepFailed": "error", "IncidentEscalated": "reason",
                 "CarrierConsulted": "outcome"}


def _decision(event: EventEnvelope) -> str:
    if event.event_type == "StepRequested":
        return f"StepRequested:{event.payload['action']}"
    return event.event_type


def _reason(event: EventEnvelope) -> str | None:
    if event.event_type == "LlmCallRecorded":
        return None if event.payload["validation_passed"] else "validation_failed"
    field = _REASON_FIELD.get(event.event_type)
    return event.payload[field] if field else None


def _record(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    conn.execute(
        """
        INSERT INTO views.view_audit_trail (event_id, kafka_partition, kafka_offset, incident_id, plan_version,
                                            actor, decision, reason, occurred_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        """,
        (event.event_id, position.partition, position.offset, event.correlation_id, event.payload.get("plan_version"), event.producer,
         _decision(event), _reason(event), event.occurred_at),
    )


HANDLERS = dict.fromkeys(AUDITED, _record)


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
