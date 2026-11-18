"""Proyector `operations` (spec incident-metrics): los hechos operativos de cada incidencia para la vista de
operaciones del front (spec operations-view).

- `view_incident_metrics`: una fila por incidencia, creada con su primer evento. Guarda el transportista, la causa y
  la gravedad, el estado final y su motivo, la versión del plan, los pasos fallidos, las compensaciones
  (`StockReleased`) y los datos y la posición del último evento. Un estado final no cambia; los contadores siguen
  sumando (tras un escalado llegan las compensaciones).
- `view_llm_calls`, `view_step_failures`, `view_carrier_consults` y `view_reissued_shipments`: una fila por evento,
  con el `event_id` como clave.
- `view_operations_steps`: la acción de cada paso pedido (`StepRequested`), porque `StepFailed` no la lleva.

Nada guarda precios, duraciones ni agregados: los calculan las consultas (`queries.operations`). Que un evento no se
aplique dos veces lo garantizan la posición por partición y `views.applied_events` (`projectors.base`); las tablas de
hechos, además, no repiten un `event_id`.
"""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.positions import Position
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "operations"
UNKNOWN_CARRIER = "unknown"
TABLES = ("views.view_incident_metrics", "views.view_llm_calls", "views.view_step_failures",
          "views.view_carrier_consults", "views.view_operations_steps", "views.view_reissued_shipments")

_LAST = ("last_event_at", "last_event_type", "last_event_id", "kafka_partition", "kafka_offset")


def _touch(conn: psycopg.Connection, position: Position, event: EventEnvelope, sets: str = "",
           params: tuple = ()) -> None:
    """Crea la fila de la incidencia si no existe (con `opened_at` = este evento), anota el último evento y aplica
    `sets` (fragmento `SET` con `%s`)."""
    last = (event.occurred_at, event.event_type, event.event_id, position.partition, position.offset)
    conn.execute(
        f"""
        INSERT INTO views.view_incident_metrics (incident_id, opened_at, {", ".join(_LAST)})
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (incident_id) DO UPDATE SET {", ".join(f"{c} = EXCLUDED.{c}" for c in _LAST)}
        """,
        (event.correlation_id, event.occurred_at, *last),
    )
    if sets:
        conn.execute(f"UPDATE views.view_incident_metrics SET {sets} WHERE incident_id = %s",
                     (*params, event.correlation_id))


def _notice(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _touch(conn, position, event, "carrier = %s", (event.payload["carrier"],))


def _delayed(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    p = event.payload
    _touch(conn, position, event, "cause = %s, severity = %s, order_count = %s",
           (p["cause"], p["severity"], len(p["order_ids"])))


def _plan(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _touch(conn, position, event, "plan_version = GREATEST(plan_version, %s)", (event.payload["plan_version"],))


def _requested(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _touch(conn, position, event)
    conn.execute(
        """
        INSERT INTO views.view_operations_steps (incident_id, step_id, action) VALUES (%s, %s, %s)
        ON CONFLICT (incident_id, step_id) DO UPDATE SET action = EXCLUDED.action
        """,
        (event.correlation_id, event.payload["step_id"], event.payload["action"]),
    )


def _failed(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    p = event.payload
    _touch(conn, position, event, "steps_failed = steps_failed + 1")
    row = conn.execute("SELECT action FROM views.view_operations_steps WHERE incident_id = %s AND step_id = %s",
                       (event.correlation_id, p["step_id"])).fetchone()
    conn.execute(
        """
        INSERT INTO views.view_step_failures (event_id, incident_id, step_id, action, error_code, plan_version,
                                              occurred_at, kafka_partition, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING
        """,
        (event.event_id, event.correlation_id, p["step_id"], row["action"] if row else None, p["error"],
         p["plan_version"], event.occurred_at, position.partition, position.offset),
    )


def _released(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _touch(conn, position, event, "compensations = compensations + 1")


def _resolved(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _touch(conn, position, event)
    conn.execute("UPDATE views.view_incident_metrics SET state = %s, closed_at = %s"
                 " WHERE incident_id = %s AND state = 'open'",
                 (event.payload["outcome"], event.occurred_at, event.correlation_id))


def _escalated(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _touch(conn, position, event)
    conn.execute("UPDATE views.view_incident_metrics SET state = 'escalated', escalation_reason = %s, closed_at = %s"
                 " WHERE incident_id = %s AND state = 'open'",
                 (event.payload["reason"], event.occurred_at, event.correlation_id))


def _llm_call(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    p = event.payload
    _touch(conn, position, event)
    conn.execute(
        """
        INSERT INTO views.view_llm_calls (event_id, incident_id, agent, model, tokens_in, tokens_out, latency_ms,
                                          validated, occurred_at, kafka_partition, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING
        """,
        (event.event_id, event.correlation_id, p["agent"], p["model"], p["tokens_in"], p["tokens_out"],
         p["latency_ms"], p["validation_passed"], event.occurred_at, position.partition, position.offset),
    )


def _consulted(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    p = event.payload
    _touch(conn, position, event)
    conn.execute(
        """
        INSERT INTO views.view_carrier_consults (event_id, incident_id, outcome, rounds, latency_ms, occurred_at,
                                                 kafka_partition, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING
        """,
        (event.event_id, event.correlation_id, p["outcome"], p["rounds"], p["latency_ms"], event.occurred_at,
         position.partition, position.offset),
    )


def _reissued(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _touch(conn, position, event)
    conn.execute(
        """
        INSERT INTO views.view_reissued_shipments (event_id, incident_id, order_id, occurred_at, kafka_partition,
                                                   kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (event_id) DO NOTHING
        """,
        (event.event_id, event.correlation_id, event.payload["order_id"], event.occurred_at, position.partition,
         position.offset),
    )


HANDLERS = {
    "CarrierNoticeReceived": _notice,
    "ShipmentDelayed": _delayed,
    "PlanCreated": _plan,
    "PlanRevised": _plan,
    "StepRequested": _requested,
    "StepFailed": _failed,
    "StockReleased": _released,
    "IncidentResolved": _resolved,
    "IncidentEscalated": _escalated,
    "LlmCallRecorded": _llm_call,
    "CarrierConsulted": _consulted,
    "ShipmentReissued": _reissued,
}


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
