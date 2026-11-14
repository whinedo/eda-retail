"""Vista `carrier_dashboard` (spec cqrs-views, «Vistas mínimas»): por transportista,
incidencias, retrasos (`cause = delay`), reclamaciones y reembolsos.

El transportista solo llega en `CarrierNoticeReceived`; `view_carrier_incidents` guarda a
qué transportista pertenece cada incidencia para atribuirle los eventos posteriores. Una
incidencia sin aviso cuenta como `unknown`.

Los contadores suman: que un evento no se aplique dos veces lo garantizan la posición por
partición y `views.applied_events` del proyector (`projectors.base`).
"""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.positions import Position
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "carrier_dashboard"
UNKNOWN_CARRIER = "unknown"


def _carrier(conn: psycopg.Connection, incident_id: str) -> str:
    row = conn.execute("SELECT carrier FROM views.view_carrier_incidents WHERE incident_id = %s", (incident_id,)).fetchone()
    return row["carrier"] if row else UNKNOWN_CARRIER


def _add(conn: psycopg.Connection, carrier: str, column: str) -> None:
    conn.execute(
        "INSERT INTO views.view_carrier_dashboard (carrier) VALUES (%s) ON CONFLICT (carrier) DO NOTHING", (carrier,)
    )
    conn.execute(f"UPDATE views.view_carrier_dashboard SET {column} = {column} + 1 WHERE carrier = %s", (carrier,))


def _notice(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    carrier = event.payload["carrier"]
    conn.execute(
        "INSERT INTO views.view_carrier_incidents (incident_id, carrier) VALUES (%s, %s) ON CONFLICT (incident_id) DO NOTHING",
        (event.correlation_id, carrier),
    )
    _add(conn, carrier, "incidents")


def _delayed(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    if event.payload["cause"] == "delay":
        _add(conn, _carrier(conn, event.correlation_id), "delays")


def _claim(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _add(conn, _carrier(conn, event.correlation_id), "claims_filed")


def _refund(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _add(conn, _carrier(conn, event.correlation_id), "refunds_issued")


HANDLERS = {
    "CarrierNoticeReceived": _notice,
    "ShipmentDelayed": _delayed,
    "ClaimFiled": _claim,
    "RefundIssued": _refund,
}


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
