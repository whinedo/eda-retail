"""Vista `carrier_dashboard` (spec cqrs-views, «Vistas mínimas»): por transportista,
incidencias, retrasos (`cause = delay`), reclamaciones y reembolsos.

El transportista solo llega en `CarrierNoticeReceived`; `view_carrier_incidents` guarda a
qué transportista pertenece cada incidencia para atribuirle los eventos posteriores. Una
incidencia sin aviso cuenta como `unknown`.

Los contadores suman, así que cada fila guarda `last_seq`: un evento con `seq <= last_seq`
ya está aplicado y no cambia la vista (proyector idempotente).
"""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "carrier_dashboard"
UNKNOWN_CARRIER = "unknown"


def _carrier(conn: psycopg.Connection, incident_id: str) -> str:
    row = conn.execute("SELECT carrier FROM view_carrier_incidents WHERE incident_id = %s", (incident_id,)).fetchone()
    return row["carrier"] if row else UNKNOWN_CARRIER


def _add(conn: psycopg.Connection, seq: int, carrier: str, column: str) -> None:
    conn.execute(
        "INSERT INTO view_carrier_dashboard (carrier) VALUES (%s) ON CONFLICT (carrier) DO NOTHING", (carrier,)
    )
    conn.execute(
        f"UPDATE view_carrier_dashboard SET {column} = {column} + 1, last_seq = %s "
        "WHERE carrier = %s AND last_seq < %s",
        (seq, carrier, seq),
    )


def _notice(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    carrier = event.payload["carrier"]
    conn.execute(
        "INSERT INTO view_carrier_incidents (incident_id, carrier) VALUES (%s, %s) ON CONFLICT (incident_id) DO NOTHING",
        (event.correlation_id, carrier),
    )
    _add(conn, seq, carrier, "incidents")


def _delayed(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    if event.payload["cause"] == "delay":
        _add(conn, seq, _carrier(conn, event.correlation_id), "delays")


def _claim(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _add(conn, seq, _carrier(conn, event.correlation_id), "claims_filed")


def _refund(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _add(conn, seq, _carrier(conn, event.correlation_id), "refunds_issued")


HANDLERS = {
    "CarrierNoticeReceived": _notice,
    "ShipmentDelayed": _delayed,
    "ClaimFiled": _claim,
    "RefundIssued": _refund,
}


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
