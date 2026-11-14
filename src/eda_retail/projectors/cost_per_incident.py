"""Vista `cost_per_incident` (spec observability-audit, «Coste por incidencia»).

Guarda solo hechos del log por incidencia: llamadas al LLM (aceptadas o no) con sus tokens,
reclamaciones, reenvíos e importe reembolsado. El coste en dinero lo calcula la consulta
con los precios de la configuración, así la vista reconstruida es idéntica aunque cambien
los precios.

Los contadores suman, así que cada fila guarda `last_seq`: un evento con `seq <= last_seq`
ya está aplicado y no cambia la vista (proyector idempotente).
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "cost_per_incident"


def _add(conn: psycopg.Connection, seq: int, event: EventEnvelope, **increments: Any) -> None:
    conn.execute(
        "INSERT INTO view_cost_per_incident (incident_id) VALUES (%s) ON CONFLICT (incident_id) DO NOTHING",
        (event.correlation_id,),
    )
    sets = ", ".join(f"{column} = {column} + %s" for column in increments)
    conn.execute(
        f"UPDATE view_cost_per_incident SET {sets}, last_seq = %s WHERE incident_id = %s AND last_seq < %s",
        (*increments.values(), seq, event.correlation_id, seq),
    )


def _llm_call(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    p = event.payload
    _add(conn, seq, event, llm_calls=1, tokens_in=p["tokens_in"], tokens_out=p["tokens_out"])


def _claim(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _add(conn, seq, event, claims=1)


def _reissue(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _add(conn, seq, event, reissues=1)


def _refund(conn: psycopg.Connection, seq: int, event: EventEnvelope) -> None:
    _add(conn, seq, event, refunds_amount=Decimal(str(event.payload["amount"])))


HANDLERS = {
    "LlmCallRecorded": _llm_call,
    "ClaimFiled": _claim,
    "ShipmentReissued": _reissue,
    "RefundIssued": _refund,
}


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
