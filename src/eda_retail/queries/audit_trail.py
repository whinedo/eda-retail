"""Consulta de `audit_trail` (spec observability-audit): solo lee `view_*` y
`projector_offsets`, nunca escribe y responde con `last_applied_seq`."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg

from eda_retail.projectors.audit_trail import PROJECTOR_NAME
from eda_retail.queries.positions import last_applied_seq


@dataclass(frozen=True)
class AuditEntry:
    seq: int
    actor: str
    decision: str
    plan_version: int | None
    reason: str | None
    occurred_at: datetime


@dataclass(frozen=True)
class AuditTrail:
    incident_id: str
    entries: list[AuditEntry]
    last_applied_seq: int


def get_audit_trail(conn: psycopg.Connection, incident_id: str) -> AuditTrail:
    """Las decisiones de la incidencia en orden de `seq`; lista vacía si no hay ninguna."""
    with conn.transaction():
        position = last_applied_seq(conn, PROJECTOR_NAME)
        rows = conn.execute(
            "SELECT seq, actor, decision, plan_version, reason, occurred_at "
            "FROM view_audit_trail WHERE incident_id = %s ORDER BY seq",
            (incident_id,),
        ).fetchall()
    return AuditTrail(incident_id, [AuditEntry(**row) for row in rows], position)
