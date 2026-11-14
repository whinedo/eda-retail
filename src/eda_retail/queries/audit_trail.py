"""Consulta de `audit_trail` (spec observability-audit): solo lee `view_*` y
`views.offsets`, nunca escribe y responde con `applied` (offset aplicado por partición)."""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime

import psycopg

from eda_retail.projectors.audit_trail import PROJECTOR_NAME
from eda_retail.queries.positions import applied


@dataclass(frozen=True)
class AuditEntry:
    event_id: uuid.UUID
    kafka_partition: int
    kafka_offset: int
    actor: str
    decision: str
    plan_version: int | None
    reason: str | None
    occurred_at: datetime


@dataclass(frozen=True)
class AuditTrail:
    incident_id: str
    entries: list[AuditEntry]
    applied: dict[int, int]


def get_audit_trail(conn: psycopg.Connection, incident_id: str, *, topic: str | None = None) -> AuditTrail:
    """Las decisiones de la incidencia en su orden causal (partición y offset); lista vacía si no hay."""
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        rows = conn.execute(
            "SELECT event_id, kafka_partition, kafka_offset, actor, decision, plan_version, reason, occurred_at "
            "FROM views.view_audit_trail WHERE incident_id = %s ORDER BY kafka_partition, kafka_offset",
            (incident_id,),
        ).fetchall()
    return AuditTrail(incident_id, [AuditEntry(**row) for row in rows], position)
