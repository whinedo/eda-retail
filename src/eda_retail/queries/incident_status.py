"""Consulta de `incident_status` (spec cqrs-views): solo lee `view_*` y `projector_offsets`.

Cada respuesta lleva `last_applied_seq`, la posición del proyector que alimenta la vista,
para que el cliente sepa cuán actual es (consistencia eventual declarada).
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg

from eda_retail.projectors.incident_status import PROJECTOR_NAME


@dataclass(frozen=True)
class IncidentStatus:
    incident_id: str
    status: str
    plan_version: int
    steps: list[dict[str, Any]]
    escalation_reason: str | None
    last_applied_seq: int

    def step(self, step_id: str) -> dict[str, Any] | None:
        return next((s for s in self.steps if s["step_id"] == step_id), None)


def last_applied_seq(conn: psycopg.Connection) -> int:
    row = conn.execute("SELECT last_seq FROM projector_offsets WHERE projector = %s", (PROJECTOR_NAME,)).fetchone()
    return row["last_seq"] if row else 0


def get_incident_status(conn: psycopg.Connection, incident_id: str) -> IncidentStatus | None:
    with conn.transaction():
        position = last_applied_seq(conn)
        row = conn.execute(
            "SELECT incident_id, status, plan_version, steps, escalation_reason "
            "FROM view_incident_status WHERE incident_id = %s",
            (incident_id,),
        ).fetchone()
    if row is None:
        return None
    return IncidentStatus(**row, last_applied_seq=position)


def wait_for_incident_status(
    conn: psycopg.Connection,
    incident_id: str,
    ready: Callable[[IncidentStatus], bool],
    *,
    timeout: float,
    interval: float = 0.1,
) -> IncidentStatus | None:
    """Espera, como mucho `timeout` segundos, a que la vista cumpla `ready` (consistencia
    eventual). Devuelve None si no llega a tiempo. Nunca escribe."""
    deadline = time.monotonic() + timeout
    while True:
        status = get_incident_status(conn, incident_id)
        if status is not None and ready(status):
            return status
        if time.monotonic() >= deadline:
            return None
        time.sleep(interval)
