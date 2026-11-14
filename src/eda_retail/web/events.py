"""Página de eventos del log para el panel (spec web-front, "Página de eventos").

Solo lee `events`; la partición y el offset de Kafka no se guardan todavía (T10) y van a None.
"""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.web.routing import node_condition, receivers

DEFAULT_LIMIT = 100
MAX_LIMIT = 500


def list_events(
    conn: psycopg.Connection,
    *,
    limit: int = DEFAULT_LIMIT,
    before_seq: int | None = None,
    incident_id: str | None = None,
    node: str | None = None,
) -> dict[str, Any]:
    where: list[str] = []
    params: list[Any] = []
    if before_seq is not None:
        where.append("seq < %s")
        params.append(before_seq)
    if incident_id:
        where.append("(correlation_id = %s OR payload->>'incident_id' = %s)")
        params.extend([incident_id, incident_id])
    if node:
        condition, node_params = node_condition(node)
        where.append(condition)
        params.extend(node_params)
    sql = (
        "SELECT seq, event_id, event_type, occurred_at, correlation_id, causation_id, producer, payload"
        " FROM events"
        + (" WHERE " + " AND ".join(where) if where else "")
        + " ORDER BY seq DESC LIMIT %s"
    )
    rows = conn.execute(sql, [*params, limit + 1]).fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    return {
        "events": [_row(r) for r in rows],
        "next_before_seq": rows[-1]["seq"] if more else None,
    }


def _row(r: dict[str, Any]) -> dict[str, Any]:
    payload = r["payload"]
    consumers = receivers(r["event_type"], payload)
    return {
        "seq": r["seq"],
        "event_id": str(r["event_id"]),
        "event_type": r["event_type"],
        "occurred_at": r["occurred_at"].isoformat(),
        "sender": r["producer"],
        "receiver": consumers[0],
        "receivers": consumers,
        "incident_id": payload.get("incident_id") or r["correlation_id"],
        "plan_version": payload.get("plan_version"),
        "partition": None,
        "offset": None,
        "causation_id": str(r["causation_id"]) if r["causation_id"] else None,
        "payload": payload,
    }
