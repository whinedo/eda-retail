"""Página de eventos del log para el panel (spec web-front, "Página de eventos").

Solo lee `events`; la partición y el offset de Kafka no se guardan todavía (T10) y van a None.
"""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.web.routing import node_condition, receivers
from eda_retail.web.traces import trace_id_from_context

DEFAULT_LIMIT = 100
MAX_LIMIT = 500
_SELECT = (
    "SELECT seq, event_id, event_type, occurred_at, correlation_id, causation_id, producer, payload,"
    " (SELECT o.trace_context FROM outbox o WHERE o.seq = events.seq) AS trace_context FROM events"
)


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
        _SELECT
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


def events_after(conn: psycopg.Connection, after_seq: int, limit: int = MAX_LIMIT) -> list[dict[str, Any]]:
    """Eventos con `seq` mayor que `after_seq`, en orden de `seq` (stream en vivo)."""
    rows = conn.execute(f"{_SELECT} WHERE seq > %s ORDER BY seq LIMIT %s", (after_seq, limit)).fetchall()
    return [_row(r) for r in rows]


def last_seq(conn: psycopg.Connection) -> int:
    return conn.execute("SELECT coalesce(max(seq), 0) AS seq FROM events").fetchone()["seq"]


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
        "trace_id": trace_id_from_context(r.get("trace_context")),
    }
