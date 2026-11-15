"""Consulta de `order_status` (spec cqrs-views, «Estado de los pedidos»; spec web-front, «Vista de
analítica de pedidos»): solo lee `views.view_order_status` y `views.offsets`, nunca escribe y
responde con `applied` (offset aplicado por partición).

Los contadores cuentan todos los estados (con cero los que no tienen pedidos) y respetan el filtro de
incidencia, pero no el de estado: al filtrar por un estado, los demás contadores siguen visibles.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg

from eda_retail.projectors.order_status import PROJECTOR_NAME, STATUSES
from eda_retail.queries.positions import applied

MAX_ORDERS = 500


@dataclass(frozen=True)
class OrderStatusRow:
    order_id: str
    incident_id: str
    status: str
    last_event_type: str
    last_event_id: str
    occurred_at: datetime
    kafka_partition: int
    kafka_offset: int


@dataclass(frozen=True)
class OrderStatusView:
    counts: dict[str, int]
    orders: list[OrderStatusRow]
    applied: dict[int, int]


def get_order_status(
    conn: psycopg.Connection,
    *,
    status: str | None = None,
    incident_id: str | None = None,
    limit: int = MAX_ORDERS,
    topic: str | None = None,
) -> OrderStatusView:
    """Contadores por estado y pedidos, los cambiados más recientemente primero."""
    incident_filter, incident_params = ("WHERE incident_id = %s", [incident_id]) if incident_id else ("", [])
    where = [*(["incident_id = %s"] if incident_id else []), *(["status = %s"] if status else [])]
    params = [*incident_params, *([status] if status else [])]
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        counted = conn.execute(
            f"SELECT status, count(*) AS n FROM views.view_order_status {incident_filter} GROUP BY status",
            incident_params,
        ).fetchall()
        rows = conn.execute(
            "SELECT order_id, incident_id, status, last_event_type, last_event_id::text AS last_event_id, "
            "occurred_at, kafka_partition, kafka_offset FROM views.view_order_status"
            + (f" WHERE {' AND '.join(where)}" if where else "")
            + " ORDER BY occurred_at DESC, order_id LIMIT %s",
            [*params, limit],
        ).fetchall()
    counts = dict.fromkeys(STATUSES, 0) | {r["status"]: r["n"] for r in counted}
    return OrderStatusView(counts, [OrderStatusRow(**r) for r in rows], position)
