"""Vista `order_status` (spec cqrs-views, «Estado de los pedidos»): estado de negocio de cada pedido
afectado por una incidencia, para la analítica del front.

- `ShipmentDelayed` crea una fila `delayed` por cada pedido de su `order_ids`.
- `PlanCreated` y `PlanRevised` pasan a `in_resolution` los pedidos de la incidencia que aún no
  están en un estado final.
- `IncidentResolved` los pasa a su `outcome` (`resolved` o `refunded`) e `IncidentEscalated` a
  `escalated`. Un estado final no vuelve a `in_resolution`.

Solo `ShipmentDelayed` lleva pedidos: los demás eventos se aplican a los pedidos que su incidencia
registró antes. Los eventos de una incidencia viajan con la misma clave (`correlation_id`) y llegan
en orden a una sola partición. Cada handler fija valores absolutos: aplicar dos veces un evento no
cambia la vista.
"""

from __future__ import annotations

from typing import Any, Literal, get_args

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.positions import Position
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "order_status"
OrderStatus = Literal["delayed", "in_resolution", "resolved", "refunded", "escalated"]
STATUSES: tuple[str, ...] = get_args(OrderStatus)
FINAL_STATUSES = ("resolved", "refunded", "escalated")


def _delayed(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    for order_id in event.payload["order_ids"]:
        conn.execute(
            """
            INSERT INTO views.view_order_status (order_id, incident_id, status, last_event_type, last_event_id,
                                                 occurred_at, kafka_partition, kafka_offset)
            VALUES (%s, %s, 'delayed', %s, %s, %s, %s, %s)
            ON CONFLICT (order_id, incident_id) DO NOTHING
            """,
            (order_id, event.correlation_id, event.event_type, event.event_id, event.occurred_at,
             position.partition, position.offset),
        )


def _set_status(conn: psycopg.Connection, position: Position, event: EventEnvelope, status: str) -> None:
    conn.execute(
        """
        UPDATE views.view_order_status
        SET status = %s, last_event_type = %s, last_event_id = %s, occurred_at = %s,
            kafka_partition = %s, kafka_offset = %s
        WHERE incident_id = %s AND status <> ALL(%s)
        """,
        (status, event.event_type, event.event_id, event.occurred_at, position.partition, position.offset,
         event.correlation_id, list(FINAL_STATUSES)),
    )


def _in_resolution(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _set_status(conn, position, event, "in_resolution")


def _resolved(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _set_status(conn, position, event, event.payload["outcome"])


def _escalated(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    _set_status(conn, position, event, "escalated")


HANDLERS = {
    "ShipmentDelayed": _delayed,
    "PlanCreated": _in_resolution,
    "PlanRevised": _in_resolution,
    "IncidentResolved": _resolved,
    "IncidentEscalated": _escalated,
}


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
