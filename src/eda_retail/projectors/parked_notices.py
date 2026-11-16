"""Vista `parked_notices` (spec cqrs-views, «Avisos aparcados»): los avisos que el intérprete aparcó porque
`view_orders` aún no tenía ninguno de sus pedidos, para el aviso del front (spec web-front, «Aviso de avisos
aparcados»).

- `NoticeParked` crea la fila de la incidencia con los pedidos que espera y su plazo.
- `ShipmentDelayed` (lo reanudó un `OrderRegistered` o su plazo con el pedido ya proyectado) e
  `IncidentEscalated` (venció su plazo sin pedidos) de esa incidencia la borran.

Los eventos de una incidencia viajan con la misma clave (`correlation_id`) y llegan en orden a una sola
partición. Aplicar dos veces un evento no cambia la vista: la inserción no pisa una fila existente y el
borrado de una fila que ya no está no hace nada.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.positions import Position
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "parked_notices"


def _parked(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    conn.execute(
        """
        INSERT INTO views.view_parked_notices (incident_id, order_ids, deadline_at, parked_at, last_event_id,
                                               kafka_partition, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (incident_id) DO NOTHING
        """,
        (event.payload["incident_id"], event.payload["order_ids"], datetime.fromisoformat(event.payload["deadline_at"]),
         event.occurred_at, event.event_id, position.partition, position.offset),
    )


def _resolved(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    conn.execute("DELETE FROM views.view_parked_notices WHERE incident_id = %s", (event.payload["incident_id"],))


HANDLERS = {
    "NoticeParked": _parked,
    "ShipmentDelayed": _resolved,
    "IncidentEscalated": _resolved,
}


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
