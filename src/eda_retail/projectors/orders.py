"""Vista `orders` (spec cqrs-views, «Vistas mínimas» y «Dato de otro dominio»): pedido, importe y
`customer_ref` de cada `OrderRegistered`. Los sistemas que necesitan un pedido (intérprete,
planificador) la consultan aquí; nunca leen las tablas de `inventory`.

Un pedido registrado dos veces conserva sus datos (`ON CONFLICT DO NOTHING`).
"""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.positions import Position
from eda_retail.projectors.base import Projector

PROJECTOR_NAME = "orders"


def _registered(conn: psycopg.Connection, position: Position, event: EventEnvelope) -> None:
    p = event.payload
    conn.execute(
        "INSERT INTO views.view_orders (order_id, customer_ref, amount) VALUES (%s, %s, %s) "
        "ON CONFLICT (order_id) DO NOTHING",
        (p["order_id"], p["customer_ref"], p["amount"]),
    )


HANDLERS = {"OrderRegistered": _registered}


def build_projector(**kwargs: Any) -> Projector:
    return Projector(PROJECTOR_NAME, HANDLERS, **kwargs)
