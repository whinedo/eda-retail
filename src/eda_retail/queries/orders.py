"""Consulta de `orders` (spec cqrs-views, «Dato de otro dominio»): solo lee `views.view_orders` y
`views.offsets`, nunca las tablas de `inventory`, y responde con `applied`."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

import psycopg

from eda_retail.projectors.orders import PROJECTOR_NAME
from eda_retail.queries.positions import applied


@dataclass(frozen=True)
class Order:
    order_id: str
    customer_ref: str
    amount: Decimal


@dataclass(frozen=True)
class Orders:
    found: dict[str, Order]  # los pedidos que la vista ya tiene
    applied: dict[int, int]

    def missing(self, order_ids: list[str]) -> list[str]:
        """Pedidos pedidos que la proyección aún no tiene (puede ir atrasada)."""
        return [o for o in order_ids if o not in self.found]


def get_orders(conn: psycopg.Connection, order_ids: list[str], *, topic: str | None = None) -> Orders:
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        rows = conn.execute(
            "SELECT order_id, customer_ref, amount FROM views.view_orders WHERE order_id = ANY(%s)", (order_ids,)
        ).fetchall()
    return Orders({r["order_id"]: Order(**r) for r in rows}, position)
