"""cqrs-views: proyección view_orders (de OrderRegistered) y su consulta («Dato de otro dominio»)."""

from __future__ import annotations

import uuid
from decimal import Decimal

from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.queries.orders import get_orders

from .projection_helpers import catch_up, publish


def _order(order_id: str, amount: str = "49.90") -> EventEnvelope:
    return build_event("OrderRegistered", {"order_id": order_id, "customer_ref": f"C-{order_id}", "amount": amount},
                       correlation_id=order_id, causation_id=None, producer="inventory")


def test_other_domain_data_comes_from_the_projection(conn, settings):
    """Dato de otro dominio: el importe del pedido se consulta en view_orders, no en inventory"""
    order_id = f"P-{uuid.uuid4().hex[:8]}"
    [registered] = publish(settings, [_order(order_id, "120.00")])
    catch_up(settings, conn, "orders")

    orders = get_orders(conn, [order_id, "P-no-existe"], topic=settings.events_topic)

    assert orders.found[order_id].amount == Decimal("120.00")
    assert orders.found[order_id].customer_ref == f"C-{order_id}"
    assert orders.missing([order_id, "P-no-existe"]) == ["P-no-existe"]
    assert orders.applied[registered.partition] >= registered.offset


def test_lagging_projection_reports_its_position(conn, settings):
    """Lectura justo después de escribir: la proyección aún no tiene el pedido y su posición lo dice"""
    order_id = f"P-{uuid.uuid4().hex[:8]}"
    [registered] = publish(settings, [_order(order_id)])

    orders = get_orders(conn, [order_id], topic=settings.events_topic)

    assert orders.missing([order_id]) == [order_id]
    assert orders.applied.get(registered.partition, -1) < registered.offset


def test_order_registered_twice_keeps_one_row(conn, settings):
    """Evento duplicado en un proyector: el pedido se guarda una vez"""
    order_id = f"P-{uuid.uuid4().hex[:8]}"
    event = _order(order_id)
    publish(settings, [event, event])
    catch_up(settings, conn, "orders")
    count = conn.execute("SELECT count(*) AS n FROM views.view_orders WHERE order_id = %s", (order_id,)).fetchone()
    conn.commit()
    assert count["n"] == 1
