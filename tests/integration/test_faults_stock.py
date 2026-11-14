"""synthetic-data: existencias simuladas y almacenes sin stock."""

from __future__ import annotations

import uuid

import pytest

from eda_retail.events.catalog import build_event
from eda_retail.executors.actions import OUT_OF_STOCK, ReserveStock
from eda_retail.executors.base import BusinessError, StepRequest
from eda_retail.synthetic.faults import empty_warehouses


def _order(conn, sku: str, quantity: int) -> str:
    order_id = f"P-{uuid.uuid4().hex[:8]}"
    conn.execute(
        "INSERT INTO orders (order_id, customer_ref, sku, quantity, amount, carrier) "
        "VALUES (%s, 'CUST-0001', %s, %s, 10, 'demo')",
        (order_id, sku, quantity),
    )
    return order_id


def _request(order_id: str) -> StepRequest:
    return StepRequest.of(build_event("StepRequested", {
        "incident_id": "I1", "plan_version": 1, "step_id": "S3", "action": "reserve_stock",
        "params": {"order_id": order_id}, "idempotency_key": uuid.uuid4().hex,
    }, correlation_id="I1", causation_id=None, producer="test"))


def test_simulated_stock_reservation(conn):
    """Reserva de stock simulada"""
    sku = f"A1-{uuid.uuid4().hex[:6]}"
    with conn.transaction():
        conn.execute("INSERT INTO stock (warehouse, sku, available) VALUES ('W9', %s, 3)", (sku,))
        order_id = _order(conn, sku, 4)
    with pytest.raises(BusinessError) as exc, conn.transaction():
        ReserveStock().execute(conn, _request(order_id), None)
    assert exc.value.error == OUT_OF_STOCK


def test_empty_warehouse_leaves_no_stock(conn):
    """Dejar sin stock un almacén: ninguna reserva sale de él"""
    sku = f"A2-{uuid.uuid4().hex[:6]}"
    with conn.transaction():
        conn.execute("INSERT INTO stock (warehouse, sku, available) VALUES ('W8', %s, 50)", (sku,))
        order_id = _order(conn, sku, 1)
    assert empty_warehouses(conn, ["W8"]) >= 1
    with pytest.raises(BusinessError) as exc, conn.transaction():
        ReserveStock().execute(conn, _request(order_id), None)
    assert exc.value.error == OUT_OF_STOCK
