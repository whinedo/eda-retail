"""synthetic-data: carga del conjunto generado en las tablas simuladas de negocio."""

from __future__ import annotations

from eda_retail.synthetic.generator import generate, load


def test_dataset_loads_orders_and_stock_once(conn):
    """El conjunto se carga en pedidos y existencias; cargarlo dos veces no duplica nada"""
    data = generate(seed=901, size=12)
    orders, stock = load(conn, data)
    assert orders == 12 and stock <= len(data.stock)  # la base de tests es compartida: puede haber stock previo
    assert load(conn, data) == (0, 0)
    ids = [o.order_id for o in data.orders]
    rows = conn.execute("SELECT order_id, carrier FROM orders WHERE order_id = ANY(%s)", (ids,)).fetchall()
    conn.commit()
    assert {r["order_id"]: r["carrier"] for r in rows} == {o.order_id: o.carrier for o in data.orders}
