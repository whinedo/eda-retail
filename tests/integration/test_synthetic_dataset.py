"""synthetic-data: el conjunto generado se siembra en `inventory` con `SeedSampleData`."""

from __future__ import annotations

from eda_retail.config import Settings
from eda_retail.experiments.harness import seed_payload
from eda_retail.inventory.handler import SAMPLE_DATA_CORRELATION, specs
from eda_retail.synthetic.generator import generate

from .component_helpers import body, client


def test_dataset_loads_orders_and_stock_once(database_url, conn):
    """El conjunto se carga en pedidos y existencias; cargarlo dos veces no duplica nada"""
    data = generate(seed=901, size=12)
    with client(database_url, "inventory", specs(Settings())) as api:
        first = api.post("/commands/SeedSampleData", json=body(SAMPLE_DATA_CORRELATION, seed_payload(data)))
        again = api.post("/commands/SeedSampleData", json=body(SAMPLE_DATA_CORRELATION, seed_payload(data)))
    assert first.json()["orders"] == 12
    assert again.json()["orders"] == 0 and again.json()["stock"] == 0
    ids = [o.order_id for o in data.orders]
    with conn.transaction():
        rows = conn.execute("SELECT order_id, carrier FROM inventory.orders WHERE order_id = ANY(%s)",
                            (ids,)).fetchall()
    assert {r["order_id"]: r["carrier"] for r in rows} == {o.order_id: o.carrier for o in data.orders}
