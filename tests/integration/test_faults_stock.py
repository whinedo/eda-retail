"""synthetic-data: almacenes sin existencias en la siembra de `inventory` («Dejar sin stock un almacén»).

«Reserva de stock simulada» la cubre `test_inventory.py` (3 unidades, se piden 4, `out_of_stock`).
"""

from __future__ import annotations

import uuid

from eda_retail.config import Settings
from eda_retail.experiments.harness import seed_payload
from eda_retail.inventory.handler import SAMPLE_DATA_CORRELATION, specs
from eda_retail.synthetic.generator import generate

from .component_helpers import body, client, outbox


def test_empty_warehouse_leaves_no_stock(database_url, conn):
    """Dejar sin stock un almacén: se siembra a 0 y ninguna reserva sale de él"""
    data = generate(seed=917, size=1)
    tag = uuid.uuid4().hex[:8]
    order = data.orders[0]
    payload = seed_payload(data, tuple(s.warehouse for s in data.stock))
    payload["orders"] = [o | {"order_id": f"{o['order_id']}-{tag}", "sku": f"{o['sku']}-{tag}"} for o in payload["orders"]]
    payload["stock"] = [s | {"sku": f"{s['sku']}-{tag}"} for s in payload["stock"]]
    incident = f"I-{tag}"
    with client(database_url, "inventory", specs(Settings())) as api:
        assert api.post("/commands/SeedSampleData", json=body(SAMPLE_DATA_CORRELATION, payload)).status_code == 202
        step = {"incident_id": incident, "plan_version": 1, "step_id": "S3", "action": "reserve_stock",
                "params": {"order_id": f"{order.order_id}-{tag}"}, "idempotency_key": f"k-{tag}"}
        assert api.post("/commands/ExecuteStep", json=body(incident, step)).status_code == 202
    [failed] = outbox(conn, "inventory", incident)
    assert (failed["event_type"], failed["payload"]["error"]) == ("StepFailed", "out_of_stock")
    with conn.transaction():
        rows = conn.execute("SELECT available FROM inventory.stock WHERE sku = ANY(%s)",
                            ([s["sku"] for s in payload["stock"]],)).fetchall()
    assert rows and all(r["available"] == 0 for r in rows)
