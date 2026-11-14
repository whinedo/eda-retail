"""synthetic-data: generador de avisos con etiquetas de causa y plan esperado."""

from __future__ import annotations

import json
import re
from collections import Counter

from eda_retail.events.catalog import Action, Cause, PlanStep
from eda_retail.synthetic.generator import VARIANTS, expected_plan, generate


def test_evaluation_set():
    """Conjunto de evaluación"""
    data = generate(seed=7, size=50)
    assert len(data.notices) == 50
    counts = Counter(n.cause for n in data.notices)
    assert set(counts) == set(VARIANTS) == {c.value for c in Cause}
    assert min(counts.values()) >= 5
    for n in data.notices:
        assert n.expected_steps, n
        for step in n.expected_steps:
            PlanStep.model_validate(step)  # mismo formato que un plan del planificador
            assert step["action"] in {a.value for a in Action}


def test_same_seed_twice():
    """Dos ejecuciones con la misma semilla"""
    assert generate(seed=42, size=50) == generate(seed=42, size=50)
    assert generate(seed=42, size=50).to_json() == generate(seed=42, size=50).to_json()
    assert generate(seed=43, size=50) != generate(seed=42, size=50)


def test_free_text_and_api_formats():
    """Cada aviso se ofrece en texto libre y en formato de API, con la misma causa y pedido"""
    for n in generate(seed=1, size=30).notices:
        assert n.order_id in n.text
        api = json.loads(n.api_text)
        assert api["order_id"] == n.order_id and api["carrier"] == n.carrier
        assert api["notice_id"] == n.carrier_notice_id
        assert n.raw_text("text") == n.text and n.raw_text("api") == n.api_text


def test_notices_carry_no_personal_data():
    """Los avisos y los pedidos solo llevan referencias, nunca datos personales"""
    data = generate(seed=3, size=50)
    blob = data.to_json()
    assert not re.search(r"[\w.]+@[\w.]+", blob)  # sin correos
    assert all(re.fullmatch(r"CUST-\d{4}", o.customer_ref) for o in data.orders)


def test_expected_plan_follows_incident_planning():
    """El plan esperado sigue incident-planning: retraso leve y pérdida del paquete"""
    actions = lambda cause, severity: [s["action"] for s in expected_plan(cause, severity, "P1")]
    assert actions("delay", "low") == ["notify_customer"]
    assert actions("loss", "high") == ["notify_customer", "file_claim", "reserve_stock", "reissue_shipment"]
    reissue = expected_plan("loss", "high", "P1")[3]
    assert reissue["depends_on"] == ["S3"]
    assert actions("strike", "low") == ["notify_customer"]  # cualquier gravedad baja: solo aviso


def test_every_notice_has_an_order_and_stock():
    """Cada aviso tiene su pedido, y cada artículo tiene existencias en algún almacén"""
    data = generate(seed=5, size=40)
    orders = {o.order_id: o for o in data.orders}
    assert {n.order_id for n in data.notices} == set(orders)
    stocked = {s.sku for s in data.stock if s.available > 0}
    assert {o.sku for o in data.orders} <= stocked
