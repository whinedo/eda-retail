"""incident-planning, «Reembolso en lugar de un reenvío caro»: el código sustituye el reenvío por un reembolso cuando
reenviar (importe + transporte + coste fijo) cuesta más del 120 % del importe del pedido."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from eda_retail.plan_validation import refund_over_reissue, validate_plan
from eda_retail.planner import handler as planner
from eda_retail.replanner import handler as replanner
from eda_retail.rules import BusinessRules

DEFAULTS = BusinessRules()
EXPENSIVE = replace(DEFAULTS, effect_cost_reissue=Decimal(15))  # 100 + 10 + 15 = 125 > 120
AMOUNTS = {"P1": Decimal("100.00")}


def _step(step_id: str, action: str, depends_on=(), order: str = "P1", **params) -> dict:
    return {"step_id": step_id, "action": action, "params": {"order_id": order, **params},
            "depends_on": list(depends_on)}


LOSS = [_step("S1", "notify_customer"), _step("S2", "reserve_stock"), _step("S3", "reissue_shipment", ["S2"])]


def _actions(steps) -> list[tuple[str, str]]:
    return [(s.step_id, s.action) for s in steps]


def test_reissue_more_expensive_than_the_refund():
    """Reenvío más caro que el reembolso: el plan del planificador lleva notify_customer y un issue_refund de P1 por
    100 €, sin reserve_stock ni reissue_shipment"""
    steps = planner.check_plan(LOSS, EXPENSIVE, AMOUNTS)
    assert _actions(steps) == [("S1", "notify_customer"), ("S4", "issue_refund")]
    refund = steps[1]
    assert refund.params == {"order_id": "P1", "amount": 100.0}  # número, como lo propondría el LLM
    assert refund.depends_on == []


def test_reissue_within_the_threshold():
    """Reenvío dentro del umbral: con EFFECT_COST_REISSUE 0 el reenvío cuesta 110 € y el plan no cambia"""
    steps = planner.check_plan(LOSS, DEFAULTS, AMOUNTS)
    assert _actions(steps) == [("S1", "notify_customer"), ("S2", "reserve_stock"), ("S3", "reissue_shipment")]


def test_reissue_exactly_at_the_threshold_is_kept():
    """Reenvío dentro del umbral: exactamente el 120 % no se sustituye (tiene que superarlo)"""
    rules = replace(DEFAULTS, effect_cost_reissue=Decimal(10))  # 100 + 10 + 10 = 120
    assert [s.action for s in planner.check_plan(LOSS, rules, AMOUNTS)][-1] == "reissue_shipment"


def test_refund_above_the_maximum_keeps_the_reissue():
    """Reembolso por encima del máximo: un pedido de 600 € con MAX_REFUND_AMOUNT 500 sigue con el reenvío"""
    steps = planner.check_plan(LOSS, EXPENSIVE, {"P1": Decimal("600.00")})
    assert [s.action for s in steps] == ["notify_customer", "reserve_stock", "reissue_shipment"]


def test_unknown_order_amount_keeps_the_reissue():
    """Sin el importe del pedido en view_orders no hay sustitución"""
    rules = EXPENSIVE
    steps = validate_plan(type("Ctx", (), {"rules": rules})(), LOSS, external_ids=set())
    assert refund_over_reissue(steps, rules, {}, taken_ids=set()) == steps


def test_dependents_of_the_reissue_move_to_the_refund():
    """Los pasos que dependían del reenvío pasan a depender del reembolso, y el step_id nuevo no repite uno usado"""
    plan = [*LOSS, _step("S4", "notify_customer", ["S3"])]
    steps = planner.check_plan(plan, EXPENSIVE, AMOUNTS)
    assert _actions(steps) == [("S1", "notify_customer"), ("S4", "notify_customer"), ("S5", "issue_refund")]
    assert steps[1].depends_on == ["S5"]


def test_reserve_used_by_another_step_is_kept():
    """La reserva solo se quita si nada más depende de ella"""
    plan = [*LOSS, _step("S4", "notify_customer", ["S2"])]
    steps = planner.check_plan(plan, EXPENSIVE, AMOUNTS)
    assert ("S2", "reserve_stock") in _actions(steps) and "reissue_shipment" not in [s.action for s in steps]


def test_revision_with_an_expensive_reissue():
    """Revisión con un reenvío caro: el replanner publica un issue_refund de P1 por su importe en lugar del reenvío,
    sin depender de la reserva completada (la saga la compensa al cancelar el reenvío)"""
    current = [
        {**_step("S1", "notify_customer"), "status": "completed", "origin_plan_version": 1},
        {**_step("S2", "reserve_stock"), "status": "completed", "origin_plan_version": 1},
        {**_step("S3", "reissue_shipment", ["S2"]), "status": "failed", "origin_plan_version": 1},
    ]
    steps = replanner.check_revision(current, [_step("S3", "reissue_shipment", ["S2"])], EXPENSIVE, AMOUNTS)
    assert _actions(steps) == [("S4", "issue_refund")]
    assert steps[0].depends_on == []


def test_revision_within_the_threshold_keeps_the_reissue():
    """Revisión con el reenvío dentro del umbral: se mantiene"""
    current = [{**_step("S2", "reserve_stock"), "status": "completed", "origin_plan_version": 1},
               {**_step("S3", "reissue_shipment", ["S2"]), "status": "failed", "origin_plan_version": 1}]
    steps = replanner.check_revision(current, [_step("S3", "reissue_shipment", ["S2"])], DEFAULTS, AMOUNTS)
    assert _actions(steps) == [("S3", "reissue_shipment")]


def test_rules_read_the_thresholds_from_the_environment(monkeypatch):
    """Los parámetros salen del entorno: SHIPPING_COST_PCT, EFFECT_COST_REISSUE y REFUND_OVER_REISSUE_PCT"""
    monkeypatch.setenv("SHIPPING_COST_PCT", "5")
    monkeypatch.setenv("EFFECT_COST_REISSUE", "2.5")
    monkeypatch.setenv("REFUND_OVER_REISSUE_PCT", "130")
    rules = BusinessRules()
    assert (rules.shipping_cost_pct, rules.effect_cost_reissue, rules.refund_over_reissue_pct) == (5, Decimal("2.5"), 130)
    monkeypatch.delenv("SHIPPING_COST_PCT")
    monkeypatch.delenv("EFFECT_COST_REISSUE")
    monkeypatch.delenv("REFUND_OVER_REISSUE_PCT")
    rules = BusinessRules()
    assert (rules.shipping_cost_pct, rules.effect_cost_reissue, rules.refund_over_reissue_pct) == (10, 0, 120)


def test_saga_compensates_the_completed_reserve_of_a_swapped_reissue():
    """Revisión con un reenvío caro: al aplicar la revisión con el reembolso, la saga cancela el reenvío y deriva el
    release_stock de la reserva completada"""
    from eda_retail.orchestrator import rules as saga

    def row(step: dict, status: str, result=None) -> dict:
        return {**step, "status": status, "origin_plan_version": 1, "compensates": None, "result": result}

    current = [row(_step("S2", "reserve_stock"), "completed", {"reservation_id": "R-1"}),
               row(_step("S3", "reissue_shipment", ["S2"]), "failed")]
    revised = [s.model_dump(mode="json") for s in
               replanner.check_revision(current, [_step("S3", "reissue_shipment", ["S2"])], EXPENSIVE, AMOUNTS)]
    revision = saga.revise(current, revised, new_version=2)
    assert revision.cancelled == ["S3"]
    assert [(c["step_id"], c["action"], c["params"]["reservation_id"]) for c in revision.compensations] == [
        ("S2-comp", "release_stock", "R-1")]
