"""Reglas de la saga de la base síncrona: las mismas que `command/saga.py` aplica con SQL."""

from __future__ import annotations

from eda_retail.baseline_sync.saga import Step, apply_revision, ready_steps


def _loss_plan() -> dict[str, Step]:
    return {
        "S1": Step("S1", "notify_customer", {"order_id": "P1"}, [], 1),
        "S2": Step("S2", "file_claim", {"order_id": "P1"}, [], 1),
        "S3": Step("S3", "reserve_stock", {"order_id": "P1"}, [], 1),
        "S4": Step("S4", "reissue_shipment", {"order_id": "P1"}, ["S3"], 1),
    }


def test_ready_steps_follow_dependencies_in_order():
    """Un paso está listo cuando sus dependencias están completadas; los listos van en orden"""
    steps = _loss_plan()
    assert [s.step_id for s in ready_steps(steps)] == ["S1", "S2", "S3"]
    steps["S3"].status = "completed"
    assert "S4" in [s.step_id for s in ready_steps(steps)]


def test_refund_waits_for_pending_compensations():
    """issue_refund no se pide mientras quede una compensación pendiente (paso pivote)"""
    steps = {
        "S3": Step("S3", "reserve_stock", {"order_id": "P1"}, [], 1, status="compensating"),
        "S3-comp": Step("S3-comp", "release_stock", {"order_id": "P1"}, [], 2, compensates="S3"),
        "S5": Step("S5", "issue_refund", {"order_id": "P1", "amount": 10.0}, [], 2),
    }
    assert [s.step_id for s in ready_steps(steps)] == ["S3-comp"]
    steps["S3-comp"].status, steps["S3"].status = "completed", "compensated"
    assert [s.step_id for s in ready_steps(steps)] == ["S5"]


def test_revision_cancels_compensates_and_keeps_origin():
    """Al revisar: se cancela lo no mantenido, se compensa la reserva huérfana y lo mantenido
    conserva su versión de origen"""
    steps = _loss_plan()
    for sid in ("S1", "S2", "S3"):
        steps[sid].status = "completed"
    steps["S3"].result = {"reservation_id": "R-1"}
    steps["S4"].status, steps["S4"].error = "failed", "injected_failure"
    apply_revision(steps, [{"step_id": "S5", "action": "issue_refund", "params": {"order_id": "P1", "amount": 10.0},
                            "depends_on": []}], 2)
    assert steps["S4"].status == "cancelled"
    comp = steps["S3-comp"]
    assert (comp.action, comp.compensates, comp.origin) == ("release_stock", "S3", 2)
    assert comp.params == {"order_id": "P1", "reservation_id": "R-1"}
    assert steps["S5"].origin == 2 and steps["S1"].origin == 1
    assert [s.step_id for s in ready_steps(steps)] == ["S3-comp"]


def test_kept_failed_step_is_retried_with_its_key():
    """Un paso fallido que el plan revisado mantiene vuelve a pending con su versión de origen"""
    steps = _loss_plan()
    steps["S2"].status, steps["S2"].error = "failed", "injected_failure"
    apply_revision(steps, [{"step_id": "S2", "action": "file_claim", "params": {"order_id": "P1"},
                            "depends_on": []}], 2)
    assert (steps["S2"].status, steps["S2"].error, steps["S2"].origin) == ("pending", None, 1)
    assert steps["S3"].status == "cancelled" and "S3-comp" not in steps
