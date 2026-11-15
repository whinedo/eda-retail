"""Reglas de la saga del orquestador (T7): decisiones sobre los pasos, sin base de datos."""

from __future__ import annotations

from eda_retail.infra.effect_keys import compute_idempotency_key
from eda_retail.orchestrator import rules


def _s(step_id: str, action: str, status: str = "pending", depends_on=(), *, origin=1, compensates=None,
       result=None, order="P-1") -> dict:
    return {"step_id": step_id, "action": action, "params": {"order_id": order}, "depends_on": list(depends_on),
            "status": status, "origin_plan_version": origin, "compensates": compensates, "result": result}


def _ids(steps) -> list[str]:
    return [s["step_id"] for s in steps]


def test_independent_steps_are_ready_together():
    """Pasos sin dependencias: se emiten los tres ExecuteStep sin esperar entre ellos"""
    steps = [_s("S1", "notify_customer"), _s("S2", "file_claim"), _s("S3", "reserve_stock")]
    assert _ids(rules.ready_steps(steps)) == ["S1", "S2", "S3"]


def test_a_step_waits_for_its_dependencies():
    """Paso con dependencia: reissue_shipment no se pide hasta que reserve_stock se completa"""
    steps = [_s("S1", "reserve_stock", "requested"), _s("S2", "reissue_shipment", depends_on=["S1"])]
    assert rules.ready_steps(steps) == []
    steps[0]["status"] = "completed"
    assert _ids(rules.ready_steps(steps)) == ["S2"]


def test_refund_is_held_while_a_compensation_is_pending():
    """Reembolso con una compensación pendiente: issue_refund espera a que release_stock termine"""
    steps = [_s("S1", "reserve_stock", "compensating"),
             _s("S1-comp", "release_stock", "requested", compensates="S1", origin=2),
             _s("S3", "issue_refund", origin=2)]
    assert rules.ready_steps(steps) == []
    steps[0]["status"], steps[1]["status"] = "compensated", "completed"
    assert _ids(rules.ready_steps(steps)) == ["S3"]


def test_an_escalated_incident_only_runs_compensations():
    steps = [_s("S1", "reserve_stock", "compensating"), _s("S1-comp", "release_stock", compensates="S1"),
             _s("S2", "notify_customer")]
    assert _ids(rules.ready_steps(steps, escalated=True)) == ["S1-comp"]


def test_plan_completed_is_resolved_or_refunded():
    """Plan completado: IncidentResolved con resolved, o refunded si hubo reembolso"""
    done = [_s("S1", "notify_customer", "completed"), _s("S2", "reserve_stock", "compensated"),
            _s("S3", "file_claim", "cancelled")]
    assert rules.outcome(done) == "resolved"
    assert rules.outcome([*done, _s("S4", "issue_refund", "completed")]) == "refunded"
    assert rules.outcome([*done, _s("S5", "file_claim", "failed")]) is None
    assert rules.outcome([]) is None


def test_compensations_run_in_reverse_order():
    """Orden inverso de las compensaciones: la del último paso va primero"""
    originals = [_s("S1", "reserve_stock", "completed", result={"reservation_id": "R1"}),
                 _s("S4", "reserve_stock", "completed", result={"reservation_id": "R4"}, order="P-2")]

    comps = rules.compensations(originals, plan_version=2)

    assert _ids(comps) == ["S4-comp", "S1-comp"]
    assert comps[0] | {"params": None} == {
        "step_id": "S4-comp", "action": "release_stock", "params": None, "depends_on": [], "status": "pending",
        "origin_plan_version": 2, "compensates": "S4", "result": None}
    assert comps[0]["params"] == {"order_id": "P-2", "reservation_id": "R4"}
    assert comps[1]["depends_on"] == ["S4-comp"]


def test_the_compensation_key_uses_the_comp_step_id():
    """La idempotency_key de la compensación se calcula con el step_id -comp"""
    [comp] = rules.compensations([_s("S3", "reserve_stock", "completed")], plan_version=1)
    request = rules.step_request("I-1", 2, comp)
    assert request["idempotency_key"] == compute_idempotency_key("I-1", "S3-comp", origin_plan_version=1)
    assert request["params"] == {"order_id": "P-1", "reservation_id": None}


def test_step_request_carries_what_the_executor_needs():
    request = rules.step_request("I-1", 3, _s("S2", "file_claim", origin=1))
    assert request == {"incident_id": "I-1", "plan_version": 3, "step_id": "S2", "action": "file_claim",
                       "params": {"order_id": "P-1"},
                       "idempotency_key": compute_idempotency_key("I-1", "S2", origin_plan_version=1)}


def test_failure_routing():
    """Fallos: compensación -> compensation_failed; reintentos agotados; sin revisiones -> max_revisions"""
    step = _s("S2", "reissue_shipment", "requested")
    comp = _s("S1-comp", "release_stock", "requested", compensates="S1")
    assert rules.on_failure(comp, retryable=False, revisions=0, max_revisions=2) == ("escalate", "compensation_failed")
    assert rules.on_failure(step, retryable=True, revisions=0, max_revisions=2) == ("escalate", "retries_exhausted")
    assert rules.on_failure(step, retryable=False, revisions=2, max_revisions=2) == ("escalate", "max_revisions")
    assert rules.on_failure(step, retryable=False, revisions=1, max_revisions=2) == ("revise", None)


def test_revision_keeps_ids_and_adds_new_steps():
    """Paso mantenido en una revisión: conserva step_id y versión de origen; los nuevos llevan la nueva"""
    steps = [_s("S1", "notify_customer", "completed"), _s("S2", "reserve_stock", "failed"),
             _s("S3", "reissue_shipment", depends_on=["S2"])]
    revised = [{"step_id": "S2", "action": "reserve_stock", "params": {"order_id": "P-1"}, "depends_on": []},
               {"step_id": "S4", "action": "issue_refund", "params": {"order_id": "P-1", "amount": 10},
                "depends_on": []}]

    change = rules.revise(steps, revised, new_version=2)

    by_id = {s["step_id"]: s for s in change.steps}
    assert by_id["S2"]["origin_plan_version"] == 1 and by_id["S2"]["status"] == "pending"
    assert by_id["S4"]["origin_plan_version"] == 2 and by_id["S4"]["status"] == "pending"
    assert by_id["S3"]["status"] == "cancelled"
    assert change.cancelled == ["S3"]
    assert change.compensations == []


def test_revision_compensates_a_reservation_whose_dependent_is_cancelled():
    """Fallo del reenvío tras reservar: el código deriva release_stock"""
    steps = [_s("S1", "reserve_stock", "completed", result={"reservation_id": "R1"}),
             _s("S2", "reissue_shipment", "failed", depends_on=["S1"])]
    revised = [{"step_id": "S3", "action": "issue_refund", "params": {"order_id": "P-1", "amount": 10},
                "depends_on": []}]

    change = rules.revise(steps, revised, new_version=2)

    assert change.cancelled == ["S2"]
    assert _ids(change.compensations) == ["S1-comp"]
    assert change.compensations[0]["params"]["reservation_id"] == "R1"


def test_a_kept_requested_step_is_requested_again():
    steps = [_s("S1", "notify_customer", "requested"), _s("S2", "file_claim", "failed")]
    revised = [{"step_id": "S1", "action": "notify_customer", "params": {"order_id": "P-1"}, "depends_on": []}]
    change = rules.revise(steps, revised, new_version=2)
    assert {s["step_id"]: s["status"] for s in change.steps} == {"S1": "pending", "S2": "cancelled"}


def test_escalation_cancels_pending_steps_and_compensates_reservations():
    """Escalado con una reserva hecha: reenvío cancelado y compensación de la reserva"""
    steps = [_s("S1", "reserve_stock", "completed", result={"reservation_id": "R1"}),
             _s("S2", "reissue_shipment", depends_on=["S1"]), _s("S3", "notify_customer", "completed"),
             _s("S4", "issue_refund")]

    cancelled, comps = rules.escalation(steps, "llm_output_invalid", plan_version=1)

    assert cancelled == ["S2", "S4"]
    assert _ids(comps) == ["S1-comp"]


def test_escalation_after_a_failed_compensation_does_not_compensate_again():
    """Liberación falla: se escala sin volver a compensar"""
    steps = [_s("S1", "reserve_stock", "compensating"),
             _s("S1-comp", "release_stock", "failed", compensates="S1"),
             _s("S2", "reissue_shipment", "cancelled", depends_on=["S1"])]
    assert rules.escalation(steps, "compensation_failed", plan_version=1) == ([], [])


def test_a_reservation_whose_dependent_completed_is_not_compensated():
    steps = [_s("S1", "reserve_stock", "completed"), _s("S2", "reissue_shipment", "completed", depends_on=["S1"])]
    assert rules.escalation(steps, "plan_timeout", plan_version=1) == ([], [])
