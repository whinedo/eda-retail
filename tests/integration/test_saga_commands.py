"""AdvancePlan, ApplyRevision y resultados obsoletos (lado de escritura de la orquestación)."""

from __future__ import annotations

import uuid

from eda_retail.command.models import AdvancePlan, ApplyRevision
from eda_retail.infra.effect_keys import compute_idempotency_key
from eda_retail.projectors.incident_status import build_projector

from .command_helpers import add_order, events_of, handler, open_incident, version_of
from .test_plans import LOSS_PLAN, create, plan_steps, revise, step
from .test_steps import record


def advance(conn, incident: str):
    result = handler().handle(
        conn,
        AdvancePlan(command_id=uuid.uuid4(), expected_version=version_of(conn, incident), producer="orchestrator",
                    incident_id=incident),
    )
    conn.commit()
    return result


def apply_revision(conn, incident: str, plan_version: int, step_ids: list[str]):
    result = handler().handle(
        conn,
        ApplyRevision(command_id=uuid.uuid4(), expected_version=version_of(conn, incident), producer="orchestrator",
                      incident_id=incident, plan_version=plan_version, step_ids=step_ids),
    )
    conn.commit()
    return result


def requested(result) -> list[str]:
    return [e.payload["step_id"] for e in result.events if e.event_type == "StepRequested"]


def complete(conn, incident: str, step_id: str, result: dict, effect: dict):
    assert record(conn, incident, step_id, result=result, effect=effect).accepted


def reserve(conn, incident: str, step_id: str = "S3", reservation: str = "R1", order: str = "P1"):
    complete(conn, incident, step_id, {"reservation_id": reservation},
             {"order_id": order, "warehouse": "W1", "reservation_id": reservation})


def done_first_three(conn, incident: str) -> None:
    """LOSS_PLAN con S1, S2 y S3 completados y S4 (reissue_shipment) pedido."""
    create(conn, incident, LOSS_PLAN)
    advance(conn, incident)
    complete(conn, incident, "S1", {"message_id": "M1"}, {"order_id": "P1", "message_id": "M1"})
    complete(conn, incident, "S2", {"claim_id": "C1"}, {"order_id": "P1", "claim_id": "C1"})
    reserve(conn, incident)
    assert requested(advance(conn, incident)) == ["S4"]


def test_advance_requests_every_ready_step(conn):
    """Pasos sin dependencias"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    before = version_of(conn, incident)
    result = advance(conn, incident)
    assert result.accepted
    assert requested(result) == ["S1", "S2", "S3"]
    assert version_of(conn, incident) == before + 1  # un solo comando, una sola versión


def test_advance_waits_for_dependencies(conn):
    """Paso con dependencia"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    assert "S4" not in requested(advance(conn, incident))
    reserve(conn, incident)
    assert requested(advance(conn, incident)) == ["S4"]


def test_advance_with_nothing_ready_writes_nothing(conn):
    """AdvancePlan sin pasos listos no escribe eventos ni cambia la versión"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    advance(conn, incident)
    before, count = version_of(conn, incident), len(events_of(conn, incident))
    result = advance(conn, incident)
    assert (result.accepted, result.events) == (True, [])
    assert (version_of(conn, incident), len(events_of(conn, incident))) == (before, count)


def test_plan_completed_resolves_incident(conn):
    """Plan completado"""
    incident, _ = open_incident(conn)
    done_first_three(conn, incident)
    complete(conn, incident, "S4", {"new_shipment_id": "SH1"}, {"order_id": "P1", "new_shipment_id": "SH1"})
    result = advance(conn, incident)
    assert [e.event_type for e in result.events] == ["IncidentResolved"]
    assert result.events[0].payload["outcome"] == "resolved"


def test_plan_completed_with_refund_is_refunded(conn):
    """Plan completado: outcome refunded si el plan vigente terminó con un reembolso"""
    incident, _ = open_incident(conn)
    create(conn, incident, [step("S1", "issue_refund", order_id="P1", amount="30")])
    advance(conn, incident)
    complete(conn, incident, "S1", {"refund_id": "RF1"}, {"order_id": "P1", "amount": "30", "refund_id": "RF1"})
    [ev] = advance(conn, incident).events
    assert (ev.event_type, ev.payload["outcome"]) == ("IncidentResolved", "refunded")


def test_failed_step_does_not_resolve(conn):
    """Un paso fallido no deja cerrar la incidencia: espera a la revisión del plan"""
    incident, _ = open_incident(conn)
    create(conn, incident, [step("S1", "file_claim", order_id="P1")])
    advance(conn, incident)
    assert record(conn, incident, "S1", outcome="failed", error="order_closed", retryable=False).accepted
    assert advance(conn, incident).events == []


def test_reissue_failure_after_reserving_stock(conn):
    """Fallo del reenvío tras reservar stock"""
    incident, _ = open_incident(conn)
    done_first_three(conn, incident)
    assert record(conn, incident, "S4", outcome="failed", error="carrier_rejected", retryable=False).accepted
    assert revise(conn, incident, [step("S5", "issue_refund", order_id="P1", amount="30")],
                  reason="carrier_rejected").accepted

    result = apply_revision(conn, incident, 2, ["S5"])
    assert result.accepted
    assert [(e.event_type, e.payload["step_id"]) for e in result.events] == [("StepCancelled", "S4")]
    rows = plan_steps(conn, incident)
    assert rows["S4"]["status"] == "cancelled"
    comp = conn.execute(
        "SELECT action, params, compensates, status, origin_plan_version FROM plan_steps "
        "WHERE incident_id = %s AND step_id = 'S3-comp'", (incident,),
    ).fetchone()
    conn.commit()
    assert dict(comp) == {
        "action": "release_stock", "params": {"order_id": "P1", "reservation_id": "R1"},
        "compensates": "S3", "status": "pending", "origin_plan_version": 2,
    }
    # La compensación la pide el código; el reembolso espera a que termine.
    adv = advance(conn, incident)
    assert requested(adv) == ["S3-comp"]
    [req] = [e for e in adv.events if e.event_type == "StepRequested"]
    assert req.payload["idempotency_key"] == compute_idempotency_key(incident, "S3-comp", origin_plan_version=2)
    assert plan_steps(conn, incident)["S3"]["status"] == "compensating"


def test_refund_waits_for_pending_compensation(conn):
    """Reembolso con una compensación pendiente"""
    incident, _ = open_incident(conn)
    done_first_three(conn, incident)
    record(conn, incident, "S4", outcome="failed", error="carrier_rejected", retryable=False)
    revise(conn, incident, [step("S5", "issue_refund", order_id="P1", amount="30")], reason="carrier_rejected")
    apply_revision(conn, incident, 2, ["S5"])
    assert "S5" not in requested(advance(conn, incident))
    assert advance(conn, incident).events == []
    complete(conn, incident, "S3-comp", {"released": True}, {"reservation_id": "R1"})
    assert plan_steps(conn, incident)["S3"]["status"] == "compensated"
    assert requested(advance(conn, incident)) == ["S5"]


def test_failed_compensation_blocks_refund(conn):
    """La liberación del stock falla: no se emite ningún reembolso"""
    incident, _ = open_incident(conn)
    done_first_three(conn, incident)
    record(conn, incident, "S4", outcome="failed", error="carrier_rejected", retryable=False)
    revise(conn, incident, [step("S5", "issue_refund", order_id="P1", amount="30")], reason="carrier_rejected")
    apply_revision(conn, incident, 2, ["S5"])
    advance(conn, incident)
    assert record(conn, incident, "S3-comp", outcome="failed", error="timeout", retryable=True, attempts=3).accepted
    assert advance(conn, incident).events == []
    assert "RefundIssued" not in [e["event_type"] for e in events_of(conn, incident)]


def test_compensations_run_in_reverse_order(conn):
    """Cuando haya varias compensaciones, se ejecutan en orden inverso al de los pasos originales"""
    p2 = add_order(conn)
    incident, _ = open_incident(conn)
    plan = [
        step("S1", "reserve_stock", order_id="P1"),
        step("S2", "reserve_stock", order_id=p2),
        step("S3", "reissue_shipment", ["S1", "S2"], order_id="P1"),
    ]
    create(conn, incident, plan)
    advance(conn, incident)
    reserve(conn, incident, "S1", "R1", "P1")
    reserve(conn, incident, "S2", "R2", p2)
    advance(conn, incident)
    record(conn, incident, "S3", outcome="failed", error="carrier_rejected", retryable=False)
    revise(conn, incident, [step("S4", "issue_refund", order_id="P1", amount="30")], reason="carrier_rejected")
    apply_revision(conn, incident, 2, ["S4"])
    rows = plan_steps(conn, incident)
    assert rows["S2-comp"]["depends_on"] == []
    assert rows["S1-comp"]["depends_on"] == ["S2-comp"]
    assert requested(advance(conn, incident)) == ["S2-comp"]


def test_kept_requested_step_is_requested_again(conn):
    """Un paso mantenido que seguía pedido se vuelve a pedir en la versión nueva con la misma clave"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    first = {e.payload["step_id"]: e.payload for e in advance(conn, incident).events}
    record(conn, incident, "S3", outcome="failed", error="out_of_stock", retryable=False)
    revise(conn, incident, [step("S2", "file_claim", order_id="P1"),
                            step("S5", "issue_refund", order_id="P1", amount="30")])
    result = apply_revision(conn, incident, 2, ["S2", "S5"])
    assert sorted((e.event_type, e.payload["step_id"]) for e in result.events) == [
        ("StepCancelled", "S1"), ("StepCancelled", "S3"), ("StepCancelled", "S4"),
    ]
    again = {e.payload["step_id"]: e.payload for e in advance(conn, incident).events}
    assert again["S2"]["plan_version"] == 2
    assert again["S2"]["idempotency_key"] == first["S2"]["idempotency_key"]


def test_result_of_an_old_version(conn):
    """Resultado de una versión antigua"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    advance(conn, incident)
    record(conn, incident, "S3", outcome="failed", error="out_of_stock", retryable=False)
    revise(conn, incident, [step("S2", "file_claim", order_id="P1")])
    apply_revision(conn, incident, 2, ["S2"])
    advance(conn, incident)
    stale = record(conn, incident, "S2", outcome="failed", error="timeout", retryable=True, plan_version=1)
    assert stale.accepted
    [ev] = stale.events
    assert (ev.event_type, ev.payload["plan_version"]) == ("StepFailed", 1)  # queda en el log
    assert plan_steps(conn, incident)["S2"]["status"] == "requested"  # no se usa para decidir


def test_late_completion_of_cancelled_compensable_step(conn):
    """Un paso compensable cancelado que termina después con efecto recibe su compensación"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    advance(conn, incident)
    record(conn, incident, "S2", outcome="failed", error="order_closed", retryable=False)
    revise(conn, incident, [step("S5", "issue_refund", order_id="P1", amount="30")], reason="order_closed")
    apply_revision(conn, incident, 2, ["S5"])
    assert plan_steps(conn, incident)["S3"]["status"] == "cancelled"
    late = record(conn, incident, "S3", result={"reservation_id": "R1"},
                  effect={"order_id": "P1", "warehouse": "W1", "reservation_id": "R1"}, plan_version=1)
    assert late.accepted
    assert [e.event_type for e in late.events] == ["StepCompleted", "StockReserved"]
    assert plan_steps(conn, incident)["S3-comp"]["status"] == "pending"
    assert requested(advance(conn, incident)) == ["S3-comp"]


def test_apply_revision_rejections(conn):
    """ApplyRevision de una versión que no es la vigente, o con pasos desconocidos, se rechaza"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    assert apply_revision(conn, incident, 1, ["S1"]).reason == "stale_revision"
    revise(conn, incident, [step("S1", "notify_customer", order_id="P1")])
    assert apply_revision(conn, incident, 2, ["S9"]).reason == "step_not_found"
    assert apply_revision(conn, incident, 2, ["S1"]).accepted
    assert apply_revision(conn, incident, 2, ["S1"]).reason == "revision_applied"


def test_states_after_a_compensation(conn):
    """Estados tras una compensación"""
    incident, _ = open_incident(conn)
    done_first_three(conn, incident)
    record(conn, incident, "S4", outcome="failed", error="carrier_rejected", retryable=False)
    revise(conn, incident, [step("S5", "issue_refund", order_id="P1", amount="30")], reason="carrier_rejected")
    apply_revision(conn, incident, 2, ["S5"])
    advance(conn, incident)
    projector = build_projector()

    def view() -> dict[str, str]:
        while projector.run_once(conn):
            pass
        row = conn.execute("SELECT steps FROM view_incident_status WHERE incident_id = %s", (incident,)).fetchone()
        conn.commit()
        return {s["step_id"]: s["status"] for s in row["steps"]}

    states = view()
    assert (states["S3"], states["S4"], states["S3-comp"]) == ("compensating", "cancelled", "requested")
    complete(conn, incident, "S3-comp", {"released": True}, {"reservation_id": "R1"})
    assert view()["S3"] == "compensated"
