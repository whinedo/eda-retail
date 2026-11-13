"""incident-planning y llm-boundaries: validación de planes en el command handler."""

from __future__ import annotations

import uuid

import pytest

from eda_retail.command.models import CreatePlan, RevisePlan

from .command_helpers import events_of, handler, open_incident, version_of


def step(step_id: str, action: str, depends_on: list[str] | None = None, **params) -> dict:
    return {"step_id": step_id, "action": action, "params": params, "depends_on": depends_on or []}


LOSS_PLAN = [
    step("S1", "notify_customer", order_id="P1"),
    step("S2", "file_claim", order_id="P1"),
    step("S3", "reserve_stock", order_id="P1"),
    step("S4", "reissue_shipment", ["S3"], order_id="P1"),
]


def create(conn, incident: str, steps: list[dict]):
    result = handler().handle(
        conn,
        CreatePlan(command_id=uuid.uuid4(), expected_version=version_of(conn, incident), producer="planner",
                   incident_id=incident, steps=steps),
    )
    conn.commit()
    return result


def revise(conn, incident: str, steps: list[dict], reason: str = "out_of_stock"):
    result = handler().handle(
        conn,
        RevisePlan(command_id=uuid.uuid4(), expected_version=version_of(conn, incident), producer="replanner",
                   incident_id=incident, steps=steps, reason=reason),
    )
    conn.commit()
    return result


def plan_steps(conn, incident: str) -> dict[str, dict]:
    rows = conn.execute(
        "SELECT step_id, action, status, origin_plan_version, depends_on FROM plan_steps WHERE incident_id = %s",
        (incident,),
    ).fetchall()
    conn.commit()
    return {r["step_id"]: r for r in rows}


def types(conn, incident: str) -> list[str]:
    return [e["event_type"] for e in events_of(conn, incident)]


def test_valid_plan_is_created(conn):
    """Un plan válido produce PlanCreated con plan_version 1 y sus pasos pendientes"""
    incident, _ = open_incident(conn)
    v = version_of(conn, incident)
    result = create(conn, incident, LOSS_PLAN)
    assert result.accepted and result.version == v + 1
    [ev] = result.events
    assert (ev.event_type, ev.payload["plan_version"]) == ("PlanCreated", 1)
    assert [s["action"] for s in ev.payload["steps"]] == ["notify_customer", "file_claim", "reserve_stock", "reissue_shipment"]
    steps = plan_steps(conn, incident)
    assert {s["status"] for s in steps.values()} == {"pending"}
    assert {s["origin_plan_version"] for s in steps.values()} == {1}
    assert steps["S4"]["depends_on"] == ["S3"]


@pytest.mark.parametrize(
    ("steps", "reason"),
    [
        ([step("S1", "delete_order", order_id="P1")], "action_not_allowed"),
        ([step("S1", "notify_customer"), step("S1-comp", "notify_customer")], "compensation_not_allowed_in_plan"),
        ([step("S1", "notify_customer"), step("S2", "release_stock", reservation_id="R1")], "compensation_not_allowed_in_plan"),
        ([{"step_id": "S1", "action": "notify_customer", "params": {}}], "invalid_plan"),
        ([step("S1", "notify_customer"), step("S1", "file_claim")], "invalid_plan"),
        ([step("S1", "notify_customer", ["S9"])], "invalid_plan"),
        ([step("S1", "notify_customer", ["S2"]), step("S2", "file_claim", ["S1"])], "invalid_plan"),
        ([step("S1", "issue_refund", order_id="P1", amount="100.01")], "refund_limit"),
        ([step("S1", "issue_refund", order_id="P1")], "invalid_plan"),
        ([step("S1", "issue_refund", order_id="P1", amount="NaN")], "invalid_plan"),
        ([], "invalid_plan"),
    ],
    ids=[
        "Plan con una acción no permitida",
        "Propuesta del LLM con una compensación incluida (-comp)",
        "Propuesta del LLM con una compensación incluida",
        "Plan sin campo obligatorio",
        "step_id repetido",
        "dependencia inexistente",
        "dependencia circular",
        "Reembolso por encima del máximo",
        "reembolso sin importe",
        "reembolso con importe NaN",
        "plan vacío",
    ],
)
def test_invalid_plan_is_rejected_without_events(conn, steps, reason):
    """Plan con una acción no permitida / Propuesta del LLM con una compensación incluida /
    Plan sin campo obligatorio / Reembolso por encima del máximo"""
    incident, _ = open_incident(conn)
    v = version_of(conn, incident)
    before = types(conn, incident)
    result = create(conn, incident, steps)
    assert (result.accepted, result.reason) == (False, reason)
    assert types(conn, incident) == before  # ni PlanCreated ni StepRequested
    assert plan_steps(conn, incident) == {}
    assert version_of(conn, incident) == v


def test_refund_at_the_limit_is_allowed(conn):
    """Un reembolso igual al máximo configurado se acepta"""
    incident, _ = open_incident(conn)
    assert create(conn, incident, [step("S1", "issue_refund", order_id="P1", amount="100.00")]).accepted


def test_second_plan_is_rejected(conn):
    """Una incidencia solo tiene un PlanCreated; los cambios van por RevisePlan"""
    incident, _ = open_incident(conn)
    assert create(conn, incident, LOSS_PLAN).accepted
    result = create(conn, incident, LOSS_PLAN)
    assert (result.accepted, result.reason) == (False, "plan_exists")


def test_step_kept_between_versions(conn):
    """Paso mantenido entre versiones"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    with conn.transaction():
        conn.execute(
            "UPDATE plan_steps SET status = 'completed' WHERE incident_id = %s AND step_id = 'S1'", (incident,)
        )
    result = revise(conn, incident, [step("S2", "file_claim", order_id="P1"), step("S5", "issue_refund", ["S1"], order_id="P1", amount="20")])
    assert result.accepted
    [ev] = result.events
    assert (ev.event_type, ev.payload["plan_version"], ev.payload["reason"]) == ("PlanRevised", 2, "out_of_stock")
    steps = plan_steps(conn, incident)
    assert steps["S2"]["origin_plan_version"] == 1  # conserva su step_id y su versión de origen
    assert steps["S5"]["origin_plan_version"] == 2
    row = conn.execute("SELECT plan_version, revisions FROM incidents WHERE incident_id = %s", (incident,)).fetchone()
    conn.commit()
    assert (row["plan_version"], row["revisions"]) == (2, 1)


def test_revised_plan_does_not_include_completed_steps(conn):
    """Sin stock en el almacén: la revisión no vuelve a incluir notify_customer, que ya estaba completado"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    with conn.transaction():
        conn.execute("UPDATE plan_steps SET status = 'completed' WHERE incident_id = %s AND step_id = 'S1'", (incident,))
    result = revise(conn, incident, [step("S1", "notify_customer", order_id="P1"), step("S5", "issue_refund", order_id="P1", amount="20")])
    assert (result.accepted, result.reason) == (False, "completed_step_in_plan")
    assert "PlanRevised" not in types(conn, incident)


def test_revised_plan_cannot_include_compensation(conn):
    """Propuesta del LLM con una compensación incluida (en una revisión)"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    result = revise(conn, incident, [step("S3-comp", "release_stock", reservation_id="R1")])
    assert (result.accepted, result.reason) == (False, "compensation_not_allowed_in_plan")


def test_kept_step_cannot_change_action(conn):
    """Un step_id mantenido no cambia de acción"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    result = revise(conn, incident, [step("S2", "issue_refund", order_id="P1", amount="20")])
    assert (result.accepted, result.reason) == (False, "invalid_plan")


def test_revise_without_plan(conn):
    """RevisePlan sin plan previo se rechaza con no_plan"""
    incident, _ = open_incident(conn)
    result = revise(conn, incident, LOSS_PLAN)
    assert (result.accepted, result.reason) == (False, "no_plan")


def test_refund_and_reissue_of_same_order(conn):
    """Reembolso y reenvío del mismo pedido"""
    incident, _ = open_incident(conn)
    steps = LOSS_PLAN + [step("S5", "issue_refund", order_id="P1", amount="50.00")]
    before = types(conn, incident)
    result = create(conn, incident, steps)
    assert (result.accepted, result.reason) == (False, "refund_and_reissue_conflict")
    assert types(conn, incident) == before  # no se publica PlanCreated


def test_refund_and_reissue_of_different_orders_is_allowed(conn):
    """Reembolsar un pedido y reenviar otro distinto en el mismo plan se admite"""
    incident, _ = open_incident(conn)
    steps = [step("S1", "reserve_stock", order_id="P1"), step("S2", "reissue_shipment", ["S1"], order_id="P1"),
             step("S3", "issue_refund", order_id="P2", amount="20.00")]
    assert create(conn, incident, steps).accepted


def test_refund_after_completed_reissue(conn):
    """Reembolso tras un reenvío completado"""
    from .test_steps import record, request

    incident, _ = open_incident(conn)
    assert create(conn, incident, LOSS_PLAN).accepted
    for sid, result, effect in [
        ("S3", {"reservation_id": "R1"}, {"order_id": "P1", "warehouse": "W1", "reservation_id": "R1"}),
        ("S4", {"shipment_id": "X1"}, {"order_id": "P1", "new_shipment_id": "X1"}),
        ("S1", {"message_id": "M1"}, {"order_id": "P1", "message_id": "M1"}),
    ]:
        assert request(conn, incident, sid).accepted
        assert record(conn, incident, sid, result=result, effect=effect).accepted
    result = revise(conn, incident, [step("S2", "file_claim", order_id="P1"),
                                     step("S5", "issue_refund", order_id="P1", amount="50.00")])
    assert (result.accepted, result.reason) == (False, "refund_and_reissue_conflict")
