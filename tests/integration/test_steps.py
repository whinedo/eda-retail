"""RequestStep y RecordStepResult: dependencias, paso pivote, idempotency_key y resultados."""

from __future__ import annotations

import uuid

from psycopg.types.json import Jsonb

from eda_retail.command.models import EscalateIncident, RecordStepResult, RequestStep
from eda_retail.infra.effect_keys import compute_idempotency_key

from .command_helpers import events_of, handler, open_incident, version_of
from .test_plans import LOSS_PLAN, create, plan_steps, revise, step


def request(conn, incident: str, step_id: str):
    result = handler().handle(
        conn,
        RequestStep(command_id=uuid.uuid4(), expected_version=version_of(conn, incident), producer="orchestrator",
                    incident_id=incident, step_id=step_id),
    )
    conn.commit()
    return result


def record(conn, incident: str, step_id: str, outcome: str = "completed", **kw):
    if "plan_version" not in kw:
        kw["plan_version"] = conn.execute(
            "SELECT plan_version FROM incidents WHERE incident_id = %s", (incident,)
        ).fetchone()["plan_version"]
        conn.commit()
    result = handler().handle(
        conn,
        RecordStepResult(command_id=uuid.uuid4(), expected_version=version_of(conn, incident), producer="executor",
                         incident_id=incident, step_id=step_id, outcome=outcome, **kw),
    )
    conn.commit()
    return result


def complete_reserve(conn, incident: str, step_id: str = "S3"):
    assert request(conn, incident, step_id).accepted
    return record(
        conn, incident, step_id, result={"reservation_id": "R1"},
        effect={"order_id": "P1", "warehouse": "W1", "reservation_id": "R1"},
    )


def test_valid_command(conn):
    """Comando válido"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    assert request(conn, incident, "S2").accepted
    before = len(events_of(conn, incident))
    result = record(conn, incident, "S2", result={"claim_id": "C1"}, effect={"order_id": "P1", "claim_id": "C1"})
    assert result.accepted
    assert [e.event_type for e in result.events] == ["StepCompleted", "ClaimFiled"]
    assert len(events_of(conn, incident)) == before + 2
    assert plan_steps(conn, incident)["S2"]["status"] == "completed"


def test_independent_steps(conn):
    """Pasos sin dependencias"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    results = [request(conn, incident, s) for s in ("S1", "S2", "S3")]
    assert all(r.accepted for r in results)
    assert [r.events[0].payload["step_id"] for r in results] == ["S1", "S2", "S3"]


def test_step_with_dependency(conn):
    """Paso con dependencia"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    blocked = request(conn, incident, "S4")
    assert (blocked.accepted, blocked.reason) == (False, "dependencies_pending")
    assert "StepRequested" not in [e["event_type"] for e in events_of(conn, incident)]
    assert complete_reserve(conn, incident).accepted
    assert request(conn, incident, "S4").accepted


def test_refund_with_pending_compensation(conn):
    """Reembolso con una compensación pendiente"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    complete_reserve(conn, incident)
    revise(conn, incident, [step("S5", "issue_refund", order_id="P1", amount="30")], reason="carrier_rejected")
    # La orquestación (fase 4) deriva la compensación: aquí se simula su estado.
    with conn.transaction():
        conn.execute(
            "UPDATE plan_steps SET status = 'compensating' WHERE incident_id = %s AND step_id = 'S3'", (incident,)
        )
        conn.execute(
            "INSERT INTO plan_steps (incident_id, step_id, origin_plan_version, action, params, compensates) "
            "VALUES (%s, 'S3-comp', 2, 'release_stock', %s, 'S3')",
            (incident, Jsonb({"reservation_id": "R1"})),
        )
    held = request(conn, incident, "S5")
    assert (held.accepted, held.reason) == (False, "compensation_pending")

    assert request(conn, incident, "S3-comp").accepted
    assert record(conn, incident, "S3-comp", result={"released": True}, effect={"reservation_id": "R1"}).accepted
    assert plan_steps(conn, incident)["S3"]["status"] == "compensated"
    assert request(conn, incident, "S5").accepted


def test_escalated_incident_emits_no_more_steps(conn):
    """Incidencia escalada"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    handler().handle(
        conn,
        EscalateIncident(command_id=uuid.uuid4(), expected_version=version_of(conn, incident), producer="ops",
                         incident_id=incident, reason="budget_exhausted"),
    )
    conn.commit()
    result = request(conn, incident, "S1")
    assert (result.accepted, result.reason) == (False, "incident_closed")
    assert "StepRequested" not in [e["event_type"] for e in events_of(conn, incident)]


def test_idempotency_key_uses_origin_plan_version(conn):
    """RequestStep calcula la idempotency_key con la versión de origen del paso"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    revise(conn, incident, [step("S2", "file_claim", order_id="P1")])
    result = request(conn, incident, "S2")
    ev = result.events[0]
    assert ev.payload["plan_version"] == 2
    assert ev.payload["idempotency_key"] == compute_idempotency_key(incident, "S2", origin_plan_version=1)
    assert ev.payload["params"] == {"order_id": "P1"}


def test_failed_step_result(conn):
    """RecordStepResult de un fallo publica StepFailed con error y retryable"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    request(conn, incident, "S3")
    result = record(conn, incident, "S3", outcome="failed", error="out_of_stock", retryable=False)
    assert result.accepted
    [ev] = result.events
    assert ev.event_version == 2
    assert ev.payload == {"step_id": "S3", "plan_version": 1, "error": "out_of_stock", "retryable": False, "attempts": 1}
    assert plan_steps(conn, incident)["S3"]["status"] == "failed"


def test_step_command_rejections(conn):
    """Pasos inexistentes, pedidos dos veces, resultados sin petición o incompletos"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    assert request(conn, incident, "S9").reason == "step_not_found"
    assert record(conn, incident, "S1", result={}, effect={"order_id": "P1", "message_id": "M1"}).reason == "step_not_requested"
    request(conn, incident, "S1")
    assert request(conn, incident, "S1").reason == "step_not_requestable"
    assert record(conn, incident, "S1").reason == "invalid_command"  # completed sin result ni efecto
    assert record(conn, incident, "S1", outcome="failed").reason == "invalid_command"  # sin error ni retryable
    assert record(conn, incident, "S1", result={}, effect={"order_id": "P1"}).reason == "invalid_command"
