"""incident-planning: revisión del plan ante un fallo, estabilidad de step_id y revisiones agotadas."""

from __future__ import annotations

from dataclasses import replace

import pytest

from eda_retail.agents.replanner import Replanner, ViewNotCaughtUp
from eda_retail.agents.runtime import AgentRunner
from eda_retail.command.handler import CommandHandler
from eda_retail.infra import inbox
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.projectors.base import load_event

from .command_helpers import RULES, events_of, handler, open_incident
from .projection_helpers import project
from .test_plans import LOSS_PLAN, create, revise
from .test_steps import record, request


def _step(step_id: str, action: str, depends_on: list[str] | None = None, amount: float | None = None) -> dict:
    return {"step_id": step_id, "action": action, "params": {"order_id": "P1", "amount": amount},
            "depends_on": depends_on or []}


REFUND_PLAN = {"steps": [_step("S2", "file_claim"), _step("S5", "issue_refund", amount=50.0)]}


def _out_of_stock(conn, rules=RULES):
    """LOSS_PLAN con el aviso completado y reserve_stock fallido sin reintento; devuelve el StepFailed."""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    assert request(conn, incident, "S1").accepted
    assert record(conn, incident, "S1", result={"message_id": "M1"},
                  effect={"order_id": "P1", "message_id": "M1"}).accepted
    assert request(conn, incident, "S3").accepted
    assert record(conn, incident, "S3", outcome="failed", error="out_of_stock", retryable=False).accepted
    return incident, _last_failed(conn, incident)


def _last_failed(conn, incident: str):
    row = conn.execute(
        "SELECT * FROM events WHERE correlation_id = %s AND event_type = 'StepFailed' ORDER BY seq DESC LIMIT 1",
        (incident,),
    ).fetchone()
    conn.commit()
    return load_event(row)


def _replan(settings, conn, failed, outputs, rules=RULES, **kw):
    project(settings, conn, "incident_status", failed.correlation_id)
    h = CommandHandler(rules)
    fake = FakeLlmClient(outputs)
    AgentRunner(Replanner(fake, h, **kw), h).process(conn, failed)
    return fake


def _of_type(conn, incident, event_type):
    return [e for e in events_of(conn, incident) if e["event_type"] == event_type]


def test_out_of_stock(conn, settings):
    """Sin stock en el almacén"""
    incident, failed = _out_of_stock(conn)
    fake = _replan(settings, conn, failed, [REFUND_PLAN])
    [revised] = _of_type(conn, incident, "PlanRevised")
    actions = [s["action"] for s in revised["payload"]["steps"]]
    assert revised["payload"]["plan_version"] == 2
    assert "issue_refund" in actions and "reissue_shipment" not in actions
    assert "notify_customer" not in actions  # S1 ya estaba completado
    assert revised["causation_id"] == failed.event_id
    prompt = fake.requests[0].prompt
    assert "S1" in prompt and "completed" in prompt and "out_of_stock" in prompt  # el plan sale de la vista


def test_step_kept_between_versions(conn, settings):
    """Paso mantenido entre versiones"""
    incident, failed = _out_of_stock(conn)
    renamed = {"steps": [_step("S9", "file_claim"), _step("S5", "issue_refund", amount=50.0)]}
    fake = _replan(settings, conn, failed, [renamed, REFUND_PLAN])
    [revised] = _of_type(conn, incident, "PlanRevised")
    assert "S2" in [s["step_id"] for s in revised["payload"]["steps"]]
    assert "step_id_changed" in fake.requests[1].prompt  # el código exige reutilizar S2
    calls = [e["payload"]["validation_passed"] for e in _of_type(conn, incident, "LlmCallRecorded")]
    assert calls == [False, True]


def test_revisions_exhausted(conn, settings):
    """Revisiones agotadas"""
    rules = replace(RULES, max_plan_revisions=1)
    incident, _ = _out_of_stock(conn)
    assert revise(conn, incident, [_step("S2", "file_claim"), _step("S5", "issue_refund", amount=50.0)]).accepted
    assert request(conn, incident, "S2").accepted
    assert record(conn, incident, "S2", outcome="failed", error="carrier_rejected", retryable=False).accepted
    fake = _replan(settings, conn, _last_failed(conn, incident), [], rules=rules)
    assert fake.requests == []
    assert [e["payload"]["reason"] for e in _of_type(conn, incident, "IncidentEscalated")] == ["max_revisions"]


def test_retryable_failure_is_ignored(conn, settings):
    """Un StepFailed reintentable no se replanifica (lo reintenta la orquestación)"""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN)
    assert request(conn, incident, "S2").accepted
    assert record(conn, incident, "S2", outcome="failed", error="timeout", retryable=True).accepted
    fake = _replan(settings, conn, _last_failed(conn, incident), [])
    assert fake.requests == [] and not _of_type(conn, incident, "PlanRevised")


def test_replanner_waits_for_the_view(conn):
    """Si incident_status aún no refleja el fallo, no decide ni registra el evento en el inbox"""
    _, failed = _out_of_stock(conn)
    fake = FakeLlmClient([REFUND_PLAN])
    runner = AgentRunner(Replanner(fake, handler(), view_timeout=0.0), handler())
    # sin ejecutar el proyector: la vista no tiene la incidencia al día
    with pytest.raises(ViewNotCaughtUp):
        runner.process(conn, failed)
    assert fake.requests == []
    assert not inbox.seen(conn, "replanner", failed.event_id)
    conn.commit()
