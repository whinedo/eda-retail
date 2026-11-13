"""incident-planning (plan según la gravedad, acción no permitida) y llm-boundaries en el planificador."""

from __future__ import annotations

import uuid
from dataclasses import replace

from eda_retail.agents.planner import Planner
from eda_retail.agents.runtime import AgentRunner
from eda_retail.command.handler import CommandHandler
from eda_retail.command.models import CreatePlan, RecordLlmCall
from eda_retail.llm.fake import FakeLlmClient

from .command_helpers import RULES, events_of, handler, open_incident, version_of
from .test_plans import plan_steps


def _step(step_id: str, action: str, order: str, depends_on: list[str] | None = None, **extra) -> dict:
    step = {"step_id": step_id, "action": action, "params": {"order_id": order, "amount": None},
            "depends_on": depends_on or []}
    return step | extra


def _loss_plan(order: str) -> dict:
    return {"steps": [
        _step("S1", "notify_customer", order), _step("S2", "file_claim", order),
        _step("S3", "reserve_stock", order), _step("S4", "reissue_shipment", order, ["S3"]),
    ]}


def _run(conn, fake: FakeLlmClient, severity: str = "high", cause: str = "loss", rules=RULES):
    incident, opened = open_incident(conn, severity=severity, cause=cause)
    delayed = opened.events[0]
    h = CommandHandler(rules)
    AgentRunner(Planner(fake, h), h).process(conn, delayed)
    evs = events_of(conn, incident)
    return incident, delayed, evs


def _of_type(evs, event_type: str) -> list[dict]:
    return [e for e in evs if e["event_type"] == event_type]


def test_mild_delay(conn):
    """Retraso leve"""
    fake = FakeLlmClient()
    incident, delayed, evs = _run(conn, fake, severity="low", cause="delay")
    [plan] = _of_type(evs, "PlanCreated")
    assert plan["payload"]["plan_version"] == 1
    assert [s["action"] for s in plan["payload"]["steps"]] == ["notify_customer"]
    assert plan["causation_id"] == delayed.event_id
    assert fake.requests == []  # plan fijo en código, sin LLM (supuesto A3)


def test_lost_package(conn):
    """Pérdida del paquete"""
    incident, opened = open_incident(conn, severity="high", cause="loss")
    order = opened.events[0].payload["order_ids"][0]
    fake = FakeLlmClient([_loss_plan(order)])
    AgentRunner(Planner(fake, handler()), handler()).process(conn, opened.events[0])
    [plan] = _of_type(events_of(conn, incident), "PlanCreated")
    assert [s["action"] for s in plan["payload"]["steps"]] == [
        "notify_customer", "file_claim", "reserve_stock", "reissue_shipment"]
    assert "loss" in fake.requests[0].prompt and order in fake.requests[0].prompt


def test_plan_with_disallowed_action(conn):
    """Plan con una acción no permitida"""
    incident, opened = open_incident(conn)
    order = opened.events[0].payload["order_ids"][0]
    bad = {"steps": [_step("S1", "delete_order", order)]}
    fake = FakeLlmClient([bad, _loss_plan(order)])
    AgentRunner(Planner(fake, handler()), handler()).process(conn, opened.events[0])
    evs = events_of(conn, incident)
    assert not _of_type(evs, "StepRequested")
    assert [e["payload"]["validation_passed"] for e in _of_type(evs, "LlmCallRecorded")] == [False, True]
    assert "action_not_allowed" in fake.requests[1].prompt  # se pide un plan nuevo explicando el rechazo
    [plan] = _of_type(evs, "PlanCreated")
    assert all(s["action"] != "delete_order" for s in plan["payload"]["steps"])


def test_plan_missing_required_field_is_not_published(conn):
    """Plan sin campo obligatorio"""
    incident, opened = open_incident(conn)
    order = opened.events[0].payload["order_ids"][0]
    no_depends = {"steps": [{"step_id": "S1", "action": "notify_customer", "params": {"order_id": order}}]}
    rules = replace(RULES, max_generations=1)
    h = CommandHandler(rules)
    AgentRunner(Planner(FakeLlmClient([no_depends]), h), h).process(conn, opened.events[0])
    evs = events_of(conn, incident)
    assert not _of_type(evs, "PlanCreated")
    assert [e["payload"]["reason"] for e in _of_type(evs, "IncidentEscalated")] == ["llm_output_invalid"]


def test_three_invalid_plans_escalate(conn):
    """Tres planes inválidos seguidos"""
    fake = FakeLlmClient(["no es JSON", {"steps": []}, {"steps": [_step("S1", "release_stock", "P1")]}])
    _, _, evs = _run(conn, fake)
    assert not _of_type(evs, "PlanCreated")
    assert [e["payload"]["reason"] for e in _of_type(evs, "IncidentEscalated")] == ["llm_output_invalid"]
    assert len(_of_type(evs, "LlmCallRecorded")) == 3


def test_token_budget_exhausted_escalates(conn):
    """Presupuesto de tokens agotado"""
    incident, opened = open_incident(conn)
    tight = CommandHandler(replace(RULES, max_tokens_per_incident=100))
    tight.handle(conn, RecordLlmCall(
        command_id=uuid.uuid4(), expected_version=0, producer="interpreter", incident_id=incident,
        agent="interpreter", model="fake-model", tokens_in=90, tokens_out=10, latency_ms=1,
        validation_passed=True, attempt=1,
    ))
    conn.commit()
    fake = FakeLlmClient()
    AgentRunner(Planner(fake, tight), tight).process(conn, opened.events[0])
    assert fake.requests == []  # la llamada no se realiza
    evs = events_of(conn, incident)
    assert [e["payload"]["reason"] for e in _of_type(evs, "IncidentEscalated")] == ["budget_exhausted"]


def test_planner_has_no_tools():
    """Agente que intenta actuar"""
    planner = Planner(FakeLlmClient(), handler())
    assert planner.TOOLS == ()
    assert not any(hasattr(planner, n) for n in ("reserve_stock", "notify_customer", "issue_refund", "execute"))


def test_handler_validate_writes_nothing(conn):
    """El ensayo del command handler no escribe estado ni eventos ni gasta seq"""
    incident, opened = open_incident(conn)
    order = opened.events[0].payload["order_ids"][0]
    command = CreatePlan(command_id=uuid.uuid4(), expected_version=version_of(conn, incident), producer="planner",
                         incident_id=incident, steps=_loss_plan(order)["steps"])
    before = len(events_of(conn, incident))
    seq_before = conn.execute("SELECT last_value FROM events_seq_seq").fetchone()["last_value"]
    conn.commit()
    assert handler().validate(conn, command).accepted
    conn.commit()
    assert len(events_of(conn, incident)) == before
    assert plan_steps(conn, incident) == {}
    assert conn.execute("SELECT last_value FROM events_seq_seq").fetchone()["last_value"] == seq_before
    conn.commit()
