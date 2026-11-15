"""Decisiones del command handler del intérprete (T5): todo lo que no necesita la base de datos."""

from __future__ import annotations

import uuid
from decimal import Decimal

from eda_retail.config import Settings
from eda_retail.events.catalog import Cause, Severity
from eda_retail.infra.commands import CommandEnvelope
from eda_retail.interpreter import handler
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import BUDGET_EXHAUSTED, Budget
from eda_retail.llm.prompts.interpreter import NoticeInterpretation
from eda_retail.queries.cost_per_incident import IncidentCost, budget_from_cost
from eda_retail.rules import BusinessRules

DELAY = {"order_ids": ["P-1", "P-9"], "cause": "delay", "estimated_delay_hours": 24, "severity": "low"}


def _value(order_ids=("P-1",)) -> NoticeInterpretation:
    return NoticeInterpretation(order_ids=list(order_ids), cause=Cause.DELAY, estimated_delay_hours=24,
                                severity=Severity.LOW)


def _command() -> tuple[CommandEnvelope, handler.OpenIncident]:
    payload = {"carrier_notice_id": "N-1", "carrier": "demo-express", "raw_text": "Retraso en P-1 y P-9"}
    event_id = uuid.uuid4()
    command = CommandEnvelope(command_id=event_id, command_type="OpenIncident", correlation_id="I-1",
                              causation_id=event_id, payload=payload)
    return command, handler.OpenIncident.model_validate(payload)


def _preparer(llm, *, interpreted=False, known=("P-1",), budget=None):
    rules = BusinessRules()
    return handler.NoticePreparer(
        llm, rules,
        interpreted=lambda notice: interpreted,
        budget=lambda incident: budget or budget_from_cost(None, rules),
        known_orders=lambda order_ids: [o for o in order_ids if o in known],
    )


def test_pending_notice_timeout_has_a_default_and_reads_the_environment(monkeypatch):
    monkeypatch.delenv("PENDING_NOTICE_TIMEOUT_S", raising=False)
    assert Settings().pending_notice_timeout_s == 300.0
    monkeypatch.setenv("PENDING_NOTICE_TIMEOUT_S", "2")
    assert Settings().pending_notice_timeout_s == 2.0


def test_budget_comes_from_the_cost_projection_and_no_row_means_nothing_spent():
    """Presupuesto por incidencia: se consulta cost_per_incident; sin fila no se ha gastado nada"""
    rules = BusinessRules()
    assert budget_from_cost(None, rules) == Budget(0, 0, rules.max_tokens_per_incident,
                                                           rules.max_llm_calls_per_incident)
    cost = IncidentCost(incident_id="I-1", llm_calls=2, tokens_in=100, tokens_out=50, claims=0, reissues=0,
                        refunds_amount=Decimal(0), llm_cost=Decimal(0), effects_cost=Decimal(0), applied={})
    assert budget_from_cost(cost, rules) == Budget(150, 2, rules.max_tokens_per_incident,
                                                           rules.max_llm_calls_per_incident)


def test_prepare_interprets_the_notice_and_looks_up_its_orders():
    llm = FakeLlmClient([DELAY])
    command, payload = _command()

    prepared = _preparer(llm)(command, payload)

    assert prepared.duplicate is False
    assert prepared.interpretation.order_ids == ["P-1", "P-9"]
    assert prepared.known_orders == ["P-1"]
    assert len(prepared.calls) == 1
    [request] = llm.requests
    assert "<aviso>\nRetraso en P-1 y P-9\n</aviso>" in request.prompt


def test_prepare_does_not_pay_the_llm_for_a_notice_already_interpreted():
    """Aviso repetido: se detecta antes de llamar al LLM"""
    llm = FakeLlmClient([])
    command, payload = _command()
    prepared = _preparer(llm, interpreted=True)(command, payload)
    assert prepared.duplicate is True
    assert llm.requests == []


def test_prepare_without_budget_does_not_call_the_llm():
    """Presupuesto de tokens agotado: no se llama al LLM"""
    rules = BusinessRules()
    spent = Budget(rules.max_tokens_per_incident, 0, rules.max_tokens_per_incident, rules.max_llm_calls_per_incident)
    llm = FakeLlmClient([])
    command, payload = _command()
    prepared = _preparer(llm, budget=spent)(command, payload)
    assert (prepared.interpretation, prepared.reason, llm.requests) == (None, BUDGET_EXHAUSTED, [])


def test_known_orders_open_the_incident_with_only_those_orders():
    """Pedidos conocidos: los pedidos que no están en view_orders se excluyen"""
    decision = handler.decide(_value(["P-1", "P-9"]), None, known_orders=["P-1"])
    assert decision == handler.Decision("delayed", ["P-1"], None)


def test_no_known_order_parks_the_notice():
    """Proyección atrasada: sin ningún pedido en view_orders, el aviso se aparca"""
    assert handler.decide(_value(["P-7"]), None, known_orders=[]) == handler.Decision("parked", ["P-7"], None)


def test_no_interpretation_escalates_with_the_generation_reason():
    assert handler.decide(None, "llm_output_invalid", known_orders=[]) == handler.Decision(
        "escalated", [], "llm_output_invalid")


def test_original_notice_text_is_the_payload_of_the_command():
    assert set(handler.OpenIncident.model_fields) == {"carrier_notice_id", "carrier", "raw_text"}
