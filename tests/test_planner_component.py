"""Command handler del planificador (T6): todo lo que no necesita la base de datos."""

from __future__ import annotations

import pathlib
import uuid
from decimal import Decimal
from types import SimpleNamespace

import pytest

from eda_retail.command.handler import Rejected as PlanRejected
from eda_retail.command.plans import validate_plan
from eda_retail.command.rules import BusinessRules
from eda_retail.config import Settings
from eda_retail.infra import ownership
from eda_retail.infra.commands import CommandEnvelope
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import BUDGET_EXHAUSTED, LLM_OUTPUT_INVALID, Budget
from eda_retail.planner import handler
from eda_retail.queries.cost_per_incident import budget_from_cost

RULES = BusinessRules()
LOSS = {"order_ids": ["P-1"], "cause": "loss", "estimated_delay_hours": 0, "severity": "high"}


def _step(step_id: str, action: str, order: str = "P-1", depends_on=(), amount=None) -> dict:
    return {"step_id": step_id, "action": action, "params": {"order_id": order, "amount": amount},
            "depends_on": list(depends_on)}


LOSS_PLAN = {"steps": [_step("S1", "notify_customer"), _step("S2", "file_claim"), _step("S3", "reserve_stock"),
                       _step("S4", "reissue_shipment", depends_on=["S3"])]}


def _command(payload: dict) -> tuple[CommandEnvelope, handler.CreatePlan]:
    command = CommandEnvelope(command_id=uuid.uuid4(), command_type="CreatePlan", correlation_id="I-1",
                              causation_id=uuid.uuid4(), payload=payload)
    return command, handler.CreatePlan.model_validate(payload)


def _preparer(llm, *, planned=False, amounts=None, budget=None):
    context = handler.PlanContext(planned=planned, budget=budget or budget_from_cost(None, RULES),
                                  amounts=amounts if amounts is not None else {"P-1": Decimal("40.00")})
    return handler.PlanPreparer(llm, RULES, context=lambda incident_id, order_ids: context)


def test_planner_url_has_a_default_and_reads_the_environment(monkeypatch):
    monkeypatch.delenv("PLANNER_URL", raising=False)
    assert Settings().planner_url == "http://127.0.0.1:8103"
    monkeypatch.setenv("PLANNER_URL", "http://127.0.0.1:9103")
    assert Settings().planner_url == "http://127.0.0.1:9103"


def test_the_command_carries_the_incident():
    assert set(handler.CreatePlan.model_fields) == {"order_ids", "cause", "estimated_delay_hours", "severity"}


def test_low_severity_gets_the_fixed_plan_without_llm():
    """Retraso leve: un único paso notify_customer, sin LLM"""
    llm = FakeLlmClient([])
    command, payload = _command({"order_ids": ["P-1"], "cause": "delay", "estimated_delay_hours": 24,
                                 "severity": "low"})

    prepared = _preparer(llm)(command, payload)

    assert [s["action"] for s in prepared.steps] == ["notify_customer"]
    assert (prepared.reason, prepared.calls, llm.requests) == (None, [], [])


def test_prepare_asks_the_llm_with_the_order_amounts_from_view_orders():
    """Pérdida del paquete: el LLM propone el plan con los importes de view_orders"""
    llm = FakeLlmClient([LOSS_PLAN])
    command, payload = _command(LOSS)

    prepared = _preparer(llm)(command, payload)

    assert [s["action"] for s in prepared.steps] == ["notify_customer", "file_claim", "reserve_stock",
                                                     "reissue_shipment"]
    assert len(prepared.calls) == 1
    [request] = llm.requests
    assert "causa loss, gravedad high" in request.prompt
    assert "- P-1: importe 40.00" in request.prompt


def test_a_plan_with_a_forbidden_action_is_asked_again():
    """Plan con una acción no permitida: se rechaza y se pide un plan nuevo"""
    llm = FakeLlmClient([{"steps": [_step("S1", "delete_order")]}, LOSS_PLAN])
    command, payload = _command(LOSS)

    prepared = _preparer(llm)(command, payload)

    assert "delete_order" not in [s["action"] for s in prepared.steps]
    assert len(llm.requests) == 2


def test_three_invalid_plans_end_without_a_plan():
    """Tres planes inválidos seguidos: llm_output_invalid"""
    llm = FakeLlmClient([{"steps": [_step("S1", "delete_order")]}] * RULES.max_generations)
    command, payload = _command(LOSS)

    prepared = _preparer(llm)(command, payload)

    assert (prepared.steps, prepared.reason) == (None, LLM_OUTPUT_INVALID)
    assert len(prepared.calls) == RULES.max_generations


def test_prepare_without_budget_does_not_call_the_llm():
    """Presupuesto de tokens agotado: no se llama al LLM"""
    spent = Budget(RULES.max_tokens_per_incident, 0, RULES.max_tokens_per_incident, RULES.max_llm_calls_per_incident)
    llm = FakeLlmClient([])
    command, payload = _command(LOSS)
    prepared = _preparer(llm, budget=spent)(command, payload)
    assert (prepared.steps, prepared.reason, llm.requests) == (None, BUDGET_EXHAUSTED, [])


def test_an_incident_already_planned_does_not_pay_the_llm():
    """Plan ya creado: se detecta antes de llamar al LLM"""
    llm = FakeLlmClient([])
    command, payload = _command(LOSS)
    prepared = _preparer(llm, planned=True)(command, payload)
    assert prepared.planned is True
    assert llm.requests == []


def _validate(steps, amounts=None):
    return validate_plan(SimpleNamespace(rules=RULES), steps, external_ids=set(), order_amounts=amounts)


def test_refund_above_the_order_amount_is_rejected():
    """Reembolso por encima del importe del pedido: refund_limit aunque no supere el máximo"""
    with pytest.raises(PlanRejected) as exc:
        _validate([_step("S1", "issue_refund", amount="60.00")], {"P-1": Decimal("40.00")})
    assert exc.value.reason == "refund_limit"


def test_refund_of_an_order_missing_in_view_orders_is_rejected():
    with pytest.raises(PlanRejected) as exc:
        _validate([_step("S1", "issue_refund", order="P-9", amount="10.00")], {"P-1": Decimal("40.00")})
    assert exc.value.reason == "refund_limit"


def test_refund_within_the_order_amount_is_accepted():
    [step] = _validate([_step("S1", "issue_refund", amount="40.00")], {"P-1": Decimal("40.00")})
    assert step.action == "issue_refund"


def test_without_order_amounts_only_the_configured_maximum_applies():
    """E5 y la base síncrona no consultan view_orders: su validación no cambia"""
    [step] = _validate([_step("S1", "issue_refund", amount="60.00")])
    assert step.params["amount"] == "60.00"


def test_planner_is_a_checked_component():
    assert ownership.COMPONENT_PACKAGES["eda_retail/planner"] == "planner"


def test_planner_does_not_read_inventory_tables():
    """Dato de otro dominio: el importe se lee en view_orders, no en las tablas de inventory"""
    package = pathlib.Path(handler.__file__).parent
    sources = "\n".join(p.read_text() for p in package.glob("*.py"))
    assert "inventory." not in sources
    assert "view_orders" in sources
