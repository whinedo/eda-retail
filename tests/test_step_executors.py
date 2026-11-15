"""Ejecutores con command handler REST (T9): todo lo que no necesita la base de datos."""

from __future__ import annotations

import pathlib
import uuid

import pytest

from eda_retail.infra import ownership
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.commands import CommandEnvelope
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import BUDGET_EXHAUSTED, Budget
from eda_retail.llm.prompts.writer import DraftRequest, _prompt
from eda_retail.orchestrator import rules
from eda_retail.queries.cost_per_incident import budget_from_cost
from eda_retail.rules import BusinessRules
from eda_retail.step_executors import cli, runtime
from eda_retail.step_executors.actions import build_action
from eda_retail.step_executors.drafted import Drafted
from eda_retail.step_executors.errors import BusinessError, Transient
from eda_retail.step_executors.services import FakeService

RULES = BusinessRules()
ROOT = pathlib.Path(__file__).parents[1]
NOTICE = ("Hemos sentido mucho el retraso de tu pedido. El transportista nos ha avisado y "
          "te escribiremos en cuanto tengamos noticias de la entrega.")
EXECUTOR_ACTIONS = ("notify_customer", "file_claim", "reissue_shipment", "issue_refund")


def _step(action: str = "reissue_shipment", **extra) -> runtime.ExecuteStep:
    params = {"order_id": "P-1", "amount": extra.pop("amount", None)}
    return runtime.ExecuteStep(incident_id="I-1", plan_version=1, step_id="S1", action=action, params=params,
                               idempotency_key="K-1", **extra)


def _command(step: runtime.ExecuteStep) -> CommandEnvelope:
    return CommandEnvelope(command_id=uuid.uuid4(), command_type="ExecuteStep", correlation_id=step.incident_id,
                           causation_id=uuid.uuid4(), payload=step.model_dump())


class _Action:
    """Acción falsa: cuenta las preparaciones y llamadas al servicio."""

    action = "reissue_shipment"
    needs_context = False

    def __init__(self, service: FakeService, error=None) -> None:
        self.service = service
        self.error = error
        self.prepared = 0

    def prepare(self, step):
        self.prepared += 1
        return runtime.Preparation(error=self.error)

    def call(self, step, preparation):
        return self.service.call(step.idempotency_key)


def _preparer(action, *, stored=None, sleeps=None):
    return runtime.StepPreparer(action, completed=lambda key: stored, max_attempts=3,
                                backoff=Backoff(0.5, 10, seed=1), sleep=(sleeps if sleeps is not None else []).append)


# --- runtime --------------------------------------------------------------------------------


def test_a_completed_key_skips_the_preparation_and_the_service():
    """Paso ya completado: el ejecutor no repite el efecto y devuelve el resultado guardado"""
    service = FakeService("carrier", "SH")
    action = _Action(service)
    prepared = _preparer(action, stored={"new_shipment_id": "SH-1"})(_command(_step()), _step())
    assert prepared.stored == {"new_shipment_id": "SH-1"}
    assert (action.prepared, service.calls) == (0, 0)


def test_a_transient_error_is_retried_with_backoff():
    """Servicio caído: reintenta con espera exponencial y el efecto se ejecuta una sola vez"""
    service = FakeService("carrier", "SH", timeouts=2)
    sleeps: list[float] = []
    prepared = _preparer(_Action(service), sleeps=sleeps)(_command(_step()), _step())
    assert prepared.error is None and prepared.external_id.startswith("SH-")
    assert prepared.attempts == 3
    assert len(sleeps) == 2 and all(0 <= s <= 10 for s in sleeps)
    assert service.executed == 1


def test_transient_errors_exhausted_end_in_a_retryable_failure():
    """Reintentos agotados: StepFailed con retryable verdadero y el número de intentos"""
    service = FakeService("carrier", "SH", down=True)
    prepared = _preparer(_Action(service))(_command(_step()), _step())
    assert (prepared.error.error, prepared.error.retryable, prepared.attempts) == ("timeout", True, 3)


def test_a_business_error_is_not_retried():
    """Rechazo del transportista: StepFailed no reintentable al primer intento"""
    service = FakeService("carrier", "SH", reject="carrier_rejected")
    prepared = _preparer(_Action(service))(_command(_step()), _step())
    assert (prepared.error.error, prepared.error.retryable, prepared.attempts) == ("carrier_rejected", False, 1)
    assert service.calls == 1


def test_a_preparation_error_does_not_call_the_service():
    service = FakeService("carrier", "SH")
    prepared = _preparer(_Action(service, error=BusinessError("order_not_found")))(_command(_step()), _step())
    assert prepared.error.error == "order_not_found" and service.calls == 0


def test_another_action_is_not_prepared():
    service = FakeService("carrier", "SH")
    action = _Action(service)
    prepared = _preparer(action)(_command(_step("issue_refund")), _step("issue_refund"))
    assert prepared == runtime.Prepared()
    assert action.prepared == 0


# --- acciones -------------------------------------------------------------------------------


def _build(action: str, llm=None, *, known=True, budget=None, service=None):
    return build_action(action, llm=llm or FakeLlmClient([]), rules=RULES, service=service,
                        order_known=lambda order_id: known,
                        budget=lambda incident_id: budget or budget_from_cost(None, RULES))


def test_notify_customer_drafts_with_the_cause_carried_by_the_command():
    """Causa del aviso en el comando: el ejecutor no lee la incidencia ni inventory"""
    llm = FakeLlmClient([{"text": NOTICE}])
    step = _step("notify_customer", cause="delay", estimated_delay_hours=48)
    preparation = _build("notify_customer", llm).prepare(step)
    assert preparation.error is None and preparation.text == NOTICE
    [request] = llm.requests
    assert request.agent == "writer"
    assert "Causa: delay. Retraso estimado: 48 h." in request.prompt
    assert "Pedido: P-1." in request.prompt


def test_an_invalid_draft_is_a_retryable_failure():
    """Borrador con datos de otro pedido: no se envía y el paso falla como reintentable"""
    llm = FakeLlmClient([{"text": NOTICE + " También el pedido P-999."}] * RULES.max_generations)
    preparation = _build("notify_customer", llm).prepare(_step("notify_customer", cause="delay",
                                                                estimated_delay_hours=1))
    assert (preparation.error.error, preparation.error.retryable) == ("invalid_draft", True)
    assert len(preparation.calls) == RULES.max_generations


def test_without_budget_the_draft_fails_without_retry():
    spent = Budget(RULES.max_tokens_per_incident, 0, RULES.max_tokens_per_incident, RULES.max_llm_calls_per_incident)
    preparation = _build("file_claim", budget=spent).prepare(_step("file_claim", cause="loss",
                                                                   estimated_delay_hours=0))
    assert (preparation.error.error, preparation.error.retryable) == (BUDGET_EXHAUSTED, False)


def test_an_order_missing_in_view_orders_is_not_drafted():
    llm = FakeLlmClient([])
    preparation = _build("notify_customer", llm, known=False).prepare(
        _step("notify_customer", cause="delay", estimated_delay_hours=1))
    assert preparation.error.error == "order_not_found"
    assert llm.requests == []


def test_drafted_actions_need_the_incident_context():
    assert isinstance(_build("notify_customer"), Drafted) and _build("notify_customer").needs_context
    assert isinstance(_build("file_claim"), Drafted)
    assert not _build("reissue_shipment").needs_context and not _build("issue_refund").needs_context


def test_refund_above_the_maximum_escalates_without_paying():
    """Importe por encima del máximo: no se ejecuta, StepFailed y IncidentEscalated con refund_limit"""
    payments = FakeService("payments", "RF")
    action = _build("issue_refund", service=payments)
    step = _step("issue_refund", amount=str(RULES.max_refund_amount + 1))
    prepared = _preparer(action)(_command(step), step)
    assert (prepared.error.error, prepared.error.retryable, prepared.error.escalate) == (
        "refund_limit", False, "refund_limit")
    assert payments.calls == 0


def test_reissue_of_an_unknown_order_fails():
    preparation = _build("reissue_shipment", known=False).prepare(_step())
    assert (preparation.error.error, preparation.error.retryable) == ("order_not_found", False)


def test_reserve_and_release_are_not_executors():
    """reserve_stock y release_stock los ejecuta inventory (T4)"""
    with pytest.raises(KeyError):
        _build("reserve_stock")
    assert set(cli.ACTIONS) == set(EXECUTOR_ACTIONS)


def test_the_legacy_prompt_with_sku_does_not_change():
    request = DraftRequest(kind="claim", incident_id="I-1", order_id="P-1", sku="SKU-1", quantity=2, cause="loss",
                           estimated_delay_hours=0)
    assert "Pedido: P-1 (2 x SKU-1)." in _prompt(request)


def test_transient_errors_come_from_the_service():
    with pytest.raises(Transient):
        FakeService("mail", "M", down=True).call("K")


# --- la saga y el cableado ------------------------------------------------------------------


def _saga_step(action: str) -> dict:
    return {"step_id": "S1", "action": action, "params": {"order_id": "P-1", "amount": None}, "depends_on": [],
            "origin_plan_version": 1}


INCIDENT = {"cause": "delay", "estimated_delay_hours": 48}


@pytest.mark.parametrize("action", ["notify_customer", "file_claim"])
def test_the_saga_sends_the_cause_to_the_drafted_actions(action):
    """El comando lleva lo que su destinatario necesita: la causa y el retraso para el redactor"""
    request = rules.step_request("I-1", 1, _saga_step(action))
    command = rules.execute_step_command(request, INCIDENT)
    assert (command["cause"], command["estimated_delay_hours"]) == ("delay", 48)
    assert "cause" not in request  # StepRequested no cambia


@pytest.mark.parametrize("action", ["reserve_stock", "release_stock", "reissue_shipment", "issue_refund"])
def test_other_actions_keep_their_command(action):
    request = rules.step_request("I-1", 1, _saga_step(action))
    assert rules.execute_step_command(request, INCIDENT) == request


def test_executor_packages_are_checked_components():
    for action in EXECUTOR_ACTIONS:
        assert ownership.COMPONENT_PACKAGES[f"eda_retail/executor_{action}"] == f"executor_{action}"


def test_executors_read_no_table_of_other_components():
    """Dato de otro dominio: el pedido se consulta en view_orders, no en las tablas de inventory ni del orquestador"""
    paths = [ROOT / "src/eda_retail/step_executors", *(ROOT / f"src/eda_retail/executor_{a}" for a in EXECUTOR_ACTIONS)]
    sources = "\n".join(p.read_text() for d in paths for p in d.glob("*.py"))
    for foreign in ("inventory.", "orchestrator.", "FROM orders", "FROM incidents"):
        assert foreign not in sources


def test_run_agents_starts_the_four_executors():
    script = (ROOT / "scripts" / "run-agents.sh").read_text()
    for action in EXECUTOR_ACTIONS:
        assert f"eda-step-executor {action}" in script
