"""Command handler del replanificador (T8): todo lo que no necesita la base de datos."""

from __future__ import annotations

import pathlib
import uuid
from decimal import Decimal

from eda_retail.infra import ownership
from eda_retail.infra.commands import CommandEnvelope
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import BUDGET_EXHAUSTED, LLM_OUTPUT_INVALID, Budget
from eda_retail.llm.prompts.replanner import check_step_ids
from eda_retail.queries.cost_per_incident import budget_from_cost
from eda_retail.replanner import handler
from eda_retail.rules import BusinessRules

RULES = BusinessRules()


def _step(step_id: str, action: str, order: str = "P-1", depends_on=(), amount=None) -> dict:
    return {"step_id": step_id, "action": action, "params": {"order_id": order, "amount": amount},
            "depends_on": list(depends_on)}


def _current(step_id: str, action: str, status: str, depends_on=(), order: str = "P-1") -> dict:
    return {**_step(step_id, action, order, depends_on), "status": status, "origin_plan_version": 1}


# Pérdida: notify completado, file_claim pendiente, reserve_stock falló sin stock, reissue pendiente.
OUT_OF_STOCK = [
    _current("S1", "notify_customer", "completed"),
    _current("S2", "file_claim", "pending"),
    _current("S3", "reserve_stock", "failed"),
    _current("S4", "reissue_shipment", "pending", ["S3"]),
]
REFUND_PLAN = {"steps": [_step("S2", "file_claim"), _step("S5", "issue_refund", amount="40.00")]}
AMOUNTS = {"P-1": Decimal("40.00")}


def _payload(steps=OUT_OF_STOCK, *, revisions=0, failed="S3", error="out_of_stock") -> dict:
    return {"plan_version": 1, "revisions": revisions, "failed_step_id": failed, "error": error, "steps": steps}


def _command(payload: dict) -> tuple[CommandEnvelope, handler.RevisePlan]:
    command = CommandEnvelope(command_id=uuid.uuid4(), command_type="RevisePlan", correlation_id="I-1",
                              causation_id=uuid.uuid4(), payload=payload)
    return command, handler.RevisePlan.model_validate(payload)


def _preparer(llm, *, revised=False, budget=None, amounts=None, closed=False):
    context = handler.RevisionContext(revised=revised, budget=budget or budget_from_cost(None, RULES),
                                      amounts=AMOUNTS if amounts is None else amounts, closed=closed)
    seen = []

    def read(incident_id, base_version, order_ids):
        seen.append((incident_id, base_version, sorted(order_ids)))
        return context

    preparer = handler.RevisionPreparer(llm, RULES, context=read)
    preparer.seen = seen  # type: ignore[attr-defined]
    return preparer


def _check(proposed, current=OUT_OF_STOCK, amounts=AMOUNTS):
    return handler.check_revision(current, proposed, RULES, amounts)


def test_the_command_carries_the_current_plan():
    """Decisión del replanificador: el comando lleva el plan vigente, el paso fallido y las revisiones"""
    assert set(handler.RevisePlan.model_fields) == {"plan_version", "revisions", "failed_step_id", "error",
                                                    "steps"}
    assert set(handler.CurrentStep.model_fields) == {"step_id", "action", "params", "depends_on", "status",
                                                     "origin_plan_version"}


def test_out_of_stock_replaces_the_reissue_with_a_refund():
    """Sin stock en el almacén: el replanner propone sustituir el reenvío por un reembolso"""
    llm = FakeLlmClient([REFUND_PLAN])
    command, payload = _command(_payload())

    prepared = _preparer(llm)(command, payload)

    assert [s["action"] for s in prepared.steps] == ["file_claim", "issue_refund"]
    assert prepared.reason is None and len(prepared.calls) == 1
    [request] = llm.requests
    assert request.agent == "replanner"
    assert "Plan vigente (versión 1)" in request.prompt
    assert "- S1: notify_customer" in request.prompt and "-> completed" in request.prompt
    assert "el paso S3 terminó con el error out_of_stock" in request.prompt


def test_prepare_reads_the_amounts_of_the_plan_orders():
    """Dato de otro dominio: los importes de los pedidos del plan se consultan en view_orders"""
    llm = FakeLlmClient([REFUND_PLAN])
    command, payload = _command(_payload())
    preparer = _preparer(llm)
    preparer(command, payload)
    assert preparer.seen == [("I-1", 1, ["P-1"])]  # type: ignore[attr-defined]


def test_a_rejected_proposal_is_explained_to_the_llm_and_asked_again():
    llm = FakeLlmClient([{"steps": [_step("S1", "notify_customer")]}, REFUND_PLAN])
    command, payload = _command(_payload())

    prepared = _preparer(llm)(command, payload)

    assert [s["step_id"] for s in prepared.steps] == ["S2", "S5"]
    assert len(llm.requests) == 2
    assert "completed_step_in_plan" in llm.requests[1].prompt


def test_three_invalid_proposals_end_without_a_plan():
    llm = FakeLlmClient([{"steps": [_step("S1", "delete_order")]}] * RULES.max_generations)
    command, payload = _command(_payload())
    prepared = _preparer(llm)(command, payload)
    assert (prepared.steps, prepared.reason) == (None, LLM_OUTPUT_INVALID)
    assert len(prepared.calls) == RULES.max_generations


def test_revisions_exhausted_escalate_without_llm():
    """Revisiones agotadas: max_revisions sin llamar al LLM"""
    llm = FakeLlmClient([])
    command, payload = _command(_payload(revisions=RULES.max_plan_revisions))
    prepared = _preparer(llm)(command, payload)
    assert (prepared.steps, prepared.reason, llm.requests) == (None, "max_revisions", [])


def test_prepare_without_budget_does_not_call_the_llm():
    spent = Budget(RULES.max_tokens_per_incident, 0, RULES.max_tokens_per_incident, RULES.max_llm_calls_per_incident)
    llm = FakeLlmClient([])
    command, payload = _command(_payload())
    prepared = _preparer(llm, budget=spent)(command, payload)
    assert (prepared.steps, prepared.reason, llm.requests) == (None, BUDGET_EXHAUSTED, [])


def test_a_version_already_revised_does_not_pay_the_llm():
    """Revisión ya hecha: se detecta antes de llamar al LLM"""
    llm = FakeLlmClient([])
    command, payload = _command(_payload())
    prepared = _preparer(llm, revised=True)(command, payload)
    assert prepared.revised is True
    assert llm.requests == []


def test_a_kept_step_keeps_its_step_id():
    """Paso mantenido entre versiones: file_claim conserva el step_id S2"""
    steps = _check(REFUND_PLAN["steps"])
    assert [s.step_id for s in steps] == ["S2", "S5"]


def test_a_completed_step_cannot_come_back():
    """Sin stock en el almacén: no vuelve a incluir notify_customer, que ya estaba completado"""
    assert _check([_step("S1", "notify_customer"), _step("S5", "issue_refund", amount="40.00")]).startswith(
        "completed_step_in_plan")


def test_a_compensated_step_cannot_come_back():
    current = [_current("S1", "reserve_stock", "compensated"), _current("S2", "reissue_shipment", "failed", ["S1"])]
    assert _check([_step("S1", "reserve_stock")], current).startswith("completed_step_in_plan")


def test_a_new_step_may_depend_on_a_completed_one():
    current = [_current("S1", "reserve_stock", "completed"), _current("S2", "reissue_shipment", "failed", ["S1"])]
    [step] = _check([_step("S2", "reissue_shipment", depends_on=["S1"])], current)
    assert step.depends_on == ["S1"]


def test_a_kept_step_cannot_change_its_action():
    assert _check([_step("S2", "issue_refund", amount="10.00")]).startswith("invalid_plan")


def test_a_new_step_repeating_an_open_one_is_rejected():
    """Paso nuevo que repite uno abierto: step_id_changed, hay que reutilizar su step_id"""
    reason = _check([_step("S9", "file_claim"), _step("S5", "issue_refund", amount="40.00")])
    assert reason.startswith("step_id_changed")
    assert "S2" in reason


def test_refund_after_a_completed_reissue_is_rejected():
    """Reembolso tras un reenvío completado: refund_and_reissue_conflict"""
    current = [_current("S1", "reissue_shipment", "completed"), _current("S2", "file_claim", "failed")]
    assert _check([_step("S3", "issue_refund", amount="10.00")], current).startswith("refund_and_reissue_conflict")


def test_a_compensation_in_the_proposal_is_rejected():
    """Propuesta del LLM con una compensación incluida: compensation_not_allowed_in_plan"""
    assert _check([_step("S3-comp", "release_stock")]).startswith("compensation_not_allowed_in_plan")


def test_refund_above_the_order_amount_is_rejected():
    assert _check([_step("S2", "file_claim"), _step("S5", "issue_refund", amount="60.00")]).startswith(
        "refund_limit")


def test_legacy_step_id_check_works_on_plain_steps():
    """El agente anterior y el componente comparten la regla de estabilidad de los step_id"""
    assert check_step_ids(OUT_OF_STOCK, [_step("S9", "file_claim")]).startswith("step_id_changed")
    assert check_step_ids(OUT_OF_STOCK, [_step("S2", "file_claim")]) is None


def test_replanner_is_a_checked_component():
    assert ownership.COMPONENT_PACKAGES["eda_retail/replanner"] == "replanner"


def test_replanner_does_not_read_the_orchestrator_tables():
    """Decisión del replanificador: no lee las tablas del orquestador; de incident_status solo pregunta, por la
    consulta del lado de lectura, si la incidencia ya está cerrada"""
    package = pathlib.Path(handler.__file__).parent
    sources = "\n".join(p.read_text() for p in package.glob("*.py"))
    assert "orchestrator." not in sources
    assert "from eda_retail.queries.incident_status import INCIDENT_CLOSED, incident_closed" in sources
    assert "view_orders" in sources


def test_run_agents_starts_the_replanner_component():
    script = (pathlib.Path(__file__).parents[1] / "scripts" / "run-agents.sh").read_text()
    assert "replanner|eda-replanner" in script
    assert "eda-agent replanner" not in script


def test_a_revise_plan_of_an_escalated_incident_does_not_call_the_llm():
    """Trabajo de una incidencia ya cerrada: un RevisePlan de una incidencia que la saga escaló con
    plan_timeout no llama al LLM y no publica eventos"""
    llm = FakeLlmClient([REFUND_PLAN])
    command, payload = _command(_payload())

    prepared = _preparer(llm, closed=True)(command, payload)
    outcome = handler.revise_plan(RULES)(None, command, payload, prepared)  # sin tocar la base de datos

    assert llm.requests == [] and prepared.calls == []
    assert outcome.reason == "incident_closed"


def test_a_closed_incident_with_exhausted_revisions_does_not_escalate_again():
    """Trabajo de una incidencia ya cerrada: sin revisiones restantes tampoco publica otro IncidentEscalated"""
    command, payload = _command(_payload(revisions=RULES.max_plan_revisions))

    prepared = _preparer(FakeLlmClient([]), closed=True)(command, payload)

    assert handler.revise_plan(RULES)(None, command, payload, prepared).reason == "incident_closed"


def test_an_open_incident_is_still_revised():
    """Trabajo de una incidencia ya cerrada: una incidencia abierta (o sin fila en la vista) se revisa igual"""
    llm = FakeLlmClient([REFUND_PLAN])
    command, payload = _command(_payload())

    prepared = _preparer(llm, closed=False)(command, payload)

    assert len(llm.requests) == 1 and not prepared.closed
    assert [s["action"] for s in prepared.steps] == ["file_claim", "issue_refund"]
