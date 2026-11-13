"""llm-boundaries: salida con esquema validado, sin herramientas, reintentos y presupuesto."""

from __future__ import annotations

import json
from types import SimpleNamespace

from eda_retail.llm.anthropic_client import FALLBACK_BETA, AnthropicClient
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import Budget, LlmRequest, generate
from eda_retail.llm.schemas import PlanProposal, strict_schema

VALID_PLAN = {
    "steps": [{"step_id": "s1", "action": "notify_customer", "params": {"order_id": "P1"}, "depends_on": []}]
}
NO_DEPENDS_ON = {"steps": [{"step_id": "s1", "action": "notify_customer", "params": {"order_id": "P1"}}]}


def _request() -> LlmRequest:
    return LlmRequest(
        agent="planner", incident_id="I1", system="Eres el planificador.", prompt="Propón un plan.",
        schema=strict_schema(PlanProposal),
    )


def _budget(**kw) -> Budget:
    return Budget(**{"tokens_used": 0, "llm_calls": 0, "max_tokens": 10_000, "max_calls": 10, **kw})


def test_plan_missing_required_field():
    """Plan sin campo obligatorio"""
    fake = FakeLlmClient([NO_DEPENDS_ON])
    result = generate(fake, _request(), PlanProposal, max_generations=1, budget=_budget())
    assert result.value is None
    assert result.reason == "llm_output_invalid"
    [call] = result.calls
    assert call.validation_passed is False
    assert "depends_on" in result.errors[0]


def test_agent_has_no_effect_tools():
    """Agente que intenta actuar"""
    captured: dict = {}

    def create(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            stop_reason="end_turn", model="claude-opus-5-5",
            content=[SimpleNamespace(type="text", text=json.dumps(VALID_PLAN))],
            usage=SimpleNamespace(input_tokens=10, output_tokens=5, cache_creation_input_tokens=0,
                                  cache_read_input_tokens=0),
        )

    sdk = SimpleNamespace(beta=SimpleNamespace(messages=SimpleNamespace(create=create)))
    client = AnthropicClient(model="claude-opus-5-5", effort="medium", sdk=sdk)
    response = client.complete(_request())
    assert "tools" not in captured and "tool_choice" not in captured
    assert captured["output_config"]["format"]["type"] == "json_schema"
    assert captured["fallbacks"] == "default" and captured["betas"] == [FALLBACK_BETA]
    assert (response.tokens_in, response.tokens_out) == (10, 5)


def test_three_invalid_plans():
    """Tres planes inválidos seguidos"""
    fake = FakeLlmClient([NO_DEPENDS_ON, "no es JSON", {"steps": []}])
    result = generate(fake, _request(), PlanProposal, max_generations=3, budget=_budget())
    assert result.reason == "llm_output_invalid"
    assert [c.attempt for c in result.calls] == [1, 2, 3]
    assert not any(c.validation_passed for c in result.calls)
    assert "rechazó" in fake.requests[1].prompt  # cada nueva petición explica el rechazo anterior


def test_token_budget_exhausted_makes_no_call():
    """Presupuesto de tokens agotado"""
    fake = FakeLlmClient([VALID_PLAN])
    result = generate(fake, _request(), PlanProposal, max_generations=3, budget=_budget(tokens_used=10_000))
    assert result.reason == "budget_exhausted"
    assert fake.requests == [] and result.calls == []


def test_budget_exhausted_between_generations():
    """El presupuesto se agota entre dos generaciones: no se hace la siguiente llamada"""
    fake = FakeLlmClient([NO_DEPENDS_ON, VALID_PLAN])
    result = generate(fake, _request(), PlanProposal, max_generations=3, budget=_budget(max_calls=1))
    assert result.reason == "budget_exhausted"
    assert len(fake.requests) == 1


def test_valid_on_second_attempt_and_business_check():
    """Una salida que pasa el esquema pero no las reglas de negocio también cuenta como rechazo"""
    fake = FakeLlmClient([VALID_PLAN, VALID_PLAN])
    seen: list[int] = []

    def check(plan: PlanProposal) -> str | None:
        seen.append(len(plan.steps))
        return "regla incumplida" if len(seen) == 1 else None

    result = generate(fake, _request(), PlanProposal, max_generations=3, budget=_budget(), check=check)
    assert result.reason is None and result.value is not None
    assert [c.validation_passed for c in result.calls] == [False, True]
    assert result.errors == ["regla incumplida"]


def test_refusal_is_an_invalid_output():
    """Un rechazo del modelo (stop_reason refusal) se trata como salida inválida"""
    fake = FakeLlmClient([FakeLlmClient.REFUSAL])
    result = generate(fake, _request(), PlanProposal, max_generations=1, budget=_budget())
    assert result.reason == "llm_output_invalid"
    assert result.errors == ["llm_refused"]


def test_strict_schema_closes_every_object():
    """El esquema enviado al modelo no admite campos extra y exige depends_on"""
    schema = strict_schema(PlanProposal)

    def objects(node):
        if isinstance(node, dict):
            if node.get("type") == "object":
                yield node
            for v in node.values():
                yield from objects(v)
        elif isinstance(node, list):
            for v in node:
                yield from objects(v)

    found = list(objects(schema))
    assert found and all(o.get("additionalProperties") is False for o in found)
    step = schema["$defs"]["ProposedStep"]
    assert "depends_on" in step["required"]
