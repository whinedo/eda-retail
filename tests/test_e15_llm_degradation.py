"""E15 Degradación del LLM (spec experiments) y Degradación del LLM (spec synthetic-data): cliente degradado, métricas
del log y H15.

Sin Kafka ni PostgreSQL. La ejecución de punta a punta está en tests/integration/test_e15_llm_degradation.py.
"""

from __future__ import annotations

import uuid

import pytest

from eda_retail.experiments.e15_llm_degradation import h15_variant, provoked, summarize
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.llm.generation import LlmRequest
from eda_retail.synthetic.llm_faults import MISSING_FIELDS, NOT_JSON, DegradedLlm, LlmUnavailable


def _request(agent: str = "planner", incident: str = "INC-1") -> LlmRequest:
    return LlmRequest(agent=agent, incident_id=incident, system="s", prompt="p", schema={})


def _client() -> FakeLlmClient:
    return FakeLlmClient(responder=lambda request: {"steps": []})


def _texts(llm: DegradedLlm, calls: int, incident: str = "INC-1") -> list[str]:
    return [llm.complete(_request(incident=incident)).text for _ in range(calls)]


def test_choice_is_reproducible():
    """Elección reproducible"""
    first = _texts(DegradedLlm(_client(), "invalid", seed=7, probability=0.5), 20)
    second = _texts(DegradedLlm(_client(), "invalid", seed=7, probability=0.5), 20)
    assert first == second
    assert {NOT_JSON, MISSING_FIELDS} & set(first) and '{"steps": []}' in first


def test_agent_not_degraded():
    """Agente no degradado"""
    llm = DegradedLlm(_client(), "invalid", seed=7, probability=1.0)
    assert llm.complete(_request("interpreter")).text == '{"steps": []}'
    assert llm.complete(_request("planner")).text in (NOT_JSON, MISSING_FIELDS)
    assert llm.injected == {"planner": 1}


def test_error_mode_raises():
    """Errores del LLM y trabajos reintentados: la llamada elegida lanza una excepción"""
    llm = DegradedLlm(_client(), "error", seed=7, probability=1.0)
    with pytest.raises(LlmUnavailable):
        llm.complete(_request())
    assert llm.injected == {"planner": 1}


def test_slow_mode_waits_and_reports_latency():
    """LLM lento y plazo del plan: cada llamada tarda los segundos dados más"""
    slept = []
    llm = DegradedLlm(_client(), "slow", delay_seconds=0.5, sleep=slept.append)
    response = llm.complete(_request("replanner"))
    assert slept == [0.5] and response.latency_ms >= 500 and response.text == '{"steps": []}'


def test_invalid_values_are_rejected():
    """Degradación del LLM: modo conocido, probabilidad en [0, 1] y retraso no negativo"""
    for kwargs in ({"mode": "flaky"}, {"mode": "invalid", "probability": 1.5},
                   {"mode": "slow", "delay_seconds": -1}):
        with pytest.raises(ValueError):
            DegradedLlm(_client(), **kwargs)


class Log:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def add(self, incident: str, event_type: str, cause=None, **payload) -> None:
        self.events.append({"event_type": event_type, "correlation_id": incident, "causation_id": cause,
                            "payload": payload})

    def plan(self, incident: str, *, valid: bool) -> None:
        cause = uuid.uuid4()
        self.add(incident, "LlmCallRecorded", cause, agent="planner", validation_passed=False, attempt=1)
        self.add(incident, "LlmCallRecorded", cause, agent="planner", validation_passed=valid, attempt=2)
        self.add(incident, "PlanCreated", cause, plan_version=1)


def test_plan_from_a_valid_call_is_not_flagged():
    """Salidas inválidas del LLM: un plan cuya última llamada pasó la validación no cuenta, aunque hubiera una inválida
    antes"""
    log = Log()
    log.plan("I-1", valid=True)
    m = summarize(log.events)
    assert m["plans_from_invalid_output"] == []
    assert m["llm_calls"] == {"planner": 2} and m["llm_invalid_calls"] == {"planner": 1}


def test_plan_from_an_invalid_call_is_flagged():
    """Salidas inválidas del LLM: todo plan publicado viene de una llamada que pasó la validación"""
    log = Log()
    log.plan("I-1", valid=False)
    assert summarize(log.events)["plans_from_invalid_output"] == [
        {"incident_id": "I-1", "event_type": "PlanCreated", "plan_version": 1}]


def test_steps_after_escalation_except_compensations():
    """LLM lento y plazo del plan: tras el escalado no se pide ningún paso del plan; las compensaciones sí"""
    log = Log()
    log.add("I-1", "IncidentEscalated", reason="plan_timeout")
    log.plan("I-1", valid=True)
    log.add("I-1", "StepRequested", step_id="S3-comp", action="release_stock")
    m = summarize(log.events)
    assert m["plans_after_escalation"] == 1 and m["steps_after_escalation"] == []
    log.add("I-1", "StepRequested", step_id="S1", action="notify_customer")
    assert summarize(log.events)["steps_after_escalation"] == [
        {"incident_id": "I-1", "step_id": "S1", "action": "notify_customer"}]


def _clean(**changes) -> dict:
    m = {"open": 0, "plans_from_invalid_output": [], "steps_after_escalation": [],
         "llm_invalid_calls": {"planner": 2}, "escalation_reasons": {"plan_timeout": 1},
         "jobs": {"planner": {"retried": 1}, "replanner": {"retried": 0}},
         "invariants": {name: {"violations": 0} for name in ("I1", "I2", "I3", "I4", "I5")}}
    return {**m, **changes}


def test_each_variant_must_provoke_its_effect():
    """Cumplimiento de H15: cada variante con degradación provoca su efecto"""
    assert all(provoked(v, _clean()) for v in ("baseline", "invalid", "slow", "error"))
    assert not provoked("invalid", _clean(llm_invalid_calls={}))
    assert not provoked("slow", _clean(escalation_reasons={"llm_output_invalid": 3}))
    assert not provoked("error", _clean(jobs={"planner": {"retried": 0}}))


def test_h15_fails_on_any_broken_condition():
    """Cumplimiento de H15"""
    assert h15_variant("invalid", _clean())
    broken = [
        {"open": 1},
        {"plans_from_invalid_output": [{"incident_id": "I-1"}]},
        {"steps_after_escalation": [{"incident_id": "I-1"}]},
        {"invariants": {**_clean()["invariants"], "I1": {"violations": 1}}},
        {"llm_invalid_calls": {}},
    ]
    for change in broken:
        assert not h15_variant("invalid", _clean(**change)), change


def test_expexecutor_checks_h15():
    """Cumplimiento de H15 en el catálogo de `eda-expexecutor`"""
    from eda_retail.expexecutor.catalog import CATALOG

    variant = {**_clean(), "h15": True}
    variants = {v: variant for v in ("baseline", "invalid", "slow", "error")}
    ok = {"hypothesis": {"H15": True}, "metrics": {"variants": variants}}
    assert all(c.ok for c in CATALOG["e15"].checks(ok))
    bad = {"hypothesis": {"H15": False},
           "metrics": {"variants": {**variants, "slow": {**variant, "h15": False,
                                                         "steps_after_escalation": [{"incident_id": "I-1"}]}}}}
    assert [c.metric for c in CATALOG["e15"].checks(bad) if not c.ok] == [
        "hypothesis.H15", "metrics.variants.slow.h15", "metrics.variants.slow.steps_after_escalation"]


def test_cli_runs_e15():
    """Ejecución de un experimento: E15 está en la CLI con 50 incidencias por defecto"""
    from eda_retail.experiments.cli import DEFAULT_INCIDENTS, EXPERIMENTS

    assert EXPERIMENTS["e15"].__name__ == "run_e15"
    assert DEFAULT_INCIDENTS["e15"] == 50


def test_failed_llm_jobs_are_closed_by_a_short_plan_deadline():
    """Errores del LLM y trabajos reintentados: si el trabajo agota `JOB_MAX_ATTEMPTS`, la saga escala la incidencia
    al vencer el plazo, así que `error` (como `slow`) usa el plazo corto; con la semilla 42, el replanner de una
    incidencia falla seis veces seguidas"""
    from eda_retail.experiments.e15_llm_degradation import plan_timeout_for
    from eda_retail.synthetic.faults import should_fail

    assert plan_timeout_for("error", 1.5) == 4.0 and plan_timeout_for("slow", 1.5) == 1.5
    assert plan_timeout_for("baseline") is None and plan_timeout_for("invalid") is None
    incident = "I-876a9512-c8aa-5363-8533-2c17eff43122"
    assert all(should_fail(42, "error", f"replanner:{incident}:{n}", 0.3) for n in range(1, 7))
