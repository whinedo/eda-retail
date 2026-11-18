"""E15 Degradación del LLM (spec experiments) de punta a punta con el LLM oráculo degradado. Una sola ejecución de E15
(las cuatro variantes) para todos los escenarios: con la semilla 15 y 15 incidencias, 13 llaman al planner, 4 agotan
sus generaciones en `invalid` y una falla en su primera llamada en `error`. El plazo de plan de `slow` (1,5 s) queda
por debajo de su cola del planner (13 llamadas de 0,5 s); `error` mantiene 4 s para que su único trabajo reintentado
no se adelante al plazo."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e15_llm_degradation import run_e15

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e15(base_settings, seed=15, incidents=15, slow_plan_timeout_s=1.5)


def _detail(report, variant: str) -> str:
    m = report.metrics["variants"][variant]
    keys = ("open", "escalation_reasons", "injected", "llm_calls", "llm_invalid_calls", "jobs",
            "plans_from_invalid_output", "plans_after_escalation", "steps_after_escalation")
    return json.dumps({k: m[k] for k in keys}, default=str)


def test_invalid_llm_outputs(report):
    """Salidas inválidas del LLM"""
    m = report.metrics["variants"]["invalid"]
    assert m["escalation_reasons"].get("llm_output_invalid", 0) > 0, _detail(report, "invalid")
    assert m["plans_from_invalid_output"] == [], _detail(report, "invalid")


def test_slow_llm_and_plan_deadline(report):
    """LLM lento y plazo del plan"""
    m = report.metrics["variants"]["slow"]
    assert m["escalation_reasons"].get("plan_timeout", 0) > 0, _detail(report, "slow")
    assert m["steps_after_escalation"] == [], _detail(report, "slow")


def test_llm_errors_and_retried_jobs(report):
    """Errores del LLM y trabajos reintentados"""
    m = report.metrics["variants"]["error"]
    assert sum(j["retried"] for j in m["jobs"].values()) > 0, _detail(report, "error")


def test_report_with_invariants(report):
    """Informe con invariantes"""
    for m in report.metrics["variants"].values():
        assert set(m["invariants"]) == {"I1", "I2", "I3", "I4", "I5"}
        broken = {name: inv for name, inv in m["invariants"].items() if inv["violations"] != 0}
        assert broken == {}, json.dumps(broken, default=str)


def test_h15(report):
    """Cumplimiento de H15"""
    for variant, m in report.metrics["variants"].items():
        assert m["h15"], _detail(report, variant)
    assert report.hypothesis == {"H15": True}
