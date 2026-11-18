"""E20 Dependencias de V2 (spec experiments) de punta a punta, con el servidor de correo y el agente del transportista
en proceso. Una sola ejecución de E20 (las seis variantes) para todos los escenarios: con la semilla 20 y 15
incidencias, 14 consultan al transportista (6 elegidas en `carrier_slow` y 7 en `carrier_401` y `carrier_bad_card`) y
de las 15 claves de `notify_customer` del primer plan, 4 reciben una salida inválida en `mail_invalid` y 4 fallan en
su primera llamada en `mail_down` (2 en los 3 intentos). Calculado sin base de datos con `generate`,
`compute_idempotency_key` y `v2_faults.chosen`."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e20_v2_dependencies import A2A_TIMEOUT_S, LATENCY_MARGIN_S, run_e20

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e20(base_settings, seed=20, incidents=15)


def _detail(report, variant: str) -> str:
    m = report.metrics["variants"][variant]
    keys = ("open", "escalation_reasons", "injected", "carrier", "carrier_outcomes", "outcome_mismatches",
            "max_consult_latency_ms", "plans_differing_from_baseline", "step_failed_errors", "mail_calls", "mail_keys",
            "max_mail_executions_per_key", "events_with_address")
    return json.dumps({k: m[k] for k in keys}, default=str)


def test_mail_server_down(report):
    """Servidor de correo caído"""
    m = report.metrics["variants"]["mail_down"]
    assert m["injected"]["mail"] > 0 and m["mail_calls"] > m["mail_keys"], _detail(report, "mail_down")
    assert m["step_failed_errors"].get("mail_unavailable", 0) > 0, _detail(report, "mail_down")
    assert m["mail_unavailable_not_retryable"] == 0, _detail(report, "mail_down")
    assert m["max_mail_executions_per_key"] <= 1, _detail(report, "mail_down")


def test_invalid_mail_server_outputs(report):
    """Salidas inválidas del servidor de correo"""
    m = report.metrics["variants"]["mail_invalid"]
    assert m["injected"]["mail"] > 0, _detail(report, "mail_invalid")
    assert m["step_failed_errors"].get("mcp_output_invalid", 0) == m["injected"]["mail"], _detail(report,
                                                                                               "mail_invalid")
    assert m["events_with_address"] == 0, _detail(report, "mail_invalid")


@pytest.mark.parametrize("variant, outcome", [("carrier_slow", "timeout"), ("carrier_401", "failed"),
                                              ("carrier_bad_card", "rejected_by_guardrail")])
def test_unstable_carrier_agent(report, variant, outcome):
    """Agente del transportista inestable"""
    m = report.metrics["variants"][variant]
    assert m["carrier_outcomes"].get(outcome, 0) == m["injected"]["carrier"] > 0, _detail(report, variant)
    assert m["outcome_mismatches"] == [], _detail(report, variant)
    assert m["max_consult_latency_ms"] <= (A2A_TIMEOUT_S + LATENCY_MARGIN_S) * 1000, _detail(report, variant)
    if variant != "carrier_slow":
        assert m["carrier"]["tasks_on_injected"] == 0, _detail(report, variant)
    assert m["plans_differing_from_baseline"] == [], _detail(report, variant)


def test_report_with_invariants(report):
    """Informe con invariantes"""
    for m in report.metrics["variants"].values():
        assert set(m["invariants"]) == {"I1", "I2", "I3", "I4", "I5"}
        broken = {name: inv for name, inv in m["invariants"].items() if inv["violations"] != 0}
        assert broken == {}, json.dumps(broken, default=str)


def test_h20(report):
    """Cumplimiento de H20"""
    for variant, m in report.metrics["variants"].items():
        assert m["h20"], _detail(report, variant)
    assert report.hypothesis == {"H20": True}
