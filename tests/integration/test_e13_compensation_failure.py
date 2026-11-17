"""E13 Fallo de compensación (spec experiments) de punta a punta con el LLM oráculo. Una sola ejecución de E13 para
todos los escenarios: con la semilla 13 y 10 incidencias hay 6 compensaciones y fallan 3."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e13_compensation_failure import run_e13

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e13(base_settings, seed=13, incidents=10)


def _detail(report) -> str:
    keys = ("compensations", "failed_incidents", "completed_incidents", "escalation_reasons", "open")
    return json.dumps({k: report.metrics[k] for k in keys}, default=str)


def test_stock_release_fails(report):
    """La liberación del stock falla"""
    failed = report.metrics["failed_incidents"]
    assert failed["total"] > 0, _detail(report)
    assert failed["escalated_compensation_failed"] == failed["total"], _detail(report)
    assert failed["refunds_requested"] == 0, _detail(report)


def test_no_compensations_after_the_escalation(report):
    """Sin compensaciones tras el escalado"""
    assert report.metrics["failed_incidents"]["compensations_after_escalation"] == 0, _detail(report)


def test_compensation_completes(report):
    """La compensación se completa"""
    completed = report.metrics["completed_incidents"]
    assert completed["total"] > 0, _detail(report)
    assert completed["refunded"] == completed["total"], _detail(report)


def test_report_with_invariants(report):
    """Informe con invariantes"""
    invariants = report.metrics["invariants"]
    assert set(invariants) == {"I1", "I2", "I3", "I4", "I5"}
    broken = {name: inv for name, inv in invariants.items() if inv["violations"] != 0}
    assert broken == {}, json.dumps(broken, default=str)


def test_h13(report):
    """Cumplimiento de H13"""
    assert report.metrics["open"] == 0, _detail(report)
    assert report.hypothesis == {"H13": True}, _detail(report)
