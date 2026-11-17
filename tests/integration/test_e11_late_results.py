"""E11 Entrega retrasada y planes revisados (spec experiments) y entrega retrasada por acción (spec
synthetic-data), de punta a punta con el LLM oráculo. Una sola ejecución de E11 para todos los escenarios."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e11_late_results import run_e11

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e11(base_settings, seed=11, incidents=10)


def test_results_arrive_late(report):
    """Resultado tardío: el arnés retrasó resultados de pasos y venció algún plazo"""
    assert report.metrics["delayed_results"] > 0
    assert report.metrics["plan_revised"]["total"] > 0


def test_result_of_an_old_version_after_a_revision(report):
    """Resultado de una versión antigua tras una revisión"""
    assert report.metrics["stale_state_changes"] == 0
    assert report.metrics["invariants"]["I2"]["violations"] == 0


def test_result_after_the_deadline(report):
    """Resultado después del plazo"""
    assert report.metrics["compensations"]["requested_more_than_once"] == 0
    assert report.metrics["compensations"]["completed"] == report.metrics["compensations"]["requested"]


def test_refund_with_pending_compensations(report):
    """Reembolso con compensaciones pendientes"""
    assert report.metrics["invariants"]["I3"]["violations"] == 0


def test_report_with_invariants(report):
    """Informe con invariantes"""
    invariants = report.metrics["invariants"]
    assert set(invariants) == {"I1", "I2", "I3", "I4", "I5"}
    broken = {name: inv for name, inv in invariants.items() if inv["violations"] != 0}
    assert broken == {}, json.dumps(broken, default=str)


def test_h11(report):
    """Cumplimiento de H11"""
    assert report.metrics["open"] == 0
    detail = {k: report.metrics[k] for k in ("stale_results", "stale_state_changes", "stale_state_change_detail",
                                             "a4_compensations", "compensations", "timeouts", "delayed_results")}
    assert report.hypothesis == {"H11": True}, json.dumps(detail, default=str)
