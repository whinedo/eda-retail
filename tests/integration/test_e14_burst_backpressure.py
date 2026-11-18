"""E14 Ráfaga y contrapresión (spec experiments) de punta a punta con el LLM oráculo. Una sola ejecución de E14 (las
dos variantes) para todos los escenarios, con 20 incidencias y capacidad 1 para que el 503 llegue seguro."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e14_burst_backpressure import run_e14

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e14(base_settings, seed=14, incidents=20, capacity=1)


def _detail(report, variant: str = "backpressure") -> str:
    m = report.metrics["variants"][variant]
    keys = ("open", "escalation_reasons", "unavailable_503", "command_attempts", "max_open_jobs", "final_queues",
            "over_capacity", "duplicate_commands")
    return json.dumps({k: m[k] for k in keys}, default=str)


def test_saturated_executor(report):
    """Ejecutor saturado"""
    m = report.metrics["variants"]["backpressure"]
    assert sum(m["unavailable_503"].values()) > 0, _detail(report)
    assert m["command_attempts"]["retried"] > 0, _detail(report)
    assert report.metrics["variants"]["baseline"]["unavailable_503"] == {}, _detail(report, "baseline")


def test_queues_drain(report):
    """Las colas se vacían"""
    for variant, m in report.metrics["variants"].items():
        assert m["final_queues"]["pending_commands"] == 0, _detail(report, variant)
        assert not any(m["final_queues"]["open_jobs"].values()), _detail(report, variant)
    assert report.metrics["variants"]["backpressure"]["over_capacity"] == {}, _detail(report)


def test_no_duplicate_commands(report):
    """Sin comandos duplicados"""
    for variant, m in report.metrics["variants"].items():
        assert m["duplicate_commands"] == [], _detail(report, variant)


def test_report_with_invariants(report):
    """Informe con invariantes"""
    for m in report.metrics["variants"].values():
        assert set(m["invariants"]) == {"I1", "I2", "I3", "I4", "I5"}
        broken = {name: inv for name, inv in m["invariants"].items() if inv["violations"] != 0}
        assert broken == {}, json.dumps(broken, default=str)


def test_h14(report):
    """Cumplimiento de H14"""
    assert report.metrics["variants"]["backpressure"]["open"] == 0, _detail(report)
    assert report.hypothesis == {"H14": True}, _detail(report)
