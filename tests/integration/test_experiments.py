"""experiments: informe reproducible, E1 y E2 ejecutados de punta a punta (LLM oráculo)."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e1_duplicates import run_e1
from eda_retail.experiments.e2_crash import run_e2
from eda_retail.experiments.e3_replan import run_e3
from eda_retail.experiments.e4_rebuild import run_e4
from eda_retail.experiments.e7_audit import run_e7
from eda_retail.experiments.report import write_report

pytestmark = pytest.mark.slow


def test_experiment_run(base_settings, tmp_path):
    """Ejecución de un experimento"""
    report = run_e1(base_settings, seed=7, incidents=6, deliveries=(2,))
    path = write_report(report, tmp_path)
    data = json.loads((path / "report.json").read_text())
    assert data["experiment"] == "E1" and data["seed"] == 7
    assert data["config"]["incidents"] == 6 and data["model"] == "oracle"
    assert len(data["prompts_version"]) == 64
    assert data["metrics"] and (path / "report.md").read_text().startswith("# E1")

def test_e2_crash_halfway(base_settings):
    """Caída a mitad del plan"""
    report = run_e2(base_settings, seed=9, incidents=6)
    m = report.metrics
    assert m["consumer_crashes"] == 1
    assert m["closed"] == 6 and m["open"] == 0
    assert m["repeated_steps"] == 0
    assert report.hypothesis == {"H2": True}

def test_e1_without_any_idempotency_is_reported(base_settings):
    """Sin ninguna idempotencia, el informe recoge las incidencias escaladas por eventos repetidos"""
    report = run_e1(base_settings, seed=8, incidents=4, deliveries=(3,))
    none = next(r for r in report.metrics["runs"] if r["idempotency"] == "none")
    assert none["closed"] == 4 and none["escalated"] > 0
    assert report.hypothesis == {"H1": True}


def test_e3_out_of_stock(base_settings):
    """Almacén sin existencias"""
    report = run_e3(base_settings, seed=5, incidents=4)
    m = report.metrics
    assert report.config["cause"] == "loss" and report.config["empty_warehouses"] == ["W1", "W2"]
    assert m["closed"] == 4 and m["open"] == 0
    assert m["refunded"] + m["escalated"] == 4 and m["refunded"] > 0
    assert m["replans"] >= m["refunded"]
    assert m["repeated_steps"] == 0 and m["refunds_over_limit"] == 0
    assert report.hypothesis == {"H3": True}


def test_e4_full_rebuild(base_settings):
    """Reconstrucción completa"""
    report = run_e4(base_settings, seed=6, incidents=6)
    views = report.metrics["views"]
    assert set(views) == {"incident_status", "cost_per_incident", "audit_trail", "carrier_dashboard"}
    for v in views.values():
        assert v["identical"] and v["events_applied"] > 0 and v["seconds"] >= 0
    assert report.hypothesis == {"H4": True}


def test_e7_explanation(base_settings):
    """Explicación de una incidencia"""
    report = run_e7(base_settings, seed=7, incidents=8, sample=4)
    explained = report.metrics["incidents"]
    assert len(explained) == 4 and report.metrics["decisions"] > 0
    for inc in explained:
        assert inc["decisions"]
        for d in inc["decisions"]:
            assert d["agent"] in {"planner", "replanner"}
            assert d["plan_version"] >= 1 and d["reason"]
    assert report.metrics["revised_plans"] > 0
    assert report.hypothesis == {"H7": True}
