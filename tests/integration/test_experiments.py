"""experiments: informe reproducible, E1 y E2 ejecutados de punta a punta (LLM oráculo)."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e1_duplicates import run_e1
from eda_retail.experiments.e2_crash import run_e2
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
