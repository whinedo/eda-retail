"""E6 Eventos frente a síncrono, con el oráculo (cliente LLM falso)."""

from __future__ import annotations

import pytest

from eda_retail.experiments.e6_compare import run_e6

pytestmark = pytest.mark.slow


def test_comparison_without_faults(base_settings):
    """Comparación sin fallos"""
    report = run_e6(base_settings, seed=11, incidents=4, repetitions=1, scenarios=("no_faults",))
    versions = report.metrics["no_faults"]
    assert set(versions) == {"events", "sync"}
    for v in versions.values():
        assert set(v["latency_ms"]) == {"mean", "median", "p95"} and v["latency_ms"]["mean"] > 0
        assert v["tokens_in"]["mean"] > 0 and v["tokens_out"]["mean"] > 0
        assert "mean" in v["cost"] and v["resolved"]["mean"] == 4
    assert report.hypothesis == {}


def test_comparison_with_faults(base_settings):
    """Comparación con fallos"""
    report = run_e6(base_settings, seed=11, incidents=4, repetitions=2, scenarios=("faults",))
    versions = report.metrics["faults"]
    for v in versions.values():
        assert v["closed"]["mean"] == 4 and v["duplicates"] == 0
        assert {"resolved", "escalated", "refunded"} <= set(v)
    assert report.config["faults"]["duplicates"] == 2
    assert [(r["repetition"], r["version"]) for r in report.runs] == [
        (1, "events"), (1, "sync"), (2, "events"), (2, "sync")]
