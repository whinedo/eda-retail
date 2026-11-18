"""E19 Pérdida del topic (spec experiments) y Pérdida del topic (spec synthetic-data) de punta a punta con el LLM
oráculo. Una sola ejecución de E19 (las dos variantes) para todos los escenarios, con 20 incidencias y 5 avisos nuevos
tras la recuperación."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e19_topic_loss import run_e19

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e19(base_settings, seed=19, incidents=20, new_notices=5)


def _detail(report, variant: str) -> str:
    m = report.metrics["variants"][variant]
    keys = ("closed_before_loss", "open", "messages_before_loss", "republished", "messages_since_loss", "lag",
            "order", "reordered", "changes")
    return json.dumps({k: m[k] for k in keys}, default=str)


def _republished_in_order(report, variant: str) -> None:
    m = report.metrics["variants"][variant]
    assert m["closed_before_loss"] == 20 and m["messages_before_loss"] > 0, _detail(report, variant)
    assert m["lag"] == 0, _detail(report, variant)
    assert m["messages_since_loss"] == sum(m["republished"].values()) > 0, _detail(report, variant)
    assert all(o == {"not_republished": 0, "unordered": 0} for o in m["order"].values()), _detail(report, variant)
    assert m["reordered"] == 0, _detail(report, variant)
    assert m["changes"] == {}, _detail(report, variant)


def test_truncated_topic(report):
    """Topic vaciado"""
    _republished_in_order(report, "truncated")


def test_truncated_topic_keeps_offsets_growing(report):
    """Topic vaciado con offsets que siguen: el primer offset republicado de cada partición es su marca de agua alta de
    antes de la pérdida"""
    m = report.metrics["variants"]["truncated"]
    start = {int(p): o for p, o in m["start_offsets"].items()}
    first = {int(p): o for p, o in m["first_offsets"].items()}
    assert first and all(first[p] == start[p] > 0 for p in first), _detail(report, "truncated")


def test_recreated_topic(report):
    """Topic recreado"""
    _republished_in_order(report, "recreated")


def test_recreated_topic_starts_from_zero(report):
    """Topic recreado desde 0: el primer mensaje republicado de cada partición recibe el offset 0"""
    m = report.metrics["variants"]["recreated"]
    assert m["first_offsets"] and all(o == 0 for o in m["first_offsets"].values()), _detail(report, "recreated")


def test_new_notices_after_recovery(report):
    """Avisos nuevos tras la recuperación"""
    for variant, m in report.metrics["variants"].items():
        assert m["open"] == 0 and m["closed"] == 25, _detail(report, variant)
        assert m["invariants"]["I5"]["violations"] == 0, json.dumps(m["invariants"]["I5"], default=str)


def test_h19(report):
    """Cumplimiento de H19"""
    for variant, m in report.metrics["variants"].items():
        broken = {name: inv for name, inv in m["invariants"].items() if inv["violations"] != 0}
        assert broken == {}, json.dumps(broken, default=str)
        assert m["h19"], _detail(report, variant)
    assert report.hypothesis == {"H19": True}
