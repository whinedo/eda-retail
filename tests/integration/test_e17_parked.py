"""E17 Proyección atrasada y avisos aparcados (spec experiments) y parada de proyectores (spec
synthetic-data), de punta a punta con el LLM oráculo y un plazo de aparcamiento corto."""

from __future__ import annotations

import pytest

from eda_retail.experiments.e17_parked import EXPIRED, ORDER_REGISTERED, PROJECTED, run_e17

pytestmark = pytest.mark.slow

INCIDENTS = 6


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e17(base_settings, seed=17, incidents=INCIDENTS, timeout=8.0)


def _variant(report, name: str) -> dict:
    return report.metrics["variants"][name]


def test_projectors_stopped(report):
    """Proyectores parados: views.view_orders no tiene esos pedidos hasta que se reanudan los proyectores"""
    for name in (ORDER_REGISTERED, EXPIRED, PROJECTED):
        assert _variant(report, name)["orders_projected_while_paused"] == 0


def test_resumed_by_the_registered_order(report):
    """Reanudación por el pedido registrado"""
    v = _variant(report, ORDER_REGISTERED)
    assert v["parked"] == INCIDENTS and v["resumed_by_order"] == INCIDENTS and v["escalated"] == 0
    assert v["still_parked"] == 0


def test_deadline_without_projection(report):
    """Plazo vencido sin proyección"""
    v = _variant(report, EXPIRED)
    assert v["parked"] == INCIDENTS and v["escalated"] == INCIDENTS and v["still_parked"] == 0


def test_order_projected_before_the_deadline(report):
    """Pedido proyectado antes del plazo"""
    v = _variant(report, PROJECTED)
    assert v["parked"] == INCIDENTS and v["resumed_by_deadline"] == INCIDENTS and v["escalated"] == 0


def test_parked_and_repeated_notice(report):
    """Aviso aparcado y repetido"""
    v = _variant(report, ORDER_REGISTERED)
    assert v["duplicate_reason"] == "duplicate_notice" and v["parked_twice"] == 0


def test_h17(report):
    """Cumplimiento de H17"""
    assert all(v["interpreter_lag_while_parked"] == 0 for v in report.metrics["variants"].values())
    assert report.hypothesis == {"H17": True}
