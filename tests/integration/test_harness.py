"""experiments: arnés de un proceso con el LLM oráculo y los modos de idempotencia de E1."""

from __future__ import annotations

import pytest

from eda_retail.experiments.harness import RunConfig, run
from eda_retail.synthetic.faults import FaultPlan

pytestmark = pytest.mark.slow


def test_harness_closes_every_incident(base_settings):
    """El arnés lleva cada aviso hasta su cierre con los mismos componentes que el sistema desplegado"""
    result = run(base_settings, RunConfig(seed=5, incidents=6))
    assert result.closed == 6 and result.open == 0
    assert result.duplicates == {} and result.consumer_crashes == 0
    assert len(result.latencies_ms) == 6 and all(v > 0 for v in result.latencies_ms)



def test_e1_with_idempotency(base_settings):
    """Con idempotencia"""
    result = run(base_settings, RunConfig(seed=8, incidents=6, faults=FaultPlan(duplicates=3)))
    assert result.closed == 6
    assert result.duplicates == {}


def test_e1_without_idempotency(base_settings):
    """Sin idempotencia"""
    result = run(base_settings, RunConfig(seed=8, incidents=6, faults=FaultPlan(duplicates=3),
                                          idempotency="no_effect_keys"))
    assert result.closed == 6
    assert sum(result.duplicates.values()) > 0
    assert set(result.duplicates) <= {"customer_messages", "claims", "reservations", "shipments", "refunds"}
