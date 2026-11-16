"""experiments: arnés de un proceso con el LLM oráculo y los modos de idempotencia de E1."""

from __future__ import annotations

import pytest

from eda_retail.experiments.harness import BUSINESS_UNIQUES, Harness, RunConfig, run
from eda_retail.synthetic.faults import FaultPlan

pytestmark = pytest.mark.slow


def test_harness_closes_every_incident(base_settings):
    """El arnés lleva cada aviso hasta su cierre con los mismos componentes que el sistema desplegado"""
    result = run(base_settings, RunConfig(seed=5, incidents=6))
    assert result.closed == 6 and result.open == 0
    assert result.duplicates == {} and result.consumer_crashes == 0
    assert len(result.latencies_ms) == 6 and all(v > 0 for v in result.latencies_ms)



def test_e1_with_idempotency(base_settings):
    """Con idempotencia: cada evento y cada comando entregados 3 veces, 0 efectos duplicados"""
    result = run(base_settings, RunConfig(seed=8, incidents=6, faults=FaultPlan(duplicates=3)))
    assert result.closed == 6
    assert result.duplicates == {}


def _indexes_present(harness) -> list[bool]:
    return [harness.conn.execute("SELECT to_regclass(%s) IS NOT NULL AS present", (index,)).fetchone()["present"]
            for index in BUSINESS_UNIQUES]


@pytest.mark.parametrize(("mode", "present"), [("full", True), ("no_effect_keys", False), ("none", False)])
def test_e1_drops_the_business_uniques_without_idempotency(base_settings, mode, present):
    """Sin idempotencia, E1 elimina también las restricciones de unicidad de negocio (design.md §8)"""
    with Harness(base_settings, RunConfig(seed=8, incidents=1, idempotency=mode)) as harness:
        assert _indexes_present(harness) == [present] * len(BUSINESS_UNIQUES)


def test_e1_without_idempotency(base_settings):
    """Sin idempotencia"""
    result = run(base_settings, RunConfig(seed=8, incidents=6, faults=FaultPlan(duplicates=3),
                                          idempotency="no_effect_keys"))
    assert result.closed == 6
    assert sum(result.duplicates.values()) > 0
    assert set(result.duplicates) <= {"notify_customer", "file_claim", "reserve_stock", "reissue_shipment",
                                      "issue_refund"}
