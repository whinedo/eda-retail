"""E6: the report counts escalations by reason, per run and per version."""

from __future__ import annotations

from decimal import Decimal
from types import SimpleNamespace

from eda_retail.experiments.e6_compare import _aggregate, _row

PRICES = SimpleNamespace(llm_price_in_per_1k=Decimal(0), llm_price_out_per_1k=Decimal(0))


def _result(reasons: dict[str, int]) -> SimpleNamespace:
    return SimpleNamespace(closed=3, resolved=3 - sum(reasons.values()), escalated=sum(reasons.values()),
                           duplicates={}, latencies_ms=[1.0], llm_calls=1, tokens_in=1, tokens_out=1,
                           elapsed_seconds=1.0, escalation_reasons=reasons)


def test_run_row_keeps_the_escalation_reasons():
    """Comparación sin fallos: cada ejecución guarda sus escaladas por motivo"""
    row = _row("events", _result({"max_revisions": 2}), 0, PRICES)
    assert row["escalation_reasons"] == {"max_revisions": 2}


def test_aggregate_adds_the_reasons_of_every_run():
    """Comparación sin fallos: el informe suma las escaladas por motivo de cada versión"""
    rows = [_row("events", _result(r), 0, PRICES) for r in ({"max_revisions": 2}, {"max_revisions": 1, "budget": 1}, {})]
    assert _aggregate(rows)["escalation_reasons"] == {"budget": 1, "max_revisions": 3}


def test_e6_run_config_gives_the_real_llm_time_for_every_incident():
    """E6 con el LLM real: el tope total del harness crece con las incidencias (300 s cortaba 10 incidencias)"""
    from eda_retail.experiments.e6_compare import run_config

    assert run_config("no_faults", 42, 10).timeout_seconds == 1200
    assert run_config("faults", 42, 50).timeout_seconds == 6000
    assert run_config("faults", 42, 1).timeout_seconds == 300  # nunca por debajo del valor por defecto
    assert run_config("faults", 42, 10).faults.duplicates == 2
