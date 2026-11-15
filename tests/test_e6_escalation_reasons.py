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
