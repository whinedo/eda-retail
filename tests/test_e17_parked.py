"""E17 Proyección atrasada y avisos aparcados (spec experiments): veredicto de H17 por variante.

Sin Kafka ni PostgreSQL: las métricas se construyen a mano. La ejecución de punta a punta está en
tests/integration/test_e17_parked.py.
"""

from __future__ import annotations

from eda_retail.experiments.e17_parked import EXPIRED, ORDER_REGISTERED, PROJECTED, h17, variant_holds

N = 50


def _variant(**overrides) -> dict:
    base = {"parked": N, "parked_twice": 0, "still_parked": 0, "interpreter_lag_while_parked": 0,
            "resumed_by_order": 0, "resumed_by_deadline": 0, "escalated": 0, "orders_projected_while_paused": 0,
            "duplicate_reason": None}
    return base | overrides


def _metrics(**variants) -> dict:
    good = {ORDER_REGISTERED: _variant(resumed_by_order=N, duplicate_reason="duplicate_notice"),
            EXPIRED: _variant(escalated=N), PROJECTED: _variant(resumed_by_deadline=N)}
    return {"variants": good | variants}


def test_h17_holds():
    """Cumplimiento de H17"""
    assert h17(_metrics(), N)


def test_resumed_by_the_registered_order():
    """Reanudación por el pedido registrado: un ShipmentDelayed por aviso al consumir su OrderRegistered"""
    assert variant_holds(ORDER_REGISTERED, _metrics()["variants"][ORDER_REGISTERED], N)
    late = _variant(resumed_by_order=N - 1, resumed_by_deadline=1, duplicate_reason="duplicate_notice")
    assert not h17(_metrics(**{ORDER_REGISTERED: late}), N)


def test_deadline_without_projection():
    """Plazo vencido sin proyección: cada aviso escala con no_valid_orders"""
    assert not h17(_metrics(**{EXPIRED: _variant(escalated=N - 2, resumed_by_deadline=2)}), N)
    assert not h17(_metrics(**{EXPIRED: _variant(escalated=N, orders_projected_while_paused=3)}), N)


def test_order_projected_before_the_deadline():
    """Pedido proyectado antes del plazo: publica ShipmentDelayed y no escala"""
    assert not h17(_metrics(**{PROJECTED: _variant(resumed_by_deadline=N - 1, escalated=1)}), N)


def test_parked_and_repeated_notice():
    """Aviso aparcado y repetido: se descarta con duplicate_notice y no hay un segundo aparcamiento"""
    twice = _variant(resumed_by_order=N, duplicate_reason="duplicate_notice", parked_twice=1)
    assert not h17(_metrics(**{ORDER_REGISTERED: twice}), N)
    accepted = _variant(resumed_by_order=N, duplicate_reason=None)
    assert not h17(_metrics(**{ORDER_REGISTERED: accepted}), N)


def test_notice_left_parked_or_interpreter_lag_fails():
    assert not h17(_metrics(**{PROJECTED: _variant(resumed_by_deadline=N, still_parked=1)}), N)
    assert not h17(_metrics(**{EXPIRED: _variant(escalated=N, interpreter_lag_while_parked=4)}), N)


def test_expexecutor_checks_of_e17():
    from eda_retail.expexecutor.catalog import CATALOG

    ok = {"metrics": {"holds": {ORDER_REGISTERED: True, EXPIRED: True, PROJECTED: True}}}
    assert all(c.ok for c in CATALOG["e17"].checks(ok))
    bad = {"metrics": {"holds": {ORDER_REGISTERED: True, EXPIRED: False, PROJECTED: True}}}
    assert [c.metric for c in CATALOG["e17"].checks(bad) if not c.ok] == ["metrics.holds.expired"]
