"""cqrs-views «Reconstrucción de vistas»: vaciar una vista y reproducir el log desde seq 1."""

from __future__ import annotations

import pytest

from eda_retail.projectors.rebuild import fingerprint, rebuild
from eda_retail.projectors.registry import PROJECTORS

from .command_helpers import open_incident
from .test_audit_view import _refund_instead_of_reissue
from .test_carrier_view import _incident as _carrier_incident
from .test_cost_view import _effects, _llm
from .test_plans import LOSS_PLAN, create
from .test_steps import record, request


def _history(conn) -> None:
    """Un log con eventos de todas las vistas."""
    from eda_retail.infra.event_store import append_events

    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN[:3])
    assert request(conn, incident, "S1").accepted
    assert record(conn, incident, "S1", result={"message_id": "M1"},
                  effect={"order_id": "P1", "message_id": "M1"}).accepted
    with conn.transaction():
        append_events(conn, [
            _llm(incident, "planner", 100, 50), *_effects(incident),
            *_refund_instead_of_reissue(f"{incident}-a"),
            *_carrier_incident("demo-rebuild", "delay", claim=True, refund=True),
        ])


@pytest.mark.parametrize("name", sorted(PROJECTORS))
def test_rebuild_gives_same_fingerprint(conn, name):
    """Reconstrucción de incident_status"""
    _history(conn)
    result = rebuild(conn, name)
    assert result.fingerprint_before == result.fingerprint_after
    assert len(result.fingerprint_after) == 64  # sha256 en hexadecimal
    assert result.seconds >= 0 and result.events_applied > 0
    assert fingerprint(conn, name) == result.fingerprint_after


def test_rebuild_changes_fingerprint_when_view_differs(conn):
    """La huella cambia si la vista cambia: detecta una vista reconstruida distinta"""
    _history(conn)
    before = fingerprint(conn, "carrier_dashboard")
    with conn.transaction():
        conn.execute("UPDATE view_carrier_dashboard SET claims_filed = claims_filed + 100 WHERE carrier = 'demo-rebuild'")
    assert fingerprint(conn, "carrier_dashboard") != before
    result = rebuild(conn, "carrier_dashboard")
    assert result.fingerprint_before != result.fingerprint_after  # la reconstrucción corrige la vista
