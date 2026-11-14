"""cqrs-views «Reconstrucción de vistas»: vaciar una vista y volver a consumir el topic desde el principio."""

from __future__ import annotations

import pytest

from eda_retail.projectors.rebuild import fingerprint, rebuild
from eda_retail.projectors.registry import PROJECTORS

from .command_helpers import open_incident
from .projection_helpers import catch_up, publish, publish_log
from .test_audit_view import _refund_instead_of_reissue
from .test_carrier_view import _incident as _carrier_incident
from .test_cost_view import _effects, _llm
from .test_orders_view import _order
from .test_plans import LOSS_PLAN, create
from .test_steps import record, request


def _only_this_topic(conn, name: str) -> None:
    """La base de tests es compartida: la vista empieza vacía para que solo refleje el topic del test."""
    with conn.transaction():
        for table in PROJECTORS[name].tables:
            conn.execute(f"DELETE FROM {table}")


def _history(settings, conn) -> None:
    """Un topic con eventos de todas las vistas."""
    incident, _ = open_incident(conn)
    create(conn, incident, LOSS_PLAN[:3])
    assert request(conn, incident, "S1").accepted
    assert record(conn, incident, "S1", result={"message_id": "M1"},
                  effect={"order_id": "P1", "message_id": "M1"}).accepted
    publish_log(settings, conn, incident)
    publish(settings, [
        _order("P-rebuild-1"), _order("P-rebuild-2"),
        _llm(incident, "planner", 100, 50), *_effects(incident),
        *_refund_instead_of_reissue(f"{incident}-a"),
        *_carrier_incident("demo-rebuild", "delay", claim=True, refund=True),
    ])


@pytest.mark.parametrize("name", sorted(PROJECTORS))
def test_rebuild_gives_same_fingerprint(conn, settings, name):
    """Reconstrucción de incident_status"""
    _only_this_topic(conn, name)
    _history(settings, conn)
    result = rebuild(settings, conn, name)
    assert result.fingerprint_before == result.fingerprint_after
    assert len(result.fingerprint_after) == 64  # sha256 en hexadecimal
    assert result.seconds >= 0 and result.events_applied > 0
    assert fingerprint(conn, name) == result.fingerprint_after


def test_rebuild_changes_fingerprint_when_view_differs(conn, settings):
    """La huella cambia si la vista cambia: detecta una vista reconstruida distinta"""
    _only_this_topic(conn, "carrier_dashboard")
    _history(settings, conn)
    catch_up(settings, conn, "carrier_dashboard")
    before = fingerprint(conn, "carrier_dashboard")
    with conn.transaction():
        conn.execute(
            "UPDATE views.view_carrier_dashboard SET claims_filed = claims_filed + 100 WHERE carrier = 'demo-rebuild'"
        )
    assert fingerprint(conn, "carrier_dashboard") != before
    result = rebuild(settings, conn, "carrier_dashboard")
    assert result.fingerprint_before != result.fingerprint_after  # la reconstrucción corrige la vista
    assert result.fingerprint_after == before
