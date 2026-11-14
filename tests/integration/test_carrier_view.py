"""cqrs-views: vista carrier_dashboard y su consulta."""

from __future__ import annotations

import uuid

from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.queries.carrier_dashboard import get_carrier_dashboard

from .projection_helpers import catch_up, publish


def _event(incident: str, event_type: str, payload: dict) -> EventEnvelope:
    return build_event(event_type, payload, correlation_id=incident, causation_id=None, producer="test")


def _incident(carrier: str | None, cause: str, *, claim: bool = False, refund: bool = False) -> list[EventEnvelope]:
    incident = f"I-{uuid.uuid4().hex[:8]}"
    events = []
    if carrier is not None:
        events.append(_event(incident, "CarrierNoticeReceived",
                             {"carrier_notice_id": f"N-{incident}", "carrier": carrier, "raw_text": "aviso"}))
    events.append(_event(incident, "ShipmentDelayed", {
        "incident_id": incident, "order_ids": ["P1"], "cause": cause, "estimated_delay_hours": 24, "severity": "high",
    }))
    if claim:
        events.append(_event(incident, "ClaimFiled", {"order_id": "P1", "claim_id": f"C-{incident}"}))
    if refund:
        events.append(_event(incident, "RefundIssued", {"order_id": "P1", "amount": "10", "refund_id": f"R-{incident}"}))
    return events


def _catch_up(settings, conn) -> None:
    catch_up(settings, conn, "carrier_dashboard")


def _board(settings, conn):
    return get_carrier_dashboard(conn, topic=settings.events_topic)


def _counts(board, carrier: str) -> tuple[int, int, int, int] | None:
    row = next((r for r in board.carriers if r.carrier == carrier), None)
    return None if row is None else (row.incidents, row.delays, row.claims_filed, row.refunds_issued)


def _carriers() -> tuple[str, str]:
    return f"fast-{uuid.uuid4().hex[:6]}", f"slow-{uuid.uuid4().hex[:6]}"


def test_carrier_dashboard_counts_per_carrier(conn, settings):
    """El panel cuenta incidencias, retrasos, reclamaciones y reembolsos por transportista"""
    fast, slow = _carriers()
    publish(settings, [
        *_incident(fast, "delay"),
        *_incident(fast, "loss", claim=True, refund=True),
        *_incident(slow, "delay", claim=True),
        *_incident(slow, "delay"),
        *_incident(slow, "damage", refund=True),
    ])
    _catch_up(settings, conn)
    board = _board(settings, conn)
    assert _counts(board, fast) == (2, 1, 1, 1)
    assert _counts(board, slow) == (3, 2, 1, 1)


def test_carrier_dashboard_without_notice_is_unknown(conn, settings):
    """Una incidencia sin aviso del transportista cuenta como transportista unknown"""
    _catch_up(settings, conn)
    before = _counts(_board(settings, conn), "unknown") or (0, 0, 0, 0)
    publish(settings, _incident(None, "delay", claim=True))
    _catch_up(settings, conn)
    after = _counts(_board(settings, conn), "unknown")
    assert after == (before[0], before[1] + 1, before[2] + 1, before[3])


def test_carrier_dashboard_new_projector_replays_history(conn, settings):
    """Vista de reclamaciones por transportista"""
    fast, _ = _carriers()
    publish(settings, [*_incident(fast, "loss", claim=True), *_incident(fast, "loss", claim=True)])
    # Proyector recién desplegado sobre un topic con historia: sin posición, consume desde el principio.
    _catch_up(settings, conn)
    board = _board(settings, conn)
    assert _counts(board, fast) == (2, 0, 2, 0)
    assert board.applied


def test_carrier_dashboard_duplicate_event_does_not_change_view(conn, settings):
    """Evento duplicado en un proyector"""
    fast, _ = _carriers()
    events = _incident(fast, "delay", claim=True, refund=True)
    publish(settings, events)
    _catch_up(settings, conn)
    before = _counts(_board(settings, conn), fast)
    publish(settings, events)  # los mismos eventos otra vez, con offsets nuevos
    _catch_up(settings, conn)
    assert _counts(_board(settings, conn), fast) == before == (1, 1, 1, 1)
