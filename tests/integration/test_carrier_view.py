"""cqrs-views: vista carrier_dashboard y su consulta."""

from __future__ import annotations

import uuid
from datetime import timedelta

from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.event_store import append_events
from eda_retail.projectors.carrier_dashboard import PROJECTOR_NAME, build_projector
from eda_retail.queries.carrier_dashboard import get_carrier_dashboard


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


def _catch_up(conn) -> None:
    # Sin espera en los huecos: la base de tests es compartida y otros tests dejan rollbacks.
    projector = build_projector(gap_timeout=timedelta(0))
    while projector.run_once(conn):
        pass


def _counts(board, carrier: str) -> tuple[int, int, int, int] | None:
    row = next((r for r in board.carriers if r.carrier == carrier), None)
    return None if row is None else (row.incidents, row.delays, row.claims_filed, row.refunds_issued)


def _carriers() -> tuple[str, str]:
    return f"fast-{uuid.uuid4().hex[:6]}", f"slow-{uuid.uuid4().hex[:6]}"


def test_carrier_dashboard_counts_per_carrier(conn):
    """El panel cuenta incidencias, retrasos, reclamaciones y reembolsos por transportista"""
    fast, slow = _carriers()
    with conn.transaction():
        append_events(conn, [
            *_incident(fast, "delay"),
            *_incident(fast, "loss", claim=True, refund=True),
            *_incident(slow, "delay", claim=True),
            *_incident(slow, "delay"),
            *_incident(slow, "damage", refund=True),
        ])
    _catch_up(conn)
    board = get_carrier_dashboard(conn)
    assert _counts(board, fast) == (2, 1, 1, 1)
    assert _counts(board, slow) == (3, 2, 1, 1)


def test_carrier_dashboard_without_notice_is_unknown(conn):
    """Una incidencia sin aviso del transportista cuenta como transportista unknown"""
    _catch_up(conn)
    before = _counts(get_carrier_dashboard(conn), "unknown") or (0, 0, 0, 0)
    with conn.transaction():
        append_events(conn, _incident(None, "delay", claim=True))
    _catch_up(conn)
    after = _counts(get_carrier_dashboard(conn), "unknown")
    assert after == (before[0], before[1] + 1, before[2] + 1, before[3])


def test_carrier_dashboard_new_projector_replays_history(conn):
    """Vista de reclamaciones por transportista"""
    fast, _ = _carriers()
    with conn.transaction():
        append_events(conn, [*_incident(fast, "loss", claim=True), *_incident(fast, "loss", claim=True)])
    with conn.transaction():  # proyector recién desplegado: sin posición ni filas
        conn.execute("DELETE FROM projector_offsets WHERE projector = %s", (PROJECTOR_NAME,))
        conn.execute("DELETE FROM view_carrier_dashboard")
        conn.execute("DELETE FROM view_carrier_incidents")
    _catch_up(conn)
    board = get_carrier_dashboard(conn)
    assert _counts(board, fast) == (2, 0, 2, 0)
    assert board.last_applied_seq > 0


def test_carrier_dashboard_duplicate_event_does_not_change_view(conn):
    """Evento duplicado en un proyector"""
    fast, _ = _carriers()
    events = _incident(fast, "delay", claim=True, refund=True)
    with conn.transaction():
        seqs = append_events(conn, events)
    _catch_up(conn)
    before = _counts(get_carrier_dashboard(conn), fast)
    projector = build_projector()
    with conn.transaction():
        for seq, event in zip(seqs, events, strict=True):
            projector.apply(conn, seq, event)
    assert _counts(get_carrier_dashboard(conn), fast) == before == (1, 1, 1, 1)
