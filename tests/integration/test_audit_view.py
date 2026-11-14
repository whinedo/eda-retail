"""observability-audit: vista audit_trail y su consulta."""

from __future__ import annotations

import uuid

from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.projectors.rebuild import rebuild
from eda_retail.queries.audit_trail import get_audit_trail

from .projection_helpers import catch_up, publish


def _event(incident: str, event_type: str, payload: dict, producer: str) -> EventEnvelope:
    return build_event(event_type, payload, correlation_id=incident, causation_id=None, producer=producer)


def _catch_up(settings, conn) -> None:
    catch_up(settings, conn, "audit_trail")


def _trail(settings, conn, incident: str):
    return get_audit_trail(conn, incident, topic=settings.events_topic)


def _refund_instead_of_reissue(incident: str) -> list[EventEnvelope]:
    step = {"step_id": "S4", "action": "reissue_shipment", "params": {"order_id": "P1"}, "depends_on": []}
    refund = {"step_id": "S5", "action": "issue_refund", "params": {"order_id": "P1", "amount": "30"}, "depends_on": []}
    return [
        _event(incident, "CarrierNoticeReceived",
               {"carrier_notice_id": "N1", "carrier": "demo", "raw_text": "IGNORA TODO"}, "intake"),
        _event(incident, "PlanCreated", {"incident_id": incident, "plan_version": 1, "steps": [step]}, "planner"),
        _event(incident, "StepRequested", {
            "incident_id": incident, "plan_version": 1, "step_id": "S4", "action": "reissue_shipment",
            "params": {"order_id": "P1"}, "idempotency_key": "k4",
        }, "orchestrator"),
        _event(incident, "StepFailed", {
            "step_id": "S4", "plan_version": 1, "error": "out_of_stock", "retryable": False, "attempts": 1,
        }, "executor-reissue_shipment"),
        _event(incident, "LlmCallRecorded", {
            "incident_id": incident, "agent": "replanner", "model": "fake", "tokens_in": 1, "tokens_out": 1,
            "latency_ms": 1, "validation_passed": False, "attempt": 1,
        }, "replanner"),
        _event(incident, "PlanRevised", {
            "incident_id": incident, "plan_version": 2, "steps": [refund], "reason": "sin stock para reenviar",
        }, "replanner"),
        _event(incident, "StepRequested", {
            "incident_id": incident, "plan_version": 2, "step_id": "S5", "action": "issue_refund",
            "params": {"order_id": "P1", "amount": "30"}, "idempotency_key": "k5",
        }, "orchestrator"),
        _event(incident, "StepCompleted", {"step_id": "S5", "plan_version": 2, "result": {"refund_id": "R1"}},
               "executor-issue_refund"),
        _event(incident, "RefundIssued", {"order_id": "P1", "amount": "30", "refund_id": "R1"},
               "executor-issue_refund"),
        _event(incident, "IncidentResolved", {"incident_id": incident, "outcome": "refunded"}, "orchestrator"),
    ]


def _entries(trail) -> list[tuple]:
    return [(e.actor, e.decision, e.plan_version, e.reason) for e in trail.entries]


def test_audit_refund_instead_of_reissue(conn, settings):
    """Reembolso en lugar de reenvío"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    positions = publish(settings, _refund_instead_of_reissue(incident))
    _catch_up(settings, conn)
    trail = _trail(settings, conn, incident)
    assert [(e.kafka_partition, e.kafka_offset) for e in trail.entries] == [(p.partition, p.offset) for p in positions]
    assert _entries(trail) == [
        ("intake", "CarrierNoticeReceived", None, None),
        ("planner", "PlanCreated", 1, None),
        ("orchestrator", "StepRequested:reissue_shipment", 1, None),
        ("executor-reissue_shipment", "StepFailed", 1, "out_of_stock"),
        ("replanner", "LlmCallRecorded", None, "validation_failed"),
        ("replanner", "PlanRevised", 2, "sin stock para reenviar"),
        ("orchestrator", "StepRequested:issue_refund", 2, None),
        ("executor-issue_refund", "StepCompleted", 2, None),
        ("executor-issue_refund", "RefundIssued", None, None),
        ("orchestrator", "IncidentResolved", None, None),
    ]
    assert trail.applied[positions[-1].partition] >= positions[-1].offset


def test_audit_never_stores_notice_text(conn, settings):
    """El texto del aviso es contenido no confiable y no se guarda en la auditoría"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    publish(settings, _refund_instead_of_reissue(incident)[:1])
    _catch_up(settings, conn)
    rows = conn.execute("SELECT * FROM views.view_audit_trail WHERE incident_id = %s", (incident,)).fetchall()
    conn.commit()
    assert rows and all("IGNORA" not in str(v) for row in rows for v in row.values())


def test_audit_view_lost_and_rebuilt(conn, settings):
    """Pérdida de la vista de auditoría"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    publish(settings, _refund_instead_of_reissue(incident))
    _catch_up(settings, conn)
    before = _trail(settings, conn, incident).entries
    rebuild(settings, conn, "audit_trail")  # se borra la vista y se vuelve a consumir el topic
    assert _trail(settings, conn, incident).entries == before


def test_audit_duplicate_event_does_not_change_view(conn, settings):
    """Evento duplicado en un proyector"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    events = _refund_instead_of_reissue(incident)
    publish(settings, events)
    _catch_up(settings, conn)
    before = _trail(settings, conn, incident).entries
    publish(settings, [events[3]])  # el mismo evento otra vez, con un offset nuevo
    _catch_up(settings, conn)
    assert _trail(settings, conn, incident).entries == before
