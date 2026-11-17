"""Invariantes comunes de resiliencia (spec experiments): las partes que se calculan sin base de datos."""

from __future__ import annotations

from eda_retail.experiments.invariants import gate, refunds_with_pending_compensations, summary


def _requested(incident: str, step_id: str, action: str) -> dict:
    return {"correlation_id": incident, "event_type": "StepRequested", "payload": {"step_id": step_id, "action": action}}


def _completed(incident: str, step_id: str) -> dict:
    return {"correlation_id": incident, "event_type": "StepCompleted", "payload": {"step_id": step_id}}


def test_refund_waits_for_pending_compensations():
    """I3: un issue_refund pedido con una compensación pendiente de su incidencia es una violación"""
    events = [
        _requested("I-1", "S3-comp", "release_stock"),
        _requested("I-1", "S5", "issue_refund"),  # la compensación sigue pendiente
        _completed("I-1", "S3-comp"),
        _requested("I-2", "S3-comp", "release_stock"),
        _completed("I-2", "S3-comp"),
        _requested("I-2", "S5", "issue_refund"),  # ya terminó
        _requested("I-3", "S5", "issue_refund"),  # nunca tuvo compensaciones
    ]
    assert refunds_with_pending_compensations(events) == [
        {"incident_id": "I-1", "step_id": "S5", "pending": ["S3-comp"]}
    ]


def test_report_with_invariants():
    """Informe con invariantes: la hipótesis no se da por cumplida si I1 o I2 tienen alguna violación"""
    clean = {name: {"violations": 0} for name in ("I1", "I2", "I3", "I4", "I5")}
    assert gate(clean) and summary(clean) == {"I1": 0, "I2": 0, "I3": 0, "I4": 0, "I5": 0}
    assert not gate(clean | {"I1": {"violations": 1}})
    assert not gate(clean | {"I2": {"violations": 2}})
    assert gate(clean | {"I3": {"violations": 1}, "I5": {"violations": None}})
