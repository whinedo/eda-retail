"""Catálogo de eventos: esquemas de payload, datos personales y conversión de versiones."""

from __future__ import annotations

import uuid

import pytest

from eda_retail.events.catalog import CATALOG, PayloadError, PersonalDataError, build_event
from eda_retail.events.versioning import UpcastError, Upcasters

EXPECTED_TYPES = {
    "CarrierNoticeReceived", "ShipmentDelayed", "PlanCreated", "PlanRevised", "StepRequested",
    "StepCompleted", "StepFailed", "CustomerNotified", "ClaimFiled", "StockReserved",
    "StockReleased", "ShipmentReissued", "RefundIssued", "IncidentResolved", "IncidentEscalated",
}


def _step_requested(params: dict) -> dict:
    return {
        "incident_id": "I1",
        "plan_version": 1,
        "step_id": "S1",
        "action": "notify_customer",
        "params": params,
        "idempotency_key": "k",
    }


def test_catalog_has_every_event_of_readme():
    """El catálogo tiene un esquema por cada evento del catálogo de docs/README.md"""
    assert set(CATALOG) == EXPECTED_TYPES
    assert all(spec.version == 1 for spec in CATALOG.values())


def test_build_event_validates_payload_and_sets_envelope():
    """Un evento del catálogo se construye con su versión y su payload validado"""
    cause = uuid.uuid4()
    ev = build_event(
        "ShipmentDelayed",
        {"incident_id": "I1", "order_ids": ["P1"], "cause": "delay", "estimated_delay_hours": 24, "severity": "low"},
        correlation_id="I1",
        causation_id=cause,
        producer="interpreter",
    )
    assert (ev.event_version, ev.correlation_id, ev.causation_id) == (1, "I1", cause)
    assert ev.payload["order_ids"] == ["P1"]


def test_unknown_cause_is_rejected():
    """La cause pertenece al conjunto delay, loss, damage, wrong_address, strike, unknown"""
    with pytest.raises(PayloadError, match="cause"):
        build_event(
            "ShipmentDelayed",
            {"incident_id": "I1", "order_ids": ["P1"], "cause": "aliens", "estimated_delay_hours": 1, "severity": "low"},
            correlation_id="I1", causation_id=None, producer="x",
        )


def test_plan_without_required_field():
    """Plan sin campo obligatorio"""
    with pytest.raises(PayloadError, match="depends_on"):
        build_event(
            "PlanCreated",
            {"incident_id": "I1", "plan_version": 1, "steps": [{"step_id": "S1", "action": "notify_customer", "params": {}}]},
            correlation_id="I1", causation_id=None, producer="planner",
        )


def test_unknown_event_type_is_rejected():
    """Un tipo fuera del catálogo no se construye"""
    with pytest.raises(PayloadError, match="NotAnEvent"):
        build_event("NotAnEvent", {}, correlation_id="I1", causation_id=None, producer="x")


def test_customer_notice_event():
    """Evento de aviso al cliente"""
    ev = build_event("StepRequested", _step_requested({"order_id": "P1"}), correlation_id="I1", causation_id=None, producer="o")
    assert ev.payload["params"] == {"order_id": "P1"}
    with pytest.raises(PersonalDataError, match="email"):
        build_event(
            "StepRequested", _step_requested({"order_id": "P1", "email": "ana@example.com"}),
            correlation_id="I1", causation_id=None, producer="o",
        )
    with pytest.raises(PersonalDataError):
        build_event(
            "StepRequested", _step_requested({"order_id": "P1", "to": "ana@example.com"}),
            correlation_id="I1", causation_id=None, producer="o",
        )


def test_upcaster_chain_and_errors():
    """Los conversores se encadenan y fallan si falta un salto"""
    reg = Upcasters()
    reg.register("X", 1)(lambda p: {**p, "b": 2})
    reg.register("X", 2)(lambda p: {**p, "c": 3})
    ev = build_event("IncidentEscalated", {"incident_id": "I1", "reason": "r"}, correlation_id="I1", causation_id=None, producer="x")
    x = ev.model_copy(update={"event_type": "X", "payload": {"a": 1}})
    assert reg.upcast(x, 3).payload == {"a": 1, "b": 2, "c": 3}
    assert reg.upcast(x, 3).event_version == 3
    assert x.payload == {"a": 1}
    with pytest.raises(UpcastError):
        reg.upcast(x, 4)
