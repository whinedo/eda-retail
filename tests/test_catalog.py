"""Catálogo de eventos: esquemas de payload, datos personales y conversión de versiones."""

from __future__ import annotations

import uuid

import pytest

from eda_retail.events.catalog import CATALOG, PayloadError, PersonalDataError, build_event
from eda_retail.events.envelope import new_event
from eda_retail.events.upcasters import to_current
from eda_retail.events.versioning import UpcastError, Upcasters

EXPECTED_TYPES = {
    "CarrierNoticeReceived", "ShipmentDelayed", "PlanCreated", "PlanRevised", "StepRequested",
    "StepCompleted", "StepFailed", "CustomerNotified", "ClaimFiled", "StockReserved",
    "StockReleased", "ShipmentReissued", "RefundIssued", "IncidentResolved", "IncidentEscalated",
    "LlmCallRecorded", "StepCancelled",
}
VERSION_2 = {"StepCompleted", "StepFailed"}


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
    assert {name for name, spec in CATALOG.items() if spec.version == 2} == VERSION_2
    assert all(spec.version == 1 for name, spec in CATALOG.items() if name not in VERSION_2)


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


def _v1(event_type: str, payload: dict):
    return new_event(
        event_type=event_type, event_version=1, correlation_id="I1", causation_id=None, producer="x", payload=payload
    )


def test_step_results_carry_plan_version():
    """StepCompleted y StepFailed v2 llevan plan_version; StepFailed también attempts"""
    done = build_event(
        "StepCompleted", {"step_id": "S1", "plan_version": 2, "result": {}},
        correlation_id="I1", causation_id=None, producer="x",
    )
    assert (done.event_version, done.payload["plan_version"]) == (2, 2)
    with pytest.raises(PayloadError, match="plan_version"):
        build_event("StepCompleted", {"step_id": "S1", "result": {}}, correlation_id="I1", causation_id=None, producer="x")
    with pytest.raises(PayloadError, match="attempts"):
        build_event(
            "StepFailed", {"step_id": "S1", "plan_version": 1, "error": "timeout", "retryable": True},
            correlation_id="I1", causation_id=None, producer="x",
        )


def test_v1_step_results_are_upcast():
    """Un StepCompleted o StepFailed v1 se lee como v2 de la versión 1 del plan y un intento"""
    done = to_current(_v1("StepCompleted", {"step_id": "S1", "result": {"claim_id": "C1"}}))
    assert done.event_version == 2
    assert done.payload == {"step_id": "S1", "plan_version": 1, "result": {"claim_id": "C1"}}
    failed = to_current(_v1("StepFailed", {"step_id": "S2", "error": "out_of_stock", "retryable": False}))
    assert failed.payload == {
        "step_id": "S2", "plan_version": 1, "error": "out_of_stock", "retryable": False, "attempts": 1,
    }


def test_current_events_are_not_upcast():
    """Un evento en su versión vigente, o fuera del catálogo, se lee tal cual"""
    ev = _v1("IncidentEscalated", {"incident_id": "I1", "reason": "r"})
    assert to_current(ev) is ev
    other = _v1("NotAnEvent", {"a": 1})
    assert to_current(other) is other


def test_step_cancelled_event():
    """StepCancelled lleva la incidencia, la versión del plan que lo cancela y el paso"""
    ev = build_event(
        "StepCancelled", {"incident_id": "I1", "plan_version": 2, "step_id": "S3"},
        correlation_id="I1", causation_id=None, producer="x",
    )
    assert ev.event_version == 1


def test_projector_upcasters_extend_shared_registry():
    """Los conversores propios de un proyector amplían el registro compartido, no lo sustituyen"""
    from eda_retail.projectors.base import Projector

    own = Upcasters()
    own.register("X", 1)(lambda p: p)
    seen = []
    projector = Projector("test", {"StepCompleted": lambda conn, seq, ev: seen.append(ev)}, upcasters=own)
    projector.apply(None, 1, _v1("StepCompleted", {"step_id": "S1", "result": {}}))
    assert (seen[0].event_version, seen[0].payload["plan_version"]) == (2, 1)
    assert ("StepCompleted", 1) not in own._converters  # el registro del llamador no cambia


def test_projector_upcasters_reject_shadowing_shared_converter():
    """Un proyector no puede redefinir en silencio un conversor del registro compartido"""
    from eda_retail.projectors.base import Projector

    own = Upcasters()
    own.register("StepCompleted", 1)(lambda p: p)
    with pytest.raises(ValueError, match="StepCompleted"):
        Projector("test", {}, upcasters=own)
