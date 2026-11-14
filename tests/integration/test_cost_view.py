"""observability-audit / cqrs-views: vista cost_per_incident y su consulta."""

from __future__ import annotations

import uuid
from dataclasses import replace
from decimal import Decimal

from eda_retail.config import Settings
from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.queries.cost_per_incident import Prices, get_incident_cost

from .projection_helpers import catch_up, publish

PRICES = Prices(
    llm_in_per_1k=Decimal("0.5"), llm_out_per_1k=Decimal("1.5"),
    claim=Decimal(4), reissue=Decimal(7),
)


def _event(incident: str, event_type: str, payload: dict) -> EventEnvelope:
    return build_event(event_type, payload, correlation_id=incident, causation_id=None, producer="test")


def _llm(incident: str, agent: str, tokens_in: int, tokens_out: int, passed: bool = True) -> EventEnvelope:
    return _event(incident, "LlmCallRecorded", {
        "incident_id": incident, "agent": agent, "model": "fake", "tokens_in": tokens_in,
        "tokens_out": tokens_out, "latency_ms": 10, "validation_passed": passed, "attempt": 1,
    })


def _effects(incident: str) -> list[EventEnvelope]:
    return [
        _event(incident, "ClaimFiled", {"order_id": "P1", "claim_id": "C1"}),
        _event(incident, "ShipmentReissued", {"order_id": "P1", "new_shipment_id": "SH1"}),
        _event(incident, "RefundIssued", {"order_id": "P1", "amount": "25.50", "refund_id": "R1"}),
    ]


def _catch_up(settings, conn) -> None:
    catch_up(settings, conn, "cost_per_incident")


def _cost(settings, conn, incident: str):
    return get_incident_cost(conn, incident, PRICES, topic=settings.events_topic)


def _incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


def test_cost_with_plan_revision(conn, settings):
    """Incidencia con una revisión del plan"""
    incident = _incident()
    publish(settings, [
        _llm(incident, "interpreter", 300, 40),
        _llm(incident, "planner", 500, 200),
        _llm(incident, "replanner", 600, 100, passed=False),  # rechazada: también cuenta
        _llm(incident, "replanner", 600, 160),
        *_effects(incident),
    ])
    _catch_up(settings, conn)
    cost = _cost(settings, conn, incident)
    assert (cost.llm_calls, cost.tokens_in, cost.tokens_out) == (4, 2000, 500)
    assert (cost.claims, cost.reissues, cost.refunds_amount) == (1, 1, Decimal("25.50"))
    assert cost.llm_cost == Decimal(2000) / 1000 * Decimal("0.5") + Decimal(500) / 1000 * Decimal("1.5")
    assert cost.effects_cost == Decimal(4) + Decimal(7) + Decimal("25.50")


def test_cost_duplicate_event_does_not_change_view(conn, settings):
    """Evento duplicado en un proyector"""
    incident = _incident()
    events = [_llm(incident, "planner", 500, 200), *_effects(incident)]
    publish(settings, events)
    _catch_up(settings, conn)
    before = _cost(settings, conn, incident)
    publish(settings, events)  # los mismos eventos otra vez, con offsets nuevos
    _catch_up(settings, conn)
    after = _cost(settings, conn, incident)
    assert after.applied != before.applied  # la posición avanza con los mensajes reenviados
    assert replace(after, applied=before.applied) == before  # la vista no cambia


def test_cost_read_right_after_write(conn, settings):
    """Lectura justo después de escribir"""
    incident = _incident()
    publish(settings, [_llm(incident, "interpreter", 10, 5)])
    _catch_up(settings, conn)
    [e1] = publish(settings, [_llm(incident, "planner", 10, 5)])
    cost = _cost(settings, conn, incident)
    assert cost.applied.get(e1.partition, -1) < e1.offset
    assert cost.llm_calls == 1  # todavía sin el evento nuevo


def test_cost_default_prices_are_zero(monkeypatch):
    """Con la configuración por defecto (Ollama local) los precios son cero"""
    for name in ("LLM_PRICE_IN_PER_1K", "LLM_PRICE_OUT_PER_1K", "EFFECT_COST_CLAIM", "EFFECT_COST_REISSUE"):
        monkeypatch.delenv(name, raising=False)
    assert Prices.from_settings(Settings()) == Prices(Decimal(0), Decimal(0), Decimal(0), Decimal(0))


def test_cost_prices_from_environment(monkeypatch):
    """Los precios se leen de las variables de entorno"""
    monkeypatch.setenv("LLM_PRICE_IN_PER_1K", "0.5")
    monkeypatch.setenv("LLM_PRICE_OUT_PER_1K", "1.5")
    monkeypatch.setenv("EFFECT_COST_CLAIM", "4")
    monkeypatch.setenv("EFFECT_COST_REISSUE", "7")
    assert Prices.from_settings(Settings()) == PRICES
