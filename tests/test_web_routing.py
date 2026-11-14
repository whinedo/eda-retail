"""Emisor y destino de cada evento (spec web-front, "Origen de emisor y destino")."""

from __future__ import annotations

from eda_retail.web.routing import PROJECTORS, receivers


def test_event_with_a_single_consumer():
    """Evento con consumidor único"""
    assert receivers("PlanCreated", {}) == ["orchestrator", PROJECTORS]


def test_step_requested_goes_to_the_executor_of_its_action():
    """Paso pedido a un ejecutor"""
    assert receivers("StepRequested", {"action": "notify_customer"})[0] == "executor-notify_customer"


def test_event_with_several_consumers():
    """Evento con varios consumidores"""
    assert receivers("StepFailed", {}) == ["orchestrator", "replanner", PROJECTORS]


def test_event_without_kafka_consumer():
    """Evento sin consumidor de Kafka"""
    assert receivers("LlmCallRecorded", {}) == [PROJECTORS]


def test_agents_receive_the_event_they_declare():
    """Cada agente recibe el tipo de evento que declara en agents.cli.AGENTS"""
    assert receivers("CarrierNoticeReceived", {})[0] == "interpreter"
    assert receivers("ShipmentDelayed", {})[0] == "planner"
