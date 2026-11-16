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
    assert receivers("StepFailed", {}) == ["orchestrator", PROJECTORS]


def test_event_without_kafka_consumer():
    """Evento sin consumidor de Kafka"""
    assert receivers("LlmCallRecorded", {}) == [PROJECTORS]


def test_agents_receive_the_event_they_declare():
    """Cada componente con LLM recibe los tipos de evento que consume de Kafka"""
    assert receivers("CarrierNoticeReceived", {})[0] == "interpreter"


def test_step_failed_does_not_go_to_the_replanner():
    """La saga invoca RevisePlan por REST (T8): el replanificador no consume StepFailed"""
    assert "replanner" not in receivers("StepFailed", {})


def test_shipment_delayed_goes_to_the_saga():
    """La saga consume ShipmentDelayed e invoca CreatePlan por REST: el planificador no lo consume"""
    assert receivers("ShipmentDelayed", {}) == ["orchestrator", PROJECTORS]
    assert receivers("IncidentEscalated", {})[0] == "orchestrator"


def test_order_registered_goes_to_the_interpreter():
    """El intérprete reanuda los avisos aparcados al consumir OrderRegistered (T5)"""
    assert receivers("OrderRegistered", {}) == ["interpreter", PROJECTORS]
