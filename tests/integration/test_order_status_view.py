"""cqrs-views «Estado de los pedidos» (proyección order_status) y su endpoint del front
(web-front «Vista de analítica de pedidos»)."""

from __future__ import annotations

import uuid

import pytest
from fastapi.testclient import TestClient

from eda_retail.queries.order_status import get_order_status
from eda_retail.web.app import create_app

from .projection_helpers import LOSS_PLAN, History, catch_up, publish


def _orders() -> tuple[str, str]:
    suffix = uuid.uuid4().hex[:6]
    return f"P-{suffix}-1", f"P-{suffix}-2"


def _statuses(conn, settings, incident: str) -> dict[str, str]:
    view = get_order_status(conn, incident_id=incident, topic=settings.events_topic)
    return {o.order_id: o.status for o in view.orders}


def _project(conn, settings, history: History) -> None:
    publish(settings, history.take())
    catch_up(settings, conn, "order_status")


def test_delayed_shipment_orders(conn, settings):
    """Pedido con envío retrasado"""
    p1, p2 = _orders()
    history = History()
    delayed = history.delayed([p1, p2])
    [position] = publish(settings, history.take())
    catch_up(settings, conn, "order_status")

    view = get_order_status(conn, incident_id=history.incident, topic=settings.events_topic)

    assert {o.order_id: o.status for o in view.orders} == {p1: "delayed", p2: "delayed"}
    row = view.orders[0]
    assert (row.last_event_type, row.last_event_id) == ("ShipmentDelayed", str(delayed.event_id))
    assert (row.kafka_partition, row.kafka_offset) == (position.partition, position.offset)
    assert view.applied[position.partition] >= position.offset


def test_order_in_resolution(conn, settings):
    """Pedido en proceso de solución"""
    p1, _ = _orders()
    history = History()
    history.delayed([p1])
    _project(conn, settings, history)
    history.plan_created(LOSS_PLAN)
    _project(conn, settings, history)

    assert _statuses(conn, settings, history.incident) == {p1: "in_resolution"}


@pytest.mark.parametrize("outcome", ["refunded", "resolved"])
def test_order_resolved_or_refunded(conn, settings, outcome: str):
    """Pedido solucionado o reembolsado"""
    p1, _ = _orders()
    history = History()
    history.delayed([p1])
    history.plan_created(LOSS_PLAN)
    _project(conn, settings, history)
    history.resolved(outcome)
    _project(conn, settings, history)

    view = get_order_status(conn, incident_id=history.incident, topic=settings.events_topic)
    assert [(o.order_id, o.status, o.last_event_type) for o in view.orders] == [(p1, outcome, "IncidentResolved")]


def test_order_of_escalated_incident(conn, settings):
    """Pedido de una incidencia escalada"""
    p1, _ = _orders()
    history = History()
    history.delayed([p1])
    history.plan_created(LOSS_PLAN)
    _project(conn, settings, history)
    history.escalated("sin plan válido")
    _project(conn, settings, history)
    assert _statuses(conn, settings, history.incident) == {p1: "escalated"}

    history.plan_revised(LOSS_PLAN)
    _project(conn, settings, history)
    assert _statuses(conn, settings, history.incident) == {p1: "escalated"}


def test_incident_escalated_before_its_delay_has_no_orders(conn, settings):
    """Una incidencia escalada antes de su ShipmentDelayed no tiene pedidos y no aparece"""
    history = History()
    history.escalated("aviso ilegible", producer="interpreter")
    _project(conn, settings, history)
    view = get_order_status(conn, incident_id=history.incident, topic=settings.events_topic)
    assert view.orders == []
    assert set(view.counts.values()) == {0}


def test_duplicated_event_does_not_change_the_view(conn, settings):
    """Evento duplicado en un proyector: el mismo PlanCreated publicado dos veces no cambia la vista"""
    p1, _ = _orders()
    history = History()
    history.delayed([p1])
    plan = history.plan_created(LOSS_PLAN)
    _project(conn, settings, history)
    history.resolved("refunded")
    _project(conn, settings, history)
    publish(settings, [plan])
    catch_up(settings, conn, "order_status")
    assert _statuses(conn, settings, history.incident) == {p1: "refunded"}


@pytest.fixture
def client(database_url: str) -> TestClient:
    return TestClient(create_app(database_url=database_url), base_url="http://localhost")


def test_analytics_endpoint_counts_and_filters(conn, settings, client: TestClient):
    """Filtro por estado: la tabla solo muestra los pedidos del estado pulsado y los contadores siguen todos"""
    delayed_order, _ = _orders()
    refunded_order, _ = _orders()
    first, second = History(), History()
    first.delayed([delayed_order])
    second.delayed([refunded_order])
    second.plan_created(LOSS_PLAN)
    _project(conn, settings, first)
    _project(conn, settings, second)
    second.resolved("refunded")
    _project(conn, settings, second)

    whole = client.get("/api/analytics/orders").json()
    orders = {o["order_id"]: o for o in whole["orders"]}
    assert orders[delayed_order]["status"] == "delayed"
    assert orders[refunded_order]["status"] == "refunded"
    assert set(whole["counts"]) == {"delayed", "in_resolution", "resolved", "refunded", "escalated"}
    assert whole["counts"]["delayed"] >= 1 and whole["counts"]["refunded"] >= 1
    assert {"incident_id", "last_event_type", "last_event_id", "occurred_at", "kafka_partition",
            "kafka_offset"} <= set(orders[refunded_order])
    recent = [o["occurred_at"] for o in whole["orders"]]
    assert recent == sorted(recent, reverse=True)

    only_delayed = client.get("/api/analytics/orders", params={"status": "delayed"}).json()
    assert {o["status"] for o in only_delayed["orders"]} == {"delayed"}
    assert only_delayed["counts"] == whole["counts"]

    by_incident = client.get("/api/analytics/orders", params={"incident_id": second.incident}).json()
    assert [o["order_id"] for o in by_incident["orders"]] == [refunded_order]
    assert by_incident["counts"] == {"delayed": 0, "in_resolution": 0, "resolved": 0, "refunded": 1,
                                     "escalated": 0}


def test_analytics_endpoint_rejects_unknown_status(client: TestClient):
    """Un estado que no es de order_status se rechaza con 422"""
    assert client.get("/api/analytics/orders", params={"status": "lost"}).status_code == 422
