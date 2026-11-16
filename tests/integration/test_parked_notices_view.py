"""cqrs-views «Avisos aparcados» (proyección parked_notices) y su endpoint del front
(web-front «Aviso de avisos aparcados»)."""

from __future__ import annotations

import uuid
from datetime import datetime

import pytest
from fastapi.testclient import TestClient

from eda_retail.queries.parked_notices import get_parked_notices
from eda_retail.web.app import create_app

from .projection_helpers import History, catch_up, publish


def _order() -> str:
    return f"P-{uuid.uuid4().hex[:6]}"


def _parked(conn, settings, incident: str) -> list:
    return [n for n in get_parked_notices(conn, topic=settings.events_topic).notices if n.incident_id == incident]


def _project(conn, settings, history: History) -> None:
    publish(settings, history.take())
    catch_up(settings, conn, "parked_notices")


def test_parked_notice_is_visible(conn, settings):
    """Aviso aparcado visible"""
    order = _order()
    history = History()
    parked = history.parked([order], "2026-11-15T20:13:36+00:00")
    [position] = publish(settings, history.take())
    catch_up(settings, conn, "parked_notices")

    view = get_parked_notices(conn, topic=settings.events_topic)

    [notice] = [n for n in view.notices if n.incident_id == history.incident]
    assert notice.order_ids == [order]
    assert notice.deadline_at == datetime.fromisoformat("2026-11-15T20:13:36+00:00")
    assert notice.last_event_id == str(parked.event_id)
    assert (notice.kafka_partition, notice.kafka_offset) == (position.partition, position.offset)
    assert view.applied[position.partition] >= position.offset


def test_parked_notice_resumed(conn, settings):
    """Aviso aparcado reanudado"""
    order = _order()
    history = History()
    history.parked([order])
    _project(conn, settings, history)
    assert len(_parked(conn, settings, history.incident)) == 1

    history.delayed([order])
    _project(conn, settings, history)

    assert _parked(conn, settings, history.incident) == []


def test_parked_notice_expired(conn, settings):
    """Aviso aparcado vencido"""
    history = History()
    history.parked([_order()])
    _project(conn, settings, history)

    history.escalated("no_valid_orders", producer="interpreter")
    _project(conn, settings, history)

    assert _parked(conn, settings, history.incident) == []


def test_duplicated_notice_parked_does_not_bring_it_back(conn, settings):
    """Evento duplicado en un proyector: el mismo NoticeParked publicado otra vez no vuelve a aparcar el aviso"""
    history = History()
    parked = history.parked([_order()])
    history.escalated("no_valid_orders", producer="interpreter")
    _project(conn, settings, history)

    publish(settings, [parked])
    catch_up(settings, conn, "parked_notices")

    assert _parked(conn, settings, history.incident) == []


@pytest.fixture
def client(database_url: str) -> TestClient:
    return TestClient(create_app(database_url=database_url), base_url="http://localhost")


def test_parked_notices_endpoint_orders_by_deadline(conn, settings, client: TestClient):
    """GET /api/parked-notices lee parked_notices: los avisos aparcados, el que vence antes primero, con applied"""
    later, sooner = History(), History()
    later.parked([_order()], "2099-01-01T00:00:10+00:00")
    sooner.parked([_order()], "2099-01-01T00:00:05+00:00")
    _project(conn, settings, later)
    _project(conn, settings, sooner)

    body = client.get("/api/parked-notices").json()

    incidents = [n["incident_id"] for n in body["notices"]]
    assert incidents.index(sooner.incident) < incidents.index(later.incident)
    deadlines = [n["deadline_at"] for n in body["notices"]]
    assert deadlines == sorted(deadlines, key=datetime.fromisoformat)
    assert {"order_ids", "deadline_at", "parked_at", "last_event_id", "kafka_partition",
            "kafka_offset"} <= set(body["notices"][0])
    assert "applied" in body
