"""cqrs-views «Avisos aparcados»: proyector `parked_notices` y su endpoint del front, sin base de datos.

Los escenarios contra PostgreSQL y Kafka reales están en tests/integration/test_parked_notices_view.py.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from fastapi.testclient import TestClient

from eda_retail.projectors import parked_notices
from eda_retail.projectors.registry import PROJECTORS
from eda_retail.queries.parked_notices import ParkedNotice, ParkedNoticesView
from eda_retail.web import app as web_app


def test_parked_notices_projector_handles_parking_and_its_resolution():
    """NoticeParked aparca; ShipmentDelayed (reanudado) e IncidentEscalated (vencido) lo quitan de la vista"""
    assert set(parked_notices.HANDLERS) == {"NoticeParked", "ShipmentDelayed", "IncidentEscalated"}
    assert PROJECTORS["parked_notices"].tables == ("views.view_parked_notices",)


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    notices = [
        ParkedNotice("I-b", ["P-1001"], datetime(2026, 11, 15, 20, 13, 36, tzinfo=UTC),
                     datetime(2026, 11, 15, 20, 8, 36, tzinfo=UTC), "e1", 0, 7),
        ParkedNotice("I-a", ["P-1004", "P-1005"], datetime(2026, 11, 15, 20, 14, 2, tzinfo=UTC),
                     datetime(2026, 11, 15, 20, 9, 2, tzinfo=UTC), "e2", 1, 3),
    ]
    calls: list[str | None] = []

    def fake(conn, *, topic=None) -> ParkedNoticesView:
        calls.append(topic)
        return ParkedNoticesView(notices, {0: 7, 1: 3})

    monkeypatch.setattr(web_app, "get_parked_notices", fake)
    app = web_app.create_app(database_url="postgresql://u:p@127.0.0.1:1/x")
    app.dependency_overrides[web_app._read_only_conn] = lambda: None
    return TestClient(app, base_url="http://localhost")


def test_parked_notices_endpoint_lists_the_notices_with_applied(client: TestClient):
    """GET /api/parked-notices devuelve los avisos aparcados de la vista, por plazo, con applied"""
    body = client.get("/api/parked-notices").json()

    assert [n["incident_id"] for n in body["notices"]] == ["I-b", "I-a"]
    assert body["notices"][1]["order_ids"] == ["P-1004", "P-1005"]
    assert body["notices"][0]["deadline_at"] == "2026-11-15T20:13:36Z"
    assert {"parked_at", "last_event_id", "kafka_partition", "kafka_offset"} <= set(body["notices"][0])
    assert body["applied"] == {"0": 7, "1": 3}
