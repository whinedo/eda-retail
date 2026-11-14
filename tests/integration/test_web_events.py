"""API de eventos del front (spec web-front: "Página de eventos", "Acceso de solo lectura y local")."""

from __future__ import annotations

import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient

from eda_retail.events.envelope import new_event
from eda_retail.infra.event_store import append_events
from eda_retail.web.app import create_app
from eda_retail.web.db import read_only_connect


def _incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


def _append(conn, incident_id: str, event_type: str, payload: dict, producer: str = "test") -> int:
    event = new_event(
        event_type=event_type,
        correlation_id=incident_id,
        causation_id=None,
        producer=producer,
        payload=payload,
    )
    with conn.transaction():
        [seq] = append_events(conn, [event])
    return seq


def _step_requested(incident_id: str, action: str) -> dict:
    return {
        "incident_id": incident_id,
        "plan_version": 1,
        "step_id": f"s-{action}",
        "action": action,
        "params": {},
        "idempotency_key": uuid.uuid4().hex,
    }


@pytest.fixture
def client(database_url: str) -> TestClient:
    return TestClient(create_app(database_url=database_url), base_url="http://localhost")


def test_page_backwards(conn, client: TestClient):
    """Paginar hacia atrás"""
    incident = _incident()
    seqs = [_append(conn, incident, "ShipmentDelayed", {"incident_id": incident}) for _ in range(5)]

    first = client.get("/api/events", params={"incident_id": incident, "limit": 2}).json()
    second = client.get(
        "/api/events", params={"incident_id": incident, "limit": 2, "before_seq": first["next_before_seq"]}
    ).json()
    third = client.get(
        "/api/events", params={"incident_id": incident, "limit": 2, "before_seq": second["next_before_seq"]}
    ).json()

    got = [e["seq"] for page in (first, second, third) for e in page["events"]]
    assert got == sorted(seqs, reverse=True)
    assert third["next_before_seq"] is None


def test_event_row_has_sender_receiver_and_columns(conn, client: TestClient):
    """Cada fila trae emisor, destino y las columnas del panel"""
    incident = _incident()
    seq = _append(
        conn,
        incident,
        "PlanCreated",
        {
            "incident_id": incident,
            "plan_version": 1,
            "steps": [{"step_id": "s1", "action": "notify_customer", "params": {}, "depends_on": []}],
        },
        producer="planner",
    )

    [row] = client.get("/api/events", params={"incident_id": incident}).json()["events"]

    assert row["seq"] == seq
    assert row["event_type"] == "PlanCreated"
    assert row["sender"] == "planner"
    assert row["receiver"] == "orchestrator"
    assert row["receivers"] == ["orchestrator", "projectors"]
    assert row["incident_id"] == incident
    assert row["plan_version"] == 1
    assert row["partition"] is None and row["offset"] is None
    assert row["causation_id"] is None
    assert row["payload"]["incident_id"] == incident
    assert row["occurred_at"]


def test_filter_by_node(conn, client: TestClient):
    """Filtrar por nodo"""
    incident = _incident()
    notify = _append(conn, incident, "StepRequested", _step_requested(incident, "notify_customer"))
    _append(conn, incident, "StepRequested", _step_requested(incident, "file_claim"))

    events = client.get(
        "/api/events", params={"incident_id": incident, "node": "executor-notify_customer"}
    ).json()["events"]

    assert [e["seq"] for e in events] == [notify]


def test_filter_by_node_includes_what_it_emits(conn, client: TestClient):
    """El filtro por nodo incluye los eventos que ese nodo emite"""
    incident = _incident()
    emitted = _append(conn, incident, "ShipmentDelayed", {"incident_id": incident}, producer="interpreter")

    events = client.get("/api/events", params={"incident_id": incident, "node": "interpreter"}).json()["events"]

    assert [e["seq"] for e in events] == [emitted]


@pytest.mark.parametrize("limit", [0, 501])
def test_limit_out_of_range(client: TestClient, limit: int):
    """Límite fuera de rango"""
    assert client.get("/api/events", params={"limit": limit}).status_code == 422


def test_direct_write_is_rejected(database_url: str):
    """Escritura directa rechazada: la conexión del front no puede escribir"""
    with read_only_connect(database_url) as conn, pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
        conn.execute("INSERT INTO inbox (consumer, event_id) VALUES ('web', gen_random_uuid())")


def _stream(database_url: str, **kwargs) -> list[int]:
    headers = kwargs.pop("headers", {})
    client = TestClient(create_app(database_url=database_url, stream_max_polls=1), base_url="http://localhost")
    response = client.get("/api/events/stream", params=kwargs, headers=headers)
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/event-stream")
    return [int(line.removeprefix("id: ")) for line in response.text.splitlines() if line.startswith("id: ")]


def test_stream_reconnection(conn, database_url: str):
    """Reconexión: con Last-Event-ID recibe desde el seq siguiente"""
    incident = _incident()
    seqs = [_append(conn, incident, "ShipmentDelayed", {"incident_id": incident}) for _ in range(3)]

    assert _stream(database_url, headers={"Last-Event-ID": str(seqs[0])}) == seqs[1:]


def test_stream_first_connection_after_page(conn, database_url: str):
    """Primera conexión tras cargar el panel: after_seq"""
    incident = _incident()
    seqs = [_append(conn, incident, "ShipmentDelayed", {"incident_id": incident}) for _ in range(3)]

    assert _stream(database_url, after_seq=seqs[1]) == seqs[2:]


def test_stream_without_position_skips_existing_events(conn, database_url: str):
    """Conexión sin posición: no reenvía los eventos que ya estaban en el log"""
    incident = _incident()
    _append(conn, incident, "ShipmentDelayed", {"incident_id": incident})

    assert _stream(database_url) == []
