"""API de eventos del front (spec web-front: "Página de eventos", "Acceso de solo lectura y local")."""

from __future__ import annotations

import itertools
import uuid

import psycopg
import pytest
from fastapi.testclient import TestClient

from eda_retail.event_log.store import record
from eda_retail.events.envelope import new_event
from eda_retail.infra.positions import Position
from eda_retail.web.app import create_app
from eda_retail.web.db import read_only_connect


def _incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


_OFFSETS = itertools.count(1)


def _append(
    conn, incident_id: str, event_type: str, payload: dict, producer: str = "test", *, partition: int = 0,
    offset: int | None = None, trace_context: dict | None = None,
) -> int:
    """Lo que hace `event_log` al consumir un mensaje: lo guarda en event_log.events."""
    event = new_event(
        event_type=event_type,
        correlation_id=incident_id,
        causation_id=None,
        producer=producer,
        payload=payload,
    )
    position = Position("eda.events", partition, next(_OFFSETS) if offset is None else offset)
    with conn.transaction():
        seq = record(conn, event, position, trace_context)
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
    assert row["partition"] == 0 and isinstance(row["offset"], int)
    assert row["causation_id"] is None
    assert row["payload"]["incident_id"] == incident
    assert row["occurred_at"]



def test_published_event_shows_partition_and_offset(conn, client: TestClient):
    """Partición y offset de un evento publicado: los que event_log guardó al consumirlo"""
    incident = _incident()
    _append(conn, incident, "ShipmentDelayed", {"incident_id": incident}, partition=2, offset=1290)

    [row] = client.get("/api/events", params={"incident_id": incident}).json()["events"]

    assert (row["partition"], row["offset"]) == (2, 1290)


def test_event_not_yet_consumed_is_not_shown(conn, client: TestClient):
    """Evento aún no consumido: no aparece hasta que event_log lo guarda"""
    incident = _incident()
    assert client.get("/api/events", params={"incident_id": incident}).json()["events"] == []

    seq = _append(conn, incident, "ShipmentDelayed", {"incident_id": incident})

    assert [e["seq"] for e in client.get("/api/events", params={"incident_id": incident}).json()["events"]] == [seq]


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
        conn.execute("INSERT INTO views.applied_events (projector, event_id) VALUES ('web', gen_random_uuid())")


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


def _llm_call(incident_id: str, agent: str, tokens_in: int, tokens_out: int, latency_ms: int, passed: bool) -> dict:
    return {
        "incident_id": incident_id,
        "agent": agent,
        "model": "fake",
        "tokens_in": tokens_in,
        "tokens_out": tokens_out,
        "latency_ms": latency_ms,
        "validation_passed": passed,
        "attempt": 1,
        "prompt_version": "1",
        "prompt_hash": None,
    }


def test_llm_agent_metrics(conn, database_url: str):
    """Métricas de un agente con LLM"""
    agent = f"planner-{uuid.uuid4().hex[:6]}"  # agente propio: la base de tests es compartida
    incident = _incident()
    _append(conn, incident, "LlmCallRecorded", _llm_call(incident, agent, 100, 20, 1000, True), producer=agent)
    _append(conn, incident, "LlmCallRecorded", _llm_call(incident, agent, 300, 40, 3000, False), producer=agent)

    from eda_retail.web.nodes import llm_metrics

    with read_only_connect(database_url) as ro:
        metrics = llm_metrics(ro, agent)

    assert metrics == {
        "calls": 2, "tokens_in": 400, "tokens_out": 60, "avg_latency_ms": 2000, "validations_passed": 1,
    }


def test_node_detail(conn, database_url: str):
    """Detalle de un nodo: descripción, tipos que consume y emite, y métricas del agente"""
    incident = _incident()
    _append(conn, incident, "ShipmentDelayed", {"incident_id": incident}, producer="interpreter")
    client = TestClient(create_app(database_url=database_url), base_url="http://localhost")

    body = client.get("/api/nodes/interpreter").json()

    assert body["id"] == "interpreter" and body["kind"] == "llm" and body["description"]
    assert body["consumes"] == ["CarrierNoticeReceived", "OrderRegistered"]  # reanuda avisos aparcados (T5)
    assert "ShipmentDelayed" in body["emits"]
    assert body["llm"]["calls"] >= 0
    assert isinstance(body["up"], bool)


def test_event_trace_id_in_page_and_stream(conn, client: TestClient, database_url: str):
    """Ver la traza de un evento: la página y el stream llevan el trace_id de event_log.events.trace_context"""
    incident = _incident()
    seq = _append(
        conn, incident, "ShipmentDelayed", {"incident_id": incident},
        trace_context={"traceparent": "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"},
    )

    [row] = client.get("/api/events", params={"incident_id": incident}).json()["events"]
    stream = TestClient(create_app(database_url=database_url, stream_max_polls=1), base_url="http://localhost")
    text = stream.get("/api/events/stream", params={"after_seq": seq - 1}).text

    assert row["trace_id"] == "4bf92f3577b34da6a3ce929d0e0e4736"
    assert '"trace_id": "4bf92f3577b34da6a3ce929d0e0e4736"' in text


def test_event_without_trace_context(conn, client: TestClient):
    """Evento sin traza: trace_id es null"""
    incident = _incident()
    _append(conn, incident, "ShipmentDelayed", {"incident_id": incident}, trace_context=None)

    [row] = client.get("/api/events", params={"incident_id": incident}).json()["events"]

    assert row["trace_id"] is None
