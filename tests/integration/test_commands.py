"""Comandos REST idempotentes contra PostgreSQL real (spec event-backbone "Invocación de comandos")."""

from __future__ import annotations

import threading
import uuid

from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from eda_retail.events.envelope import new_event
from eda_retail.infra.commands import Accepted, CommandSpec, Rejected, command_router
from eda_retail.infra.db import connect


class Payload(BaseModel):
    incident_id: str


def _handle(conn, command, payload, prepared):
    if payload.incident_id == "closed":
        return Rejected("incident_closed")
    return Accepted(events=[new_event(
        event_type="PlanCreated", correlation_id=command.correlation_id, causation_id=command.command_id,
        producer="planner", payload={"incident_id": payload.incident_id},
    )])


def _client(database_url, prepare=None) -> TestClient:
    app = FastAPI()
    spec = CommandSpec(payload_model=Payload, handle=_handle, prepare=prepare)
    app.include_router(command_router("planner", {"CreatePlan": spec}, lambda: connect(database_url)))
    return TestClient(app)


def _body(incident: str, command_id: uuid.UUID | None = None) -> dict:
    return {
        "command_id": str(command_id or uuid.uuid4()),
        "correlation_id": incident,
        "causation_id": str(uuid.uuid4()),
        "payload": {"incident_id": incident},
    }


def _events(conn, incident: str) -> int:
    return conn.execute(
        "SELECT count(*) AS n FROM planner.outbox WHERE correlation_id = %s", (incident,)
    ).fetchone()["n"]


def test_command_invoked_twice_writes_once(database_url, conn):
    """Comando invocado dos veces: la misma respuesta y un solo evento"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    client = _client(database_url)
    body = _body(incident)

    first = client.post("/commands/CreatePlan", json=body)
    second = client.post("/commands/CreatePlan", json=body)

    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    assert second.headers["Idempotent-Replayed"] == "true"
    assert _events(conn, incident) == 1
    row = conn.execute(
        "SELECT causation_id FROM planner.outbox WHERE correlation_id = %s", (incident,)
    ).fetchone()
    assert str(row["causation_id"]) == body["command_id"]


def test_concurrent_duplicates_apply_once(database_url, conn):
    """Dos peticiones simultáneas con el mismo command_id: una aplica y la otra devuelve su respuesta"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    gate = threading.Barrier(2)

    def prepare(command, payload):
        gate.wait(timeout=10)  # las dos pasan la lectura previa antes de que ninguna confirme

    client = _client(database_url, prepare=prepare)
    body = _body(incident)
    results = []

    def call():
        results.append(client.post("/commands/CreatePlan", json=body))

    threads = [threading.Thread(target=call) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert sorted(r.status_code for r in results) == [202, 202]
    assert results[0].json() == results[1].json()
    assert _events(conn, incident) == 1


def test_rejected_command_is_stored_and_writes_no_event(database_url, conn):
    """Comando rechazado: 422, ningún evento y la misma respuesta si se repite"""
    client = _client(database_url)
    body = _body("closed")
    first = client.post("/commands/CreatePlan", json=body)
    again = client.post("/commands/CreatePlan", json=body)
    assert first.status_code == again.status_code == 422
    assert first.json()["reason"] == "incident_closed" and again.json() == first.json()
    assert _events(conn, "closed") == 0


def test_invalid_payload_writes_nothing(database_url, conn):
    """Payload que no cumple el esquema del comando: 422 sin escribir en el inbox"""
    body = _body("I-x") | {"payload": {"wrong": 1}}
    response = _client(database_url).post("/commands/CreatePlan", json=body)
    assert response.status_code == 422 and response.json()["reason"] == "invalid_payload"
    row = conn.execute(
        "SELECT count(*) AS n FROM planner.inbox WHERE message_id = %s", (body["command_id"],)
    ).fetchone()
    assert row["n"] == 0
