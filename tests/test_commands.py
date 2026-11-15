"""Comandos REST idempotentes (spec event-backbone "Invocación de comandos", spec idempotency
"Inbox por consumidor"; design.md §4.4).

Sin PostgreSQL: una base falsa en memoria que entiende las sentencias del inbox y del outbox del
componente, con rollback de la transacción si el handler falla. Los escenarios contra la base
real están en tests/integration/test_commands.py.
"""

from __future__ import annotations

import contextlib
import copy
import uuid

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from pydantic import BaseModel

from eda_retail.events.envelope import new_event
from eda_retail.infra import commands
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec, Rejected


class _Result:
    def __init__(self, rows=(), rowcount=0):
        self._rows = list(rows)
        self.rowcount = rowcount

    def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeDb:
    """Inbox y outbox de un componente en memoria."""

    def __init__(self, schema="planner"):
        self.schema = schema
        self.inbox: dict[uuid.UUID, dict] = {}
        self.outbox: list[dict] = []
        self.statements: list[str] = []

    @contextlib.contextmanager
    def transaction(self):
        snapshot = copy.deepcopy((self.inbox, self.outbox))
        try:
            yield
        except BaseException:
            self.inbox, self.outbox = snapshot
            raise

    def execute(self, sql, params=()):
        sql = " ".join(sql.split())
        self.statements.append(sql)
        s = self.schema
        if sql.startswith(f"SELECT status_code, response FROM {s}.inbox"):
            row = self.inbox.get(params[0])
            return _Result([row] if row and row["status_code"] is not None else [])
        if sql.startswith(f"INSERT INTO {s}.inbox"):
            if params[0] in self.inbox:
                return _Result(rowcount=0)
            self.inbox[params[0]] = {"kind": params[1], "status_code": None, "response": None}
            return _Result(rowcount=1)
        if sql.startswith(f"UPDATE {s}.inbox SET status_code"):
            status_code, response, message_id = params
            self.inbox[message_id].update(status_code=status_code, response=response.obj)
            return _Result(rowcount=1)
        if sql.startswith(f"INSERT INTO {s}.outbox"):
            self.outbox.append({"event_id": params[0], "event_type": params[1], "causation_id": params[5]})
            return _Result([{"id": len(self.outbox)}], rowcount=1)
        raise AssertionError(f"SQL inesperado: {sql}")


class CreatePlanPayload(BaseModel):
    incident_id: str
    severity: str


def _command(**overrides) -> CommandEnvelope:
    body = {
        "command_id": uuid.uuid4(),
        "command_type": "CreatePlan",
        "correlation_id": "I-1",
        "causation_id": uuid.uuid4(),
        "payload": {"incident_id": "I-1", "severity": "high"},
    } | overrides
    return CommandEnvelope.model_validate(body)


def _plan_created(command, payload, prepared=None):
    return Accepted(
        events=[
            new_event(
                event_type="PlanCreated", correlation_id=command.correlation_id, causation_id=command.command_id,
                producer="planner", payload={"incident_id": payload.incident_id, "plan": prepared},
            )
        ],
        body={"plan_version": 1},
    )


def _spec(handle=None, prepare=None) -> CommandSpec:
    return CommandSpec(
        payload_model=CreatePlanPayload,
        handle=handle or (lambda conn, command, payload, prepared: _plan_created(command, payload, prepared)),
        prepare=prepare,
    )


def test_accepted_command_writes_its_events_and_answers_202():
    """El handler acepta el comando: inbox, estado y eventos en una transacción y respuesta 202"""
    db = FakeDb()
    command = _command()

    response = commands.execute_command(db, "planner", command, _spec())  # type: ignore[arg-type]

    assert response.status_code == 202 and not response.replayed
    assert response.body == {"status": "accepted", "command_id": str(command.command_id), "plan_version": 1}
    assert [e["event_type"] for e in db.outbox] == ["PlanCreated"]
    assert db.inbox[command.command_id]["kind"] == "command"
    assert db.inbox[command.command_id]["status_code"] == 202


def test_repeated_command_returns_the_same_response_without_side_effects():
    """Comando invocado dos veces: misma respuesta, sin estado, eventos ni efectos nuevos"""
    db = FakeDb()
    command = _command()
    calls = []
    spec = _spec(prepare=lambda c, p: calls.append("prepare") or "plan-v1")

    first = commands.execute_command(db, "planner", command, spec)  # type: ignore[arg-type]
    second = commands.execute_command(db, "planner", command, spec)  # type: ignore[arg-type]

    assert (second.status_code, second.body) == (first.status_code, first.body)
    assert second.replayed
    assert len(db.outbox) == 1
    assert calls == ["prepare"]  # el duplicado no paga otra llamada al LLM


def test_without_inbox_a_repeated_command_runs_again():
    """E1 sin idempotencia: sin inbox de comandos, el comando repetido se vuelve a ejecutar"""
    db = FakeDb()
    command = _command()

    first = commands.execute_command(db, "planner", command, _spec(), inbox_enabled=False)  # type: ignore[arg-type]
    second = commands.execute_command(db, "planner", command, _spec(), inbox_enabled=False)  # type: ignore[arg-type]

    assert first.status_code == second.status_code == 202 and not second.replayed
    assert [e["event_type"] for e in db.outbox] == ["PlanCreated", "PlanCreated"]
    assert db.inbox == {}


def test_rejected_command_answers_422_and_writes_no_event():
    """Comando rechazado por sus invariantes: 422 y ningún evento; el rechazo también se repite igual"""
    db = FakeDb()
    command = _command()
    spec = _spec(handle=lambda conn, c, p, prepared: Rejected("incident_closed"))

    first = commands.execute_command(db, "planner", command, spec)  # type: ignore[arg-type]
    again = commands.execute_command(db, "planner", command, spec)  # type: ignore[arg-type]

    assert first.status_code == 422
    assert first.body == {"status": "rejected", "command_id": str(command.command_id), "reason": "incident_closed"}
    assert db.outbox == []
    assert (again.status_code, again.body, again.replayed) == (422, first.body, True)


def test_payload_that_breaks_the_command_schema_is_rejected_without_writes():
    """Comando rechazado: un payload que no cumple el esquema responde 422 sin escribir nada"""
    db = FakeDb()
    response = commands.execute_command(db, "planner", _command(payload={"incident_id": "I-1"}), _spec())  # type: ignore[arg-type]
    assert response.status_code == 422
    assert response.body["reason"] == "invalid_payload"
    assert db.statements == []


def test_failing_handler_records_nothing_so_the_retry_runs_again():
    """Si el handler falla, la transacción se deshace: el reintento con el mismo command_id se procesa"""
    db = FakeDb()
    command = _command()

    def boom(conn, c, p, prepared):
        raise RuntimeError("caída a mitad")

    with pytest.raises(RuntimeError):
        commands.execute_command(db, "planner", command, _spec(handle=boom))  # type: ignore[arg-type]
    assert db.inbox == {} and db.outbox == []

    retry = commands.execute_command(db, "planner", command, _spec())  # type: ignore[arg-type]
    assert retry.status_code == 202 and not retry.replayed


def test_events_must_be_caused_by_the_command():
    """El evento que produce el handler lleva como causation_id el command_id"""
    db = FakeDb()
    command = _command()

    def wrong(conn, c, p, prepared):
        return Accepted(events=[new_event(event_type="PlanCreated", correlation_id="I-1", causation_id=None,
                                          producer="planner", payload={})])

    with pytest.raises(ValueError, match="causation_id"):
        commands.execute_command(db, "planner", command, _spec(handle=wrong))  # type: ignore[arg-type]
    assert db.outbox == []


# ---------------------------------------------------------------- API REST


class _Executor:
    def __init__(self, response):
        self.response = response
        self.calls: list[tuple] = []

    def __call__(self, conn, schema, command, spec):
        self.calls.append((schema, command, spec))
        return self.response


class _Conn:
    def close(self):
        self.closed = True


def _client(executor) -> TestClient:
    app = FastAPI()
    app.include_router(commands.command_router("planner", {"CreatePlan": _spec()}, _Conn, executor=executor))
    return TestClient(app)


def _body(**overrides):
    return {
        "command_id": str(uuid.uuid4()),
        "correlation_id": "I-1",
        "causation_id": str(uuid.uuid4()),
        "payload": {"incident_id": "I-1", "severity": "high"},
    } | overrides


def test_post_command_answers_with_the_handler_response():
    executor = _Executor(commands.CommandResponse(202, {"status": "accepted"}, replayed=False))
    response = _client(executor).post("/commands/CreatePlan", json=_body())
    assert response.status_code == 202 and response.json() == {"status": "accepted"}
    assert "Idempotent-Replayed" not in response.headers
    [(schema, command, _)] = executor.calls
    assert schema == "planner" and command.command_type == "CreatePlan"


def test_replayed_command_is_marked_in_a_header():
    executor = _Executor(commands.CommandResponse(202, {"status": "accepted"}, replayed=True))
    response = _client(executor).post("/commands/CreatePlan", json=_body())
    assert response.status_code == 202 and response.headers["Idempotent-Replayed"] == "true"


def test_unknown_command_is_not_found():
    executor = _Executor(None)
    assert _client(executor).post("/commands/DeleteOrder", json=_body()).status_code == 404
    assert executor.calls == []


@pytest.mark.parametrize("broken", [{"command_id": "no-uuid"}, {"correlation_id": ""}, {"extra": 1}])
def test_malformed_command_is_rejected_without_calling_the_handler(broken):
    executor = _Executor(None)
    response = _client(executor).post("/commands/CreatePlan", json=_body(**broken))
    assert response.status_code == 422 and response.json()["reason"] == "invalid_command"
    assert executor.calls == []
