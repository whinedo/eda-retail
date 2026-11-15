"""Comandos con trabajo largo: 202 al aceptar y el trabajo en segundo plano (spec event-backbone, «Invocación de
comandos»; design.md §4.4; odd/tasks/eda-async-commands.md S1, S2).

Sin PostgreSQL: una base falsa en memoria que entiende el inbox, el outbox y `command_jobs` del componente, con
rollback de la transacción si algo falla y un reloj propio para los plazos de los trabajos.
"""

from __future__ import annotations

import contextlib
import copy
import uuid

import pytest
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace import NonRecordingSpan, SpanContext, TraceFlags
from pydantic import BaseModel

from eda_retail.events.envelope import new_event
from eda_retail.infra import commands, jobs
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.commands import Accepted, CommandEnvelope, CommandSpec, Rejected


class _Result:
    def __init__(self, rows=(), rowcount=0):
        self._rows = list(rows)
        self.rowcount = rowcount

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


def _obj(value):
    return getattr(value, "obj", value)


class FakeDb:
    """Inbox, outbox y `command_jobs` de un componente en memoria; `now` es el reloj de la base."""

    def __init__(self, schema="planner"):
        self.schema = schema
        self.inbox: dict[uuid.UUID, dict] = {}
        self.outbox: list[dict] = []
        self.jobs: list[dict] = []
        self.now = 0.0

    @contextlib.contextmanager
    def transaction(self):
        snapshot = copy.deepcopy((self.inbox, self.outbox, self.jobs))
        try:
            yield
        except BaseException:
            self.inbox, self.outbox, self.jobs = snapshot
            raise

    def _job(self, job_id):
        return next(j for j in self.jobs if j["job_id"] == job_id)

    def execute(self, sql, params=()):
        sql = " ".join(sql.split())
        s = f"{self.schema}."
        if sql.startswith(f"SELECT status_code, response FROM {s}inbox"):
            row = self.inbox.get(params[0])
            return _Result([row] if row and row["status_code"] is not None else [])
        if sql.startswith(f"INSERT INTO {s}inbox"):
            if params[0] in self.inbox:
                return _Result(rowcount=0)
            self.inbox[params[0]] = {"kind": params[1], "status_code": None, "response": None}
            return _Result(rowcount=1)
        if sql.startswith(f"UPDATE {s}inbox SET status_code"):
            status_code, response, message_id = params
            self.inbox[message_id].update(status_code=status_code, response=_obj(response))
            return _Result(rowcount=1)
        if sql.startswith(f"INSERT INTO {s}outbox"):
            self.outbox.append({"event_id": params[0], "event_type": params[1], "causation_id": params[5]})
            return _Result([{"id": len(self.outbox)}], rowcount=1)
        if sql.startswith(f"INSERT INTO {s}command_jobs"):
            command_id, command_type, correlation_id, causation_id, payload, trace_context = params
            self.jobs.append({
                "job_id": len(self.jobs) + 1, "command_id": command_id, "command_type": command_type,
                "correlation_id": correlation_id, "causation_id": causation_id, "payload": _obj(payload),
                "trace_context": _obj(trace_context), "status": "pending", "attempts": 0,
                "next_attempt_at": self.now, "lease_until": None, "outcome": None,
            })
            return _Result(rowcount=1)
        if sql.startswith(f"UPDATE {s}command_jobs SET status = 'running'"):  # claim
            (lease,) = params
            due = [j for j in self.jobs
                   if (j["status"] == "pending" and j["next_attempt_at"] <= self.now)
                   or (j["status"] == "running" and j["lease_until"] <= self.now)]
            if not due:
                return _Result()
            job = min(due, key=lambda j: (j["next_attempt_at"], j["job_id"]))
            job.update(status="running", attempts=job["attempts"] + 1, lease_until=self.now + lease)
            return _Result([dict(job)], rowcount=1)
        if sql.startswith(f"SELECT job_id FROM {s}command_jobs WHERE job_id = %s AND status = 'running'"):
            job = self._job(params[0])
            ok = job["status"] == "running" and job["attempts"] == params[1]
            return _Result([{"job_id": job["job_id"]}] if ok else [])
        if sql.startswith(f"UPDATE {s}command_jobs SET status = %s, outcome = %s"):  # fin
            status, outcome, job_id = params
            self._job(job_id).update(status=status, outcome=_obj(outcome), lease_until=None)
            return _Result(rowcount=1)
        if sql.startswith(f"UPDATE {s}command_jobs SET status = 'pending'"):  # reintento
            delay, job_id, attempts = params
            job = self._job(job_id)
            if job["status"] != "running" or job["attempts"] != attempts:
                return _Result(rowcount=0)
            job.update(status="pending", next_attempt_at=self.now + delay, lease_until=None)
            return _Result(rowcount=1)
        raise AssertionError(f"SQL inesperado: {sql}")


class Payload(BaseModel):
    incident_id: str


def _command(**overrides) -> CommandEnvelope:
    return CommandEnvelope.model_validate({
        "command_id": uuid.uuid4(), "command_type": "CreatePlan", "correlation_id": "I-1",
        "causation_id": uuid.uuid4(), "payload": {"incident_id": "I-1"},
    } | overrides)


class Work:
    """Un handler con `prepare` largo (el LLM) que anota lo que hace."""

    def __init__(self, *, accept=None, handle=None, prepare=None):
        self.calls: list[str] = []
        self._accept, self._handle, self._prepare = accept, handle, prepare

    def prepare(self, command, payload):
        self.calls.append("prepare")
        return self._prepare(command, payload) if self._prepare else "plan"

    def handle(self, conn, command, payload, prepared):
        self.calls.append(f"handle:{prepared}")
        if self._handle:
            return self._handle(conn, command, payload, prepared)
        return Accepted([new_event(event_type="PlanCreated", correlation_id=command.correlation_id,
                                   causation_id=command.command_id, producer="planner", payload={})],
                        {"outcome": "created"})

    def accept(self, conn, command, payload):
        self.calls.append("accept")
        return self._accept(conn, command, payload) if self._accept else None

    def spec(self) -> CommandSpec:
        return CommandSpec(Payload, self.handle, prepare=self.prepare, deferred=True, accept=self.accept)


def _run(db, work, **kw) -> bool:
    return jobs.run_job_once(db, db.schema, {"CreatePlan": work.spec()}, lease_seconds=kw.get("lease", 600),  # type: ignore[arg-type]
                             backoff=kw.get("backoff", Backoff(1, 1, seed=1)))


def test_long_work_answers_202_before_the_llm_and_emits_the_result_when_the_job_ends():
    """Comando con trabajo largo"""
    db, work, command = FakeDb(), Work(), _command()

    response = commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]

    assert response.status_code == 202
    assert response.body == {"status": "accepted", "command_id": str(command.command_id)}
    assert work.calls == ["accept"]  # el LLM no se llama dentro de la petición
    assert db.outbox == []
    [job] = db.jobs
    assert (job["status"], job["command_id"], job["payload"]) == ("pending", command.command_id, {"incident_id": "I-1"})
    assert db.inbox[command.command_id]["status_code"] == 202

    assert _run(db, work) is True
    assert work.calls == ["accept", "prepare", "handle:plan"]
    assert [e["event_type"] for e in db.outbox] == ["PlanCreated"]
    assert db.outbox[0]["causation_id"] == command.command_id
    assert (db.jobs[0]["status"], db.jobs[0]["outcome"]) == ("done", {"outcome": "created", "status": "accepted"})
    assert _run(db, work) is False  # no queda trabajo


def test_acceptance_check_rejects_with_422_inside_the_request_without_a_job():
    """Un invariante que no necesita el LLM (plan_exists) se rechaza en la petición, sin trabajo"""
    db, command = FakeDb(), _command()
    work = Work(accept=lambda conn, c, p: Rejected("plan_exists"))

    first = commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]
    again = commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]

    assert first.status_code == 422 and first.body["reason"] == "plan_exists"
    assert (again.status_code, again.body, again.replayed) == (422, first.body, True)
    assert db.jobs == [] and db.outbox == []
    assert work.calls == ["accept"]


def test_repeated_command_replays_the_202_without_a_second_job():
    """Comando invocado dos veces: el segundo devuelve el 202 guardado y no crea otro trabajo"""
    db, work, command = FakeDb(), Work(), _command()

    first = commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]
    second = commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]

    assert (second.status_code, second.body, second.replayed) == (202, first.body, True)
    assert len(db.jobs) == 1


def test_without_inbox_a_repeated_deferred_command_runs_twice():
    """E1 sin idempotencia: sin inbox, el comando repetido crea otro trabajo y repite el trabajo"""
    db, work, command = FakeDb(), Work(), _command()

    for _ in range(2):
        commands.execute_command(db, "planner", command, work.spec(), inbox_enabled=False)  # type: ignore[arg-type]
    while _run(db, work):
        pass

    assert len(db.jobs) == 2 and db.inbox == {}
    assert [e["event_type"] for e in db.outbox] == ["PlanCreated", "PlanCreated"]


def test_a_command_that_is_not_deferred_still_works_inside_the_request():
    """Comando sin trabajo largo (inventory): prepare y handle en la petición, sin command_jobs"""
    db, work, command = FakeDb(), Work(), _command()
    spec = CommandSpec(Payload, work.handle, prepare=work.prepare)

    response = commands.execute_command(db, "planner", command, spec)  # type: ignore[arg-type]

    assert response.status_code == 202 and response.body["outcome"] == "created"
    assert work.calls == ["prepare", "handle:plan"]
    assert db.jobs == [] and [e["event_type"] for e in db.outbox] == ["PlanCreated"]


def test_a_late_rejection_in_the_job_writes_no_events(caplog):
    """Rechazo tardío: una carrera que la aceptación no podía ver deja el trabajo rechazado, sin eventos"""
    db, command = FakeDb(), _command()
    work = Work(handle=lambda conn, c, p, prepared: Rejected("plan_exists"))
    commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]

    assert _run(db, work) is True

    assert db.outbox == []
    assert (db.jobs[0]["status"], db.jobs[0]["outcome"]) == ("rejected", {"reason": "plan_exists"})
    assert "plan_exists" in caplog.text


def test_a_failing_job_rolls_back_and_is_retried_later():
    """Un fallo a mitad del trabajo deshace su transacción y el trabajo vuelve a pending con espera"""
    db, command = FakeDb(), _command()
    failures = iter([RuntimeError("caída a mitad")])

    def flaky(conn, c, p, prepared):
        if (exc := next(failures, None)) is not None:
            conn.execute("INSERT INTO planner.outbox x", (uuid.uuid4(), "Half", 1, None, "I-1", None))
            raise exc
        return Work().handle(conn, c, p, prepared)

    work = Work(handle=flaky)
    commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]

    assert _run(db, work) is True
    assert db.outbox == []
    assert (db.jobs[0]["status"], db.jobs[0]["attempts"]) == ("pending", 1)
    assert db.jobs[0]["next_attempt_at"] > db.now
    assert _run(db, work) is False  # aún no toca

    db.now = db.jobs[0]["next_attempt_at"]
    assert _run(db, work) is True
    assert [e["event_type"] for e in db.outbox] == ["PlanCreated"]
    assert db.jobs[0]["status"] == "done"


def test_a_job_left_running_by_a_crash_is_taken_again_and_written_once():
    """Caída con el trabajo pendiente"""
    db, work, command = FakeDb(), Work(), _command()
    commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]
    # El worker toma el trabajo y cae antes de terminarlo: queda running con su lease.
    jobs_sql = "UPDATE planner.command_jobs SET status = 'running'"
    db.execute(jobs_sql, (60,))

    assert _run(db, work) is False  # el lease aún no venció
    db.now = 61
    assert _run(db, work) is True

    assert [e["event_type"] for e in db.outbox] == ["PlanCreated"]
    assert (db.jobs[0]["status"], db.jobs[0]["attempts"]) == ("done", 2)
    assert _run(db, work) is False


def test_a_job_taken_by_another_worker_meanwhile_is_not_written_twice():
    """Si el lease venció y otro worker tomó el trabajo, el primero no escribe su resultado"""
    db, command = FakeDb(), _command()

    def slow(c, p):
        db.now = 700  # el LLM tarda más que el lease
        db.execute("UPDATE planner.command_jobs SET status = 'running'", (600,))  # otro worker lo toma
        return "plan"

    work = Work(prepare=slow)
    commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]

    assert _run(db, work) is True

    assert db.outbox == [] and work.calls == ["accept", "prepare"]
    assert (db.jobs[0]["status"], db.jobs[0]["attempts"]) == ("running", 2)


def test_the_job_runs_under_the_trace_of_the_request():
    """El trabajo continúa la traza de la petición que lo aceptó (trace_context guardado con el trabajo)"""
    db, command = FakeDb(), _command()
    seen = []
    work = Work(prepare=lambda c, p: seen.append(trace.get_current_span().get_span_context().trace_id))
    parent = SpanContext(trace_id=0x1234, span_id=0x99, is_remote=False, trace_flags=TraceFlags(1))
    token = otel_context.attach(trace.set_span_in_context(NonRecordingSpan(parent)))
    try:
        commands.execute_command(db, "planner", command, work.spec())  # type: ignore[arg-type]
    finally:
        otel_context.detach(token)

    assert db.jobs[0]["trace_context"]["traceparent"].split("-")[1] == f"{0x1234:032x}"
    _run(db, work)
    assert seen == [0x1234]


@pytest.mark.parametrize("command_type", ["Unknown"])
def test_a_job_of_an_unknown_command_is_rejected(command_type, caplog):
    db, work = FakeDb(), Work()
    commands.execute_command(db, "planner", _command(), work.spec())  # type: ignore[arg-type]
    db.jobs[0]["command_type"] = command_type

    assert _run(db, work) is True
    assert db.jobs[0]["status"] == "rejected" and db.outbox == []
