"""Plazos de la saga desde el primer envío y revisión pendiente (spec incident-planning, «Plazos de la saga» y
«Revisión del plan ante un fallo»; odd/tasks/eda-async-commands.md S3, S4).

Sin PostgreSQL: una conexión falsa que guarda las sentencias y responde a las lecturas que hacen falta. Los
escenarios contra la base real están en tests/integration/test_orchestrator_saga.py.
"""

from __future__ import annotations

import contextlib
import uuid

from eda_retail.config import Settings
from eda_retail.infra import command_client
from eda_retail.orchestrator import rules
from eda_retail.orchestrator.dispatcher import dispatch_once
from eda_retail.orchestrator.saga import Transition
from eda_retail.rules import BusinessRules


class _Result:
    def __init__(self, rows=(), rowcount=1):
        self._rows = list(rows)
        self.rowcount = rowcount

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def fetchall(self):
        return list(self._rows)


class FakeConn:
    def __init__(self, due: dict | None = None):
        self.due = due
        self.statements: list[tuple[str, tuple]] = []

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params=()):
        sql = " ".join(sql.split())
        self.statements.append((sql, params))
        if sql.startswith("SELECT * FROM orchestrator.command_outbox WHERE status = 'pending'"):
            return _Result([self.due] if self.due else [])
        return _Result()

    def sql(self, prefix: str) -> list[tuple[str, tuple]]:
        return [(s, p) for s, p in self.statements if s.startswith(prefix)]


def test_the_saga_stores_the_timeout_and_the_deadline_starts_unset():
    """La saga guarda el plazo del comando sin empezar a contarlo: lo empieza el primer envío"""
    conn = FakeConn()
    t = Transition(conn, Settings(), BusinessRules(), {"incident_id": "I-1"}, uuid.uuid4())  # type: ignore[arg-type]

    command_id = t.enqueue("CreatePlan", "planner", {}, kind="create_plan", timeout_s=300)

    [(sql, params)] = conn.sql("INSERT INTO orchestrator.deadlines")
    assert "timeout_s" in sql and "expires_at" not in sql
    assert params == (command_id, "I-1", "create_plan", None, 300)


def _due_command() -> dict:
    return {"command_id": uuid.uuid4(), "incident_id": "I-1", "command_type": "ExecuteStep",
            "target": "executor_file_claim", "causation_id": uuid.uuid4(), "payload": {}, "trace_context": None,
            "attempts": 0}


def _deadline_started(conn: FakeConn, command_id) -> bool:
    return any("SET expires_at = now() + make_interval(secs => timeout_s)" in s and "expires_at IS NULL" in s
               and p == (command_id,) for s, p in conn.sql("UPDATE orchestrator.deadlines"))


def test_the_deadline_starts_when_the_dispatcher_first_sends_the_command():
    """Plazo desde el envío: el dispatcher empieza a contar el plazo antes de invocar el comando"""
    row = _due_command()
    conn = FakeConn(due=row)
    started_before_send = []

    def invoke(url, command_type, **kw):
        started_before_send.append(_deadline_started(conn, row["command_id"]))
        return command_client.CommandReply(202, {"status": "accepted"})

    assert dispatch_once(conn, Settings(), BusinessRules(), invoke=invoke)  # type: ignore[arg-type]

    assert started_before_send == [True]


def test_the_deadline_also_starts_when_the_target_does_not_answer():
    """Paso sin respuesta: aunque el ejecutor no responda, el plazo empezó al primer envío y vencerá"""
    row = _due_command()
    conn = FakeConn(due=row)

    def invoke(url, command_type, **kw):
        raise command_client.CommandUnavailable("caído")

    assert dispatch_once(conn, Settings(), BusinessRules(), invoke=invoke)  # type: ignore[arg-type]

    assert _deadline_started(conn, row["command_id"])


def test_a_failure_while_a_revision_is_pending_waits_for_it():
    """Segundo fallo con una revisión pendiente: no se pide otra revisión ni se escala"""
    step = {"step_id": "S2", "compensates": None}
    assert rules.on_failure(step, retryable=False, revisions=0, max_revisions=2,
                            revision_pending=True) == ("wait", None)
    assert rules.on_failure(step, retryable=False, revisions=2, max_revisions=2,
                            revision_pending=True) == ("wait", None)


def test_failures_that_escalate_still_escalate_with_a_pending_revision():
    """Las reglas de escalado no dependen de la revisión: compensación fallida y reintentos agotados"""
    comp = {"step_id": "S3-comp", "compensates": "S3"}
    step = {"step_id": "S2", "compensates": None}
    assert rules.on_failure(comp, retryable=False, revisions=0, max_revisions=2,
                            revision_pending=True) == ("escalate", "compensation_failed")
    assert rules.on_failure(step, retryable=True, revisions=0, max_revisions=2,
                            revision_pending=True) == ("escalate", "retries_exhausted")


def _s(step_id, status, compensates=None):
    return {"step_id": step_id, "status": status, "compensates": compensates}


def test_steps_that_failed_while_the_revision_was_pending_and_are_kept_are_handled_again():
    """Al aplicar la revisión, un paso que falló mientras tanto y que el plan nuevo mantiene se trata con las
    reglas normales; el paso que pidió la revisión y los que el plan nuevo no mantiene, no"""
    before = [_s("S1", "completed"), _s("S2", "failed"), _s("S3", "failed"), _s("S4", "failed"),
              _s("S5-comp", "failed", compensates="S5")]
    revised = [{"step_id": "S2"}, {"step_id": "S3"}, {"step_id": "S6"}]

    assert rules.failed_meanwhile(before, revised, trigger="S2") == ["S3"]
    assert rules.failed_meanwhile(before, revised, trigger=None) == []
