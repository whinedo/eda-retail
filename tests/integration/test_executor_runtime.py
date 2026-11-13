"""Runtime de los ejecutores: enrutado, reintentos, idempotencia de efectos y concurrencia."""

from __future__ import annotations

import threading
import uuid

from eda_retail.executors.base import BusinessError, Transient
from eda_retail.executors.pool import ConcurrentRunner
from eda_retail.infra.consumer import Outcome

from .command_helpers import events_of
from .executor_helpers import ScriptedClaim, SlowNotify, claim_plan, executor, requested_steps
from .test_plans import plan_steps, step


def claims_of(conn, incident: str) -> int:
    n = conn.execute("SELECT count(*) AS n FROM claims WHERE incident_id = %s", (incident,)).fetchone()["n"]
    conn.commit()
    return n


def types(conn, incident: str) -> list[str]:
    return [e["event_type"] for e in events_of(conn, incident)]


def test_routing_by_action(conn):
    """Enrutado por acción"""
    incident, [notify, claim] = requested_steps(conn, claim_plan())
    ex, _ = executor(ScriptedClaim())
    assert ex.process(conn, notify) == Outcome.IGNORED
    assert ex.process(conn, claim) == Outcome.PROCESSED
    assert claims_of(conn, incident) == 1
    assert plan_steps(conn, incident)["S1"]["status"] == "requested"


def test_transient_error_solved_on_second_attempt(conn):
    """Error transitorio resuelto en el segundo intento"""
    incident, [_, claim] = requested_steps(conn, claim_plan())
    action = ScriptedClaim([Transient()])
    ex, sleeps = executor(action)
    assert ex.process(conn, claim) == Outcome.PROCESSED
    assert action.calls == 2 and len(sleeps) == 1 and 0 <= sleeps[0] <= 0.5
    assert claims_of(conn, incident) == 1  # el efecto se ejecuta una sola vez
    assert types(conn, incident).count("ClaimFiled") == 1
    assert plan_steps(conn, incident)["S2"]["status"] == "completed"


def test_transient_error(conn):
    """Error transitorio"""
    incident, [_, claim] = requested_steps(conn, claim_plan())
    exhausted = []
    ex, sleeps = executor(ScriptedClaim([Transient()] * 3), on_exhausted=lambda c, r, e: exhausted.append(r.step_id))
    ex.process(conn, claim)
    [failed] = [e for e in events_of(conn, incident) if e["event_type"] == "StepFailed"]
    assert failed["payload"] == {"step_id": "S2", "plan_version": 1, "error": "timeout", "retryable": True, "attempts": 3}
    assert len(sleeps) == 2 and claims_of(conn, incident) == 0
    assert exhausted == ["S2"]
    row = conn.execute("SELECT status, attempts FROM plan_steps WHERE incident_id = %s AND step_id = 'S2'",
                       (incident,)).fetchone()
    conn.commit()
    assert (row["status"], row["attempts"]) == ("failed", 3)


def test_business_error_is_not_retried(conn):
    """Los errores no reintentables no se reintentan"""
    incident, [_, claim] = requested_steps(conn, claim_plan())
    action = ScriptedClaim([BusinessError("order_closed")])
    ex, sleeps = executor(action)
    ex.process(conn, claim)
    [failed] = [e for e in events_of(conn, incident) if e["event_type"] == "StepFailed"]
    assert (failed["payload"]["retryable"], failed["payload"]["attempts"]) == (False, 1)
    assert action.calls == 1 and sleeps == []


def test_duplicate_event(conn):
    """Evento duplicado"""
    incident, [_, claim] = requested_steps(conn, claim_plan())
    action = ScriptedClaim()
    ex, _ = executor(action)
    ex.process(conn, claim)
    assert ex.process(conn, claim) == Outcome.DUPLICATE
    assert action.calls == 1 and claims_of(conn, incident) == 1


def test_new_event_asks_for_executed_effect(conn):
    """Evento nuevo que pide un efecto ya ejecutado"""
    incident, [_, claim] = requested_steps(conn, claim_plan())
    action = ScriptedClaim()
    ex, _ = executor(action)
    ex.process(conn, claim)
    first = next(e for e in events_of(conn, incident) if e["event_type"] == "StepCompleted")["payload"]["result"]
    again = claim.model_copy(update={"event_id": uuid.uuid4()})
    assert ex.process(conn, again) == Outcome.PROCESSED  # el inbox lo registra como nuevo
    assert action.calls == 1 and claims_of(conn, incident) == 1
    assert types(conn, incident).count("ClaimFiled") == 1
    inbox = conn.execute("SELECT count(*) AS n FROM inbox WHERE consumer = 'executor-file_claim' AND event_id = %s",
                         (again.event_id,)).fetchone()["n"]
    conn.commit()
    assert inbox == 1
    assert first["claim_id"].startswith("C-")


def test_two_executors_at_once(connect_db):
    """Dos ejecutores a la vez"""
    conns = [connect_db(), connect_db()]
    incident, [_, claim] = requested_steps(conns[0], claim_plan())
    action = ScriptedClaim()
    barrier = threading.Barrier(2)
    outcomes = []

    def run(c):
        ex, _ = executor(action)
        barrier.wait()
        outcomes.append(ex.process(c, claim))

    threads = [threading.Thread(target=run, args=(c,)) for c in conns]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(o.value for o in outcomes) == ["duplicate", "processed"]
    assert action.calls == 1 and claims_of(conns[0], incident) == 1


def test_peak_of_incidents(conn, database_url):
    """Pico de incidencias"""
    from eda_retail.infra.db import connect

    per_incident = []
    for _ in range(10):  # 10 incidencias x 5 avisos = 50 pasos pendientes
        _, requested = requested_steps(conn, [step(f"S{i}", "notify_customer", order_id=f"P{i}") for i in range(5)])
        per_incident.append(requested)
    events = [ev for group in zip(*per_incident, strict=True) for ev in group]  # llegan intercalados
    action = SlowNotify()
    ex, _ = executor(action)
    runner = ConcurrentRunner(ex, lambda: connect(database_url), max_concurrency=5)
    try:
        outcomes = []
        for i in range(0, len(events), 5):
            outcomes += runner.process_batch(events[i:i + 5])
    finally:
        runner.close()
    assert action.peak <= 5  # nunca hay más de 5 en curso
    assert outcomes == [Outcome.PROCESSED] * 50  # los 50 terminan sin pérdida
    done = conn.execute(
        "SELECT count(*) AS n FROM plan_steps WHERE incident_id = ANY(%s) AND status = 'completed'",
        (list({e.correlation_id for e in events}),),
    ).fetchone()["n"]
    conn.commit()
    assert done == 50
