"""Saga del orquestador contra PostgreSQL real (T7): sus tablas, su command_outbox, sus plazos y su outbox.

Los eventos se entregan llamando a la transición de la saga sin Kafka, en una transacción como la del
inbox. El dispatcher usa un `invoke` falso en lugar de HTTP.
"""

from __future__ import annotations

import uuid
from dataclasses import replace

import pytest

from eda_retail.events.catalog import build_event
from eda_retail.infra import command_client
from eda_retail.infra.effect_keys import compute_idempotency_key
from eda_retail.orchestrator.consumer import dead_letter_hook
from eda_retail.orchestrator.dispatcher import dispatch_once, expire_once
from eda_retail.orchestrator.saga import Saga
from eda_retail.rules import BusinessRules

from .component_helpers import outbox

RULES = BusinessRules()


@pytest.fixture
def settings(base_settings, database_url):
    return replace(base_settings, database_url=database_url)


@pytest.fixture
def saga(settings):
    return Saga(settings, RULES)


def _incident() -> str:
    return f"I-{uuid.uuid4().hex[:8]}"


def _deliver(conn, saga, event_type: str, incident: str, payload: dict, producer: str = "planner"):
    event = build_event(event_type, payload, correlation_id=incident, causation_id=uuid.uuid4(), producer=producer)
    with conn.transaction():
        saga.handlers()[event_type](conn, event)
    return event


def _step(step_id, action, depends_on=(), order="P-1", amount=None) -> dict:
    params = {"order_id": order} | ({"amount": amount} if amount is not None else {})
    return {"step_id": step_id, "action": action, "params": params, "depends_on": list(depends_on)}


def _open(conn, saga, steps=None, *, severity="high") -> str:
    incident = _incident()
    _deliver(conn, saga, "ShipmentDelayed", incident, {"incident_id": incident, "order_ids": ["P-1"],
                                                        "cause": "loss", "estimated_delay_hours": 24,
                                                        "severity": severity}, producer="interpreter")
    if steps is not None:
        _deliver(conn, saga, "PlanCreated", incident, {"incident_id": incident, "plan_version": 1, "steps": steps})
    return incident


def _completed(conn, saga, incident, step_id, plan_version=1, result=None):
    return _deliver(conn, saga, "StepCompleted", incident,
                    {"step_id": step_id, "plan_version": plan_version, "result": result or {}}, producer="inventory")


def _failed(conn, saga, incident, step_id, plan_version=1, *, error="out_of_stock", retryable=False):
    return _deliver(conn, saga, "StepFailed", incident, {"step_id": step_id, "plan_version": plan_version,
                                                         "error": error, "retryable": retryable, "attempts": 1},
                    producer="inventory")


def _commands(conn, incident, command_type=None) -> list[dict]:
    with conn.transaction():
        rows = conn.execute("SELECT * FROM orchestrator.command_outbox WHERE incident_id = %s ORDER BY created_at",
                            (incident,)).fetchall()
    return [r for r in rows if command_type is None or r["command_type"] == command_type]


def _executed(conn, incident) -> list[str]:
    return [c["payload"]["step_id"] for c in _commands(conn, incident, "ExecuteStep")]


def _statuses(conn, incident) -> dict[str, str]:
    with conn.transaction():
        rows = conn.execute("SELECT step_id, status FROM orchestrator.plan_steps WHERE incident_id = %s",
                            (incident,)).fetchall()
    return {r["step_id"]: r["status"] for r in rows}


def _incident_row(conn, incident) -> dict:
    with conn.transaction():
        return conn.execute("SELECT * FROM orchestrator.incidents WHERE incident_id = %s", (incident,)).fetchone()


def _types(conn, incident) -> list[str]:
    return [e["event_type"] for e in outbox(conn, "orchestrator", incident)]


def _expire(conn, incident, kind: str) -> None:
    with conn.transaction():
        conn.execute("UPDATE orchestrator.deadlines SET expires_at = now() - interval '1 second'"
                     " WHERE incident_id = %s AND kind = %s AND resolved_at IS NULL", (incident, kind))


LOSS = [_step("S1", "notify_customer"), _step("S2", "file_claim"), _step("S3", "reserve_stock"),
        _step("S4", "reissue_shipment", ["S3"])]


def test_saga_start_opens_the_incident_and_invokes_create_plan(conn, saga):
    """Inicio de la saga: incidencia open y CreatePlan al planificador, con un plazo"""
    incident = _incident()
    event = _deliver(conn, saga, "ShipmentDelayed", incident, {
        "incident_id": incident, "order_ids": ["P-1"], "cause": "loss", "estimated_delay_hours": 24,
        "severity": "high"}, producer="interpreter")

    assert _incident_row(conn, incident)["status"] == "open"
    [command] = _commands(conn, incident)
    assert (command["command_type"], command["target"], command["causation_id"]) == ("CreatePlan", "planner",
                                                                                     event.event_id)
    assert command["payload"] == {"order_ids": ["P-1"], "cause": "loss", "estimated_delay_hours": 24,
                                  "severity": "high"}
    with conn.transaction():
        deadline = conn.execute("SELECT * FROM orchestrator.deadlines WHERE command_id = %s",
                                (command["command_id"],)).fetchone()
    assert deadline["kind"] == "create_plan" and deadline["resolved_at"] is None


def test_independent_steps_are_invoked_together(conn, saga):
    """Pasos sin dependencias: los tres ExecuteStep se invocan sin esperar entre ellos"""
    incident = _open(conn, saga, LOSS)

    assert _executed(conn, incident) == ["S1", "S2", "S3"]
    assert _types(conn, incident) == ["StepRequested"] * 3
    assert _statuses(conn, incident)["S4"] == "pending"
    targets = {c["payload"]["step_id"]: c["target"] for c in _commands(conn, incident, "ExecuteStep")}
    assert targets == {"S1": "executor_notify_customer", "S2": "executor_file_claim", "S3": "inventory"}
    payloads = {c["payload"]["step_id"]: c["payload"] for c in _commands(conn, incident, "ExecuteStep")}
    row = _incident_row(conn, incident)
    for drafted in ("S1", "S2"):  # el redactor necesita la causa y el retraso: llegan en el comando
        assert (payloads[drafted]["cause"], payloads[drafted]["estimated_delay_hours"]) == (
            row["cause"], row["estimated_delay_hours"])
    assert "cause" not in payloads["S3"]
    assert all("cause" not in e["payload"] for e in outbox(conn, "orchestrator", incident))


def test_step_result_unblocks_its_dependent_in_the_same_transaction(conn, saga):
    """Resultado de un paso: completed, ExecuteStep del dependiente y su StepRequested"""
    incident = _open(conn, saga, LOSS)

    _completed(conn, saga, incident, "S3", result={"reservation_id": "R1", "warehouse": "W1"})

    assert _statuses(conn, incident)["S3"] == "completed"
    assert "S4" in _executed(conn, incident)
    requested = outbox(conn, "orchestrator", incident)[-1]
    assert (requested["event_type"], requested["payload"]["step_id"]) == ("StepRequested", "S4")


def test_a_completed_plan_resolves_the_incident(conn, saga):
    """Plan completado: IncidentResolved con resolved"""
    incident = _open(conn, saga, [_step("S1", "notify_customer")], severity="low")

    _completed(conn, saga, incident, "S1")

    assert _incident_row(conn, incident)["status"] == "resolved"
    resolved = outbox(conn, "orchestrator", incident)[-1]
    assert (resolved["event_type"], resolved["payload"]["outcome"]) == ("IncidentResolved", "resolved")


def test_a_non_retryable_failure_asks_for_a_revision(conn, saga):
    """Sin stock: StepFailed no reintentable -> RevisePlan al replanificador con el plan vigente"""
    incident = _open(conn, saga, LOSS)

    _failed(conn, saga, incident, "S3")

    [revise] = _commands(conn, incident, "RevisePlan")
    assert revise["target"] == "replanner"
    assert revise["payload"]["failed_step_id"] == "S3" and revise["payload"]["revisions"] == 0
    assert {s["step_id"]: s["status"] for s in revise["payload"]["steps"]}["S3"] == "failed"


def test_revisions_exhausted_escalate(conn, saga):
    """Revisiones agotadas: escalado con max_revisions"""
    incident = _open(conn, saga, LOSS)
    with conn.transaction():
        conn.execute("UPDATE orchestrator.incidents SET revisions = %s WHERE incident_id = %s",
                     (RULES.max_plan_revisions, incident))

    _failed(conn, saga, incident, "S2")

    assert _incident_row(conn, incident)["status"] == "escalated"
    assert outbox(conn, "orchestrator", incident)[-1]["payload"]["reason"] == "max_revisions"


def _replace_reissue_with_refund(conn, saga, incident):
    _completed(conn, saga, incident, "S3", result={"reservation_id": "R1", "warehouse": "W1"})
    _failed(conn, saga, incident, "S4", error="carrier_rejected")
    _deliver(conn, saga, "PlanRevised", incident, {
        "incident_id": incident, "plan_version": 2, "reason": "reenvío imposible",
        "steps": [_step("S1", "notify_customer"), _step("S2", "file_claim"), _step("S5", "issue_refund", amount=10)],
    }, producer="replanner")


def test_revision_compensates_the_reservation_and_holds_the_refund(conn, saga):
    """Fallo del reenvío tras reservar stock: release_stock derivado por el código y reembolso retenido"""
    incident = _open(conn, saga, LOSS)

    _replace_reissue_with_refund(conn, saga, incident)

    statuses = _statuses(conn, incident)
    assert statuses["S4"] == "cancelled" and statuses["S3"] == "compensating"
    assert statuses["S3-comp"] == "requested" and statuses["S5"] == "pending"
    [comp] = [c for c in _commands(conn, incident, "ExecuteStep") if c["payload"]["step_id"] == "S3-comp"]
    assert comp["payload"]["action"] == "release_stock" and comp["target"] == "inventory"
    assert comp["payload"]["params"]["reservation_id"] == "R1"
    assert comp["payload"]["idempotency_key"] == compute_idempotency_key(incident, "S3-comp", origin_plan_version=2)
    assert "StepCancelled" in _types(conn, incident)
    assert _incident_row(conn, incident)["plan_version"] == 2


def test_finished_compensation_releases_the_refund(conn, saga):
    """Estados tras una compensación / Compensación terminada: compensating, después compensated e issue_refund"""
    incident = _open(conn, saga, LOSS)
    _replace_reissue_with_refund(conn, saga, incident)
    assert _statuses(conn, incident)["S3"] == "compensating"

    _completed(conn, saga, incident, "S3-comp", plan_version=2, result={"released": True})

    assert _statuses(conn, incident)["S3"] == "compensated"
    assert "S5" in _executed(conn, incident)


def test_failed_release_escalates_without_refund(conn, saga):
    """La liberación del stock falla: release_stock con los reintentos agotados -> IncidentEscalated
    compensation_failed y nunca se emite el reembolso"""
    incident = _open(conn, saga, LOSS)
    _replace_reissue_with_refund(conn, saga, incident)

    _failed(conn, saga, incident, "S3-comp", plan_version=2, error="timeout", retryable=True)

    assert _incident_row(conn, incident)["reason"] == "compensation_failed"
    escalated = outbox(conn, "orchestrator", incident)[-1]
    assert (escalated["event_type"], escalated["payload"]["reason"]) == ("IncidentEscalated", "compensation_failed")
    assert "S5" not in _executed(conn, incident)
    assert _statuses(conn, incident)["S5"] == "cancelled"


def test_escalation_with_a_reservation_compensates_it(conn, saga):
    """Escalado con una reserva hecha: reenvío cancelado y su -comp invocada"""
    incident = _open(conn, saga, LOSS)
    _completed(conn, saga, incident, "S3", result={"reservation_id": "R1", "warehouse": "W1"})

    _deliver(conn, saga, "IncidentEscalated", incident, {"incident_id": incident, "reason": "llm_output_invalid"},
             producer="replanner")

    statuses = _statuses(conn, incident)
    assert statuses["S4"] == "cancelled" and statuses["S3-comp"] == "requested"
    assert _incident_row(conn, incident)["status"] == "escalated"
    assert "IncidentEscalated" not in _types(conn, incident)  # ya lo publicó el replanificador


def test_an_old_version_result_does_not_decide(conn, saga):
    """Resultado de una versión antigua: un StepFailed de la versión 1 no se usa tras la versión 2

    El orquestador no lo usa para decidir: solo queda en su inbox (no hay registro explícito de obsoleto)."""
    incident = _open(conn, saga, LOSS)
    _replace_reissue_with_refund(conn, saga, incident)
    before = (_statuses(conn, incident), len(_commands(conn, incident)))

    _failed(conn, saga, incident, "S2", plan_version=1)

    assert (_statuses(conn, incident), len(_commands(conn, incident))) == before


def test_planner_without_answer_escalates(conn, saga, settings):
    """Planificador sin respuesta: escalado con plan_timeout"""
    incident = _open(conn, saga)
    _expire(conn, incident, "create_plan")

    assert expire_once(conn, settings, RULES) >= 1

    assert _incident_row(conn, incident)["reason"] == "plan_timeout"
    assert _commands(conn, incident, "CreatePlan")[0]["status"] == "expired"


def test_step_without_answer_fails_with_timeout(conn, saga, settings):
    """Paso sin respuesta: failed con timeout y RevisePlan"""
    incident = _open(conn, saga, [_step("S1", "file_claim")])
    _expire(conn, incident, "execute_step")

    expire_once(conn, settings, RULES)

    assert _statuses(conn, incident)["S1"] == "failed"
    assert _commands(conn, incident, "RevisePlan")[0]["payload"]["error"] == "timeout"


def test_reservation_without_answer_is_compensated(conn, saga, settings):
    """Reserva sin respuesta: además de fallar, se pide reserve_stock-comp"""
    incident = _open(conn, saga, [_step("S1", "reserve_stock")])
    _expire(conn, incident, "execute_step")

    expire_once(conn, settings, RULES)

    assert "S1-comp" in _executed(conn, incident)


def test_result_after_the_deadline_is_stale(conn, saga, settings):
    """Resultado después del plazo: no cambia el estado"""
    incident = _open(conn, saga, [_step("S1", "file_claim")])
    _expire(conn, incident, "execute_step")
    expire_once(conn, settings, RULES)

    _completed(conn, saga, incident, "S1")

    assert _statuses(conn, incident)["S1"] == "failed"


def test_deadlines_expired_while_down_are_handled_on_restart(conn, saga, settings):
    """Plazos tras un reinicio: un plazo vencido con el proceso parado se trata al arrancar"""
    incident = _open(conn, saga)
    _expire(conn, incident, "create_plan")

    expire_once(conn, replace(settings), RULES)  # un proceso nuevo, sin memoria: solo sus tablas

    assert _incident_row(conn, incident)["status"] == "escalated"


class _Invoke:
    def __init__(self, *replies) -> None:
        self.replies = list(replies)
        self.calls: list[dict] = []

    def __call__(self, url, command_type, **kw):
        self.calls.append({"url": url, "command_type": command_type, **kw})
        reply = self.replies.pop(0)
        if isinstance(reply, Exception):
            raise reply
        return reply


def _drain(conn, settings, invoke, incident) -> None:
    """Despacha los comandos pendientes de esta incidencia. La base es compartida entre pruebas: los
    comandos pendientes de otras incidencias se aplazan para que el dispatcher no los tome."""
    with conn.transaction():
        conn.execute("UPDATE orchestrator.command_outbox SET next_attempt_at = 'infinity'"
                     " WHERE status = 'pending' AND incident_id <> %s", (incident,))
    while dispatch_once(conn, settings, RULES, invoke=invoke):
        if not invoke.replies:
            break


def test_command_survives_a_crash_and_is_invoked_with_its_command_id(conn, saga, settings):
    """Caída antes de invocar: el CreatePlan pendiente se invoca al arrancar con su command_id"""
    incident = _open(conn, saga)
    [command] = _commands(conn, incident)
    invoke = _Invoke(command_client.CommandReply(202, {"status": "accepted"}))

    _drain(conn, settings, invoke, incident)

    assert invoke.calls[0]["command_id"] == command["command_id"]
    assert invoke.calls[0]["url"] == settings.planner_url
    assert _commands(conn, incident)[0]["status"] == "delivered"


def test_unavailable_system_is_retried_with_the_same_command_id(conn, saga, settings):
    """Sistema que no responde: espera con jitter y reintenta con el mismo command_id"""
    incident = _open(conn, saga)
    invoke = _Invoke(command_client.CommandUnavailable("caído"))

    _drain(conn, settings, invoke, incident)

    [command] = _commands(conn, incident)
    assert (command["status"], command["attempts"]) == ("pending", 1)
    with conn.transaction():
        conn.execute("UPDATE orchestrator.command_outbox SET next_attempt_at = now() WHERE command_id = %s",
                     (command["command_id"],))
    invoke.replies.append(command_client.CommandReply(202, {"status": "accepted"}))
    _drain(conn, settings, invoke, incident)
    assert [c["command_id"] for c in invoke.calls] == [command["command_id"]] * 2


def test_rejected_step_fails_and_asks_for_a_revision(conn, saga, settings):
    """Comando rechazado: un 422 deja el paso failed con error rejected"""
    incident = _open(conn, saga, [_step("S1", "reserve_stock")])
    with conn.transaction():
        conn.execute("UPDATE orchestrator.command_outbox SET status = 'delivered' WHERE incident_id = %s"
                     " AND command_type = 'CreatePlan'", (incident,))
    invoke = _Invoke(command_client.CommandReply(422, {"status": "rejected", "reason": "action_not_handled"}))

    _drain(conn, settings, invoke, incident)

    assert _statuses(conn, incident)["S1"] == "failed"
    assert _commands(conn, incident, "RevisePlan")[0]["payload"]["error"] == "rejected"


def test_rejected_create_plan_escalates(conn, saga, settings):
    incident = _open(conn, saga)
    invoke = _Invoke(command_client.CommandReply(422, {"status": "rejected", "reason": "plan_exists"}))

    _drain(conn, settings, invoke, incident)

    assert _incident_row(conn, incident)["reason"] == "command_rejected"


def test_a_dead_letter_escalates_its_incident(conn, saga, settings):
    """Evento con esquema inválido: la incidencia se escala con invalid_schema"""
    incident = _open(conn, saga, LOSS)

    with conn.transaction():
        dead_letter_hook(settings, RULES)(conn, incident, uuid.uuid4())

    assert _incident_row(conn, incident)["reason"] == "invalid_schema"
    assert _types(conn, incident)[-1] == "IncidentEscalated"


def test_restarted_orchestrator_recovers_its_incidents_from_its_tables(conn, connect_db, settings):
    """Reinicio del orquestador: la versión vigente, los pasos pendientes y los plazos de cada incidencia
    salen de orchestrator.plan_steps y orchestrator.deadlines"""
    before = Saga(settings, RULES)
    incident = _open(conn, before, LOSS)
    _replace_reissue_with_refund(conn, before, incident)

    restarted = Saga(replace(settings), RULES)  # un proceso nuevo, sin memoria: solo sus tablas
    other = connect_db()
    _failed(other, restarted, incident, "S2", plan_version=1)  # resultado de la versión 1: ya no decide
    _completed(other, restarted, incident, "S3-comp", plan_version=2, result={"released": True})

    assert _incident_row(conn, incident)["plan_version"] == 2
    statuses = _statuses(conn, incident)
    assert (statuses["S2"], statuses["S3"], statuses["S5"]) == ("requested", "compensated", "requested")
    with conn.transaction():
        open_deadlines = conn.execute(
            "SELECT step_id FROM orchestrator.deadlines WHERE incident_id = %s AND kind = 'execute_step'"
            " AND resolved_at IS NULL", (incident,)).fetchall()
    assert {d["step_id"] for d in open_deadlines} == {"S1", "S2", "S5"}


class _CountingInvoke:
    """Invocación REST falsa que mide cuántas hay en curso a la vez."""

    def __init__(self) -> None:
        self.in_flight = 0
        self.peak = 0
        self.calls: list[dict] = []

    def __call__(self, url, command_type, **kw):
        self.in_flight += 1
        self.peak = max(self.peak, self.in_flight)
        try:
            self.calls.append({"command_type": command_type, **kw})
            return command_client.CommandReply(202, {"status": "accepted"})
        finally:
            self.in_flight -= 1


def test_burst_of_pending_steps_is_invoked_within_the_limit(conn, saga, settings):
    """Pico de incidencias: 50 pasos pendientes se invocan sin superar el límite de operaciones en curso
    y ninguno se pierde"""
    steps = [_step(f"S{i}", "notify_customer") for i in range(1, 51)]
    incident = _open(conn, saga, steps)
    with conn.transaction():
        conn.execute("UPDATE orchestrator.command_outbox SET next_attempt_at = 'infinity'"
                     " WHERE status = 'pending' AND incident_id <> %s", (incident,))
    invoke = _CountingInvoke()

    for _ in range(200):
        if not dispatch_once(conn, settings, RULES, invoke=invoke):
            break

    assert invoke.peak <= settings.executor_max_concurrency
    invoked = [c["payload"]["step_id"] for c in invoke.calls if c["command_type"] == "ExecuteStep"]
    assert sorted(invoked) == sorted(s["step_id"] for s in steps)
    assert {c["status"] for c in _commands(conn, incident, "ExecuteStep")} == {"delivered"}
