"""Transiciones de la saga del orquestador (design.md §4.2, spec incident-planning "Saga reactiva").

Cada transición corre en una transacción que bloquea la fila de la incidencia (`FOR UPDATE`), así
que el consumidor, el dispatcher y el hilo de plazos nunca deciden a la vez sobre la misma
incidencia. En esa transacción escribe sus tablas (`orchestrator.incidents`, `plan_steps`), deja en
`command_outbox` los comandos que invocará por REST con su plazo en `deadlines`, y en su outbox los
eventos: `StepRequested` (uno por `ExecuteStep`, un evento de notificación para la auditoría y el
front), `StepCancelled`, `IncidentResolved` e `IncidentEscalated`.

Un resultado o una revisión que llegan tarde (versión superada, paso que ya no está `requested`,
incidencia cerrada) no cambian nada: quedan en el inbox. La excepción es un `reserve_stock`
cancelado que sí reservó (supuesto A4): se marca completado y se compensa.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from eda_retail.command.rules import BusinessRules
from eda_retail.config import Settings
from eda_retail.events.catalog import Action, build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import outbox, tracing
from eda_retail.orchestrator import rules

SCHEMA = "orchestrator"
PLAN_TIMEOUT = "plan_timeout"
STEP_TIMEOUT = "step_timeout"
COMMAND_REJECTED = "command_rejected"
TIMEOUT = "timeout"
REJECTED = "rejected"

INVENTORY_ACTIONS = {Action.RESERVE_STOCK.value, Action.RELEASE_STOCK.value}
_STEP_COLUMNS = ("step_id", "action", "params", "depends_on", "compensates", "status", "origin_plan_version",
                 "result")


def step_target(action: str) -> str:
    """Sistema que atiende el `ExecuteStep` de una acción."""
    return "inventory" if action in INVENTORY_ACTIONS else f"executor_{action}"


def target_url(settings: Settings, target: str) -> str:
    return getattr(settings, f"{target}_url")


@dataclass
class Transition:
    """Una transición de la saga sobre una incidencia ya bloqueada."""

    conn: psycopg.Connection
    settings: Settings
    rules: BusinessRules
    incident: dict[str, Any]
    cause: uuid.UUID  # evento o comando que la origina: causation_id de lo que emite
    events: list[EventEnvelope] = field(default_factory=list)

    @property
    def incident_id(self) -> str:
        return self.incident["incident_id"]

    def emit(self, event_type: str, payload: dict[str, Any]) -> None:
        self.events.append(build_event(event_type, payload, correlation_id=self.incident_id,
                                       causation_id=self.cause, producer=SCHEMA))

    def flush(self) -> None:
        outbox.append(self.conn, SCHEMA, self.events)
        self.events = []

    # --- lectura y escritura de su estado ---------------------------------------------------

    def steps(self) -> list[dict[str, Any]]:
        return self.conn.execute(
            f"SELECT {', '.join(_STEP_COLUMNS)} FROM orchestrator.plan_steps WHERE incident_id = %s",
            (self.incident_id,),
        ).fetchall()

    def step(self, step_id: str) -> dict[str, Any] | None:
        return self.conn.execute(
            f"SELECT {', '.join(_STEP_COLUMNS)} FROM orchestrator.plan_steps WHERE incident_id = %s AND step_id = %s",
            (self.incident_id, step_id),
        ).fetchone()

    def insert_steps(self, steps: list[dict[str, Any]]) -> None:
        for s in steps:
            self.conn.execute(
                "INSERT INTO orchestrator.plan_steps (incident_id, step_id, origin_plan_version, action, params,"
                " depends_on, compensates, status) VALUES (%s, %s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING",
                (self.incident_id, s["step_id"], s["origin_plan_version"], s["action"], Jsonb(s["params"]),
                 s["depends_on"], s["compensates"], s["status"]),
            )

    def set_step(self, step_id: str, status: str, **columns: Any) -> None:
        sets = ", ".join(["status = %s", *(f"{c} = %s" for c in columns)])
        values = [Jsonb(v) if c in ("params", "result") else v for c, v in columns.items()]
        self.conn.execute(f"UPDATE orchestrator.plan_steps SET {sets} WHERE incident_id = %s AND step_id = %s",
                          (status, *values, self.incident_id, step_id))

    def set_incident(self, **columns: Any) -> None:
        sets = ", ".join(f"{c} = %s" for c in columns)
        self.conn.execute(f"UPDATE orchestrator.incidents SET {sets} WHERE incident_id = %s",
                          (*columns.values(), self.incident_id))
        self.incident.update(columns)

    def enqueue(self, command_type: str, target: str, payload: dict[str, Any], *, kind: str, timeout_s: float,
                step_id: str | None = None) -> uuid.UUID:
        """Deja el comando en `command_outbox` con su plazo durable."""
        command_id = uuid.uuid4()
        trace_context = tracing.inject_current()
        self.conn.execute(
            "INSERT INTO orchestrator.command_outbox (command_id, incident_id, command_type, target, causation_id,"
            " payload, trace_context) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (command_id, self.incident_id, command_type, target, self.cause, Jsonb(payload),
             Jsonb(trace_context) if trace_context else None),
        )
        self.conn.execute(
            "INSERT INTO orchestrator.deadlines (command_id, incident_id, kind, step_id, expires_at)"
            " VALUES (%s, %s, %s, %s, now() + make_interval(secs => %s))",
            (command_id, self.incident_id, kind, step_id, timeout_s),
        )
        return command_id

    def close_deadlines(self, kind: str, step_id: str | None = None) -> None:
        """Resuelve los plazos abiertos (el resultado llegó) y deja de invocar sus comandos pendientes."""
        rows = self.conn.execute(
            "UPDATE orchestrator.deadlines SET resolved_at = now() WHERE incident_id = %s AND kind = %s"
            " AND (%s::text IS NULL OR step_id = %s) AND resolved_at IS NULL RETURNING command_id",
            (self.incident_id, kind, step_id, step_id),
        ).fetchall()
        if rows:
            self.conn.execute(
                "UPDATE orchestrator.command_outbox SET status = 'expired' WHERE command_id = ANY(%s)"
                " AND status = 'pending'",
                ([r["command_id"] for r in rows],),
            )

    # --- decisiones -------------------------------------------------------------------------

    def request(self, step: dict[str, Any]) -> None:
        self.set_step(step["step_id"], "requested")
        if step["compensates"]:
            self.set_step(step["compensates"], "compensating")
        payload = rules.step_request(self.incident_id, self.incident["plan_version"], step)
        self.enqueue("ExecuteStep", step_target(step["action"]), payload, kind="execute_step",
                     timeout_s=self.settings.saga_step_timeout_s, step_id=step["step_id"])
        self.emit("StepRequested", payload)

    def advance(self) -> None:
        """Pide los pasos listos o, si el plan terminó, resuelve la incidencia."""
        steps = self.steps()
        escalated = self.incident["status"] == "escalated"
        ready = rules.ready_steps(steps, escalated=escalated)
        for s in ready:
            self.request(s)
        if ready or self.incident["status"] != "open":
            return
        if (result := rules.outcome(steps)) is not None:
            self.set_incident(status="resolved", closed_at=_now(self.conn))
            self.emit("IncidentResolved", {"incident_id": self.incident_id, "outcome": result})

    def escalate(self, reason: str, *, publish: bool) -> None:
        """Escalado: cancela los pasos sin efecto, compensa las reservas y nunca pide el reembolso."""
        if self.incident["status"] != "open":
            return
        steps = self.steps()
        cancelled, comps = rules.escalation(steps, reason, plan_version=max(self.incident["plan_version"], 1))
        self.set_incident(status="escalated", reason=reason, closed_at=_now(self.conn))
        for kind in ("create_plan", "revise_plan"):
            self.close_deadlines(kind)
        for step_id in cancelled:
            self.set_step(step_id, "cancelled")
            self.close_deadlines("execute_step", step_id)
            self.emit("StepCancelled", {"incident_id": self.incident_id,
                                        "plan_version": max(self.incident["plan_version"], 1), "step_id": step_id})
        if publish:
            self.emit("IncidentEscalated", {"incident_id": self.incident_id, "reason": reason})
        self.insert_steps(comps)
        self.advance()

    def fail(self, step: dict[str, Any], *, error: str, retryable: bool, attempts: int = 1) -> None:
        """Un paso `requested` que falla: revisión del plan o escalado (spec incident-planning)."""
        self.set_step(step["step_id"], "failed", error=error, attempts=attempts)
        self.close_deadlines("execute_step", step["step_id"])
        if self.incident["status"] != "open":
            return  # escalada: solo quedaban compensaciones
        if step["action"] == Action.ISSUE_REFUND.value and error == TIMEOUT:
            self.escalate(STEP_TIMEOUT, publish=True)
            return
        if step["action"] in rules.COMPENSATIONS and error == TIMEOUT:
            self.insert_steps(rules.compensations([step], plan_version=self.incident["plan_version"]))
        decision, reason = rules.on_failure(step, retryable=retryable, revisions=self.incident["revisions"],
                                            max_revisions=self.rules.max_plan_revisions)
        if decision == "escalate":
            self.escalate(reason, publish=True)
            return
        self.enqueue("RevisePlan", "replanner", {
            "plan_version": self.incident["plan_version"], "revisions": self.incident["revisions"],
            "failed_step_id": step["step_id"], "error": error,
            "steps": [_plan_step(s) for s in self.steps()],
        }, kind="revise_plan", timeout_s=self.settings.saga_plan_timeout_s)
        self.advance()


def _plan_step(step: dict[str, Any]) -> dict[str, Any]:
    return {k: step[k] for k in ("step_id", "action", "params", "depends_on", "status", "origin_plan_version")}


def _now(conn: psycopg.Connection):
    return conn.execute("SELECT now() AS now").fetchone()["now"]


def lock(conn: psycopg.Connection, settings: Settings, rules_: BusinessRules, incident_id: str,
         cause: uuid.UUID) -> Transition | None:
    row = conn.execute("SELECT * FROM orchestrator.incidents WHERE incident_id = %s FOR UPDATE",
                       (incident_id,)).fetchone()
    return Transition(conn, settings, rules_, dict(row), cause) if row is not None else None


class Saga:
    """Las transiciones que disparan los eventos que consume el orquestador (dentro de la transacción del inbox)."""

    def __init__(self, settings: Settings, rules_: BusinessRules) -> None:
        self._settings = settings
        self._rules = rules_

    def _lock(self, conn: psycopg.Connection, event: EventEnvelope) -> Transition | None:
        return lock(conn, self._settings, self._rules, event.correlation_id, event.event_id)

    def handlers(self) -> dict[str, Any]:
        return {
            "ShipmentDelayed": self.shipment_delayed,
            "PlanCreated": self.plan_created,
            "PlanRevised": self.plan_revised,
            "StepCompleted": self.step_completed,
            "StepFailed": self.step_failed,
            "IncidentEscalated": self.incident_escalated,
        }

    def shipment_delayed(self, conn: psycopg.Connection, event: EventEnvelope) -> None:
        """Inicio de la saga: incidencia `open` y `CreatePlan` al planificador, con su plazo."""
        p = event.payload
        inserted = conn.execute(
            "INSERT INTO orchestrator.incidents (incident_id, status, order_ids, cause, estimated_delay_hours,"
            " severity) VALUES (%s, 'open', %s, %s, %s, %s) ON CONFLICT DO NOTHING RETURNING *",
            (p["incident_id"], p["order_ids"], p["cause"], p["estimated_delay_hours"], p["severity"]),
        ).fetchone()
        if inserted is None:
            return
        t = Transition(conn, self._settings, self._rules, dict(inserted), event.event_id)
        t.enqueue("CreatePlan", "planner", {k: p[k] for k in ("order_ids", "cause", "estimated_delay_hours",
                                                              "severity")},
                  kind="create_plan", timeout_s=self._settings.saga_plan_timeout_s)

    def plan_created(self, conn: psycopg.Connection, event: EventEnvelope) -> None:
        t = self._lock(conn, event)
        if t is None or t.incident["status"] != "open" or t.incident["plan_version"] != 0:
            return
        t.close_deadlines("create_plan")
        t.set_incident(plan_version=1)
        t.insert_steps([{**s, "status": "pending", "origin_plan_version": 1, "compensates": None}
                        for s in event.payload["steps"]])
        t.advance()
        t.flush()

    def plan_revised(self, conn: psycopg.Connection, event: EventEnvelope) -> None:
        t = self._lock(conn, event)
        new_version = event.payload["plan_version"]
        if t is None or t.incident["status"] != "open" or new_version != t.incident["plan_version"] + 1:
            return
        before = {s["step_id"]: s for s in t.steps()}
        revision = rules.revise(list(before.values()), event.payload["steps"], new_version=new_version)
        t.close_deadlines("revise_plan")
        t.set_incident(plan_version=new_version, revisions=t.incident["revisions"] + 1)
        for s in revision.steps:
            old = before.get(s["step_id"])
            if old is None:
                t.insert_steps([s])
                continue
            if old["status"] == "requested" and s["status"] != "requested":
                t.close_deadlines("execute_step", s["step_id"])
            t.set_step(s["step_id"], s["status"], params=s["params"], depends_on=s["depends_on"])
        for step_id in revision.cancelled:
            t.emit("StepCancelled", {"incident_id": t.incident_id, "plan_version": new_version, "step_id": step_id})
        t.insert_steps(revision.compensations)
        t.advance()
        t.flush()

    def step_completed(self, conn: psycopg.Connection, event: EventEnvelope) -> None:
        t = self._lock(conn, event)
        p = event.payload
        step = t.step(p["step_id"]) if t is not None else None
        if t is None or step is None or t.incident["status"] == "resolved":
            return
        stale = p["plan_version"] < t.incident["plan_version"] or step["status"] != "requested"
        if stale:
            if step["status"] == "cancelled" and step["action"] in rules.COMPENSATIONS:  # supuesto A4
                t.set_step(step["step_id"], "completed", result=p["result"])
                t.insert_steps(rules.compensations([{**step, "result": p["result"]}],
                                                   plan_version=max(t.incident["plan_version"], 1)))
                t.advance()
                t.flush()
            return
        t.set_step(step["step_id"], "completed", result=p["result"])
        t.close_deadlines("execute_step", step["step_id"])
        if step["compensates"]:
            t.set_step(step["compensates"], "compensated")
        t.advance()
        t.flush()

    def step_failed(self, conn: psycopg.Connection, event: EventEnvelope) -> None:
        t = self._lock(conn, event)
        p = event.payload
        step = t.step(p["step_id"]) if t is not None else None
        if t is None or step is None or t.incident["status"] == "resolved":
            return
        if p["plan_version"] < t.incident["plan_version"] or step["status"] != "requested":
            return  # versión antigua: no decide
        t.fail(step, error=p["error"], retryable=p["retryable"], attempts=p["attempts"])
        t.flush()

    def incident_escalated(self, conn: psycopg.Connection, event: EventEnvelope) -> None:
        """Escalado de otro componente (planificador, replanificador, ejecutor…)."""
        if event.producer == SCHEMA:
            return
        t = self._lock(conn, event)
        if t is None:
            return
        t.escalate(event.payload["reason"], publish=False)
        t.flush()
