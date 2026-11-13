"""Orquestador: consumidor sin estado que traduce eventos en comandos (supuesto A1).

| Evento | Comandos |
| --- | --- |
| `PlanCreated` | `AdvancePlan` |
| `PlanRevised` | `ApplyRevision` y `AdvancePlan` |
| `StepCompleted` | `AdvancePlan` |
| `StepFailed` de un paso `-comp` | `EscalateIncident` `compensation_failed` |
| `StepFailed` reintentable (el ejecutor agotó sus intentos) | `EscalateIncident` `retries_exhausted` |
| `StepFailed` no reintentable | ninguno: lo replanifica el replanner |

Los `StepCompleted` y `StepFailed` de una `plan_version` anterior a la vigente no se usan
para decidir (spec incident-planning, «Eventos obsoletos»): quedan en el log y el span
lleva `eda.stale=true`. Todo el estado está en `plan_steps` e `incidents`; un orquestador
reiniciado sigue donde lo dejó el anterior.

Inbox y comandos van en una transacción. Si un comando choca con otro escritor
(`concurrency_conflict`, p. ej. dos ejecutores en paralelo), se deshace y se vuelve a
decidir con la versión nueva.
"""

from __future__ import annotations

import psycopg
from opentelemetry import trace

from eda_retail.command.handler import CommandHandler, CommandResult
from eda_retail.command.models import AdvancePlan, ApplyRevision, Command, EscalateIncident
from eda_retail.command.steps import COMPENSATION_SUFFIX
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import inbox, tracing
from eda_retail.infra.consumer import Outcome

NAME = "orchestrator"
EVENT_TYPES = ("PlanCreated", "PlanRevised", "StepCompleted", "StepFailed")
COMPENSATION_FAILED = "compensation_failed"
RETRIES_EXHAUSTED = "retries_exhausted"


class ConcurrencyRetriesExhausted(RuntimeError):
    """El comando siguió en conflicto tras los reintentos; el offset no se confirma."""


class _Conflict(Exception):
    pass


class Orchestrator:
    def __init__(self, handler: CommandHandler, *, inbox_enabled: bool = True, max_conflicts: int = 5) -> None:
        self._handler = handler
        self._inbox_enabled = inbox_enabled
        self._max_conflicts = max_conflicts
        self.results: list[CommandResult] = []  # resultados del último evento (trazas y tests)

    @property
    def handlers(self) -> dict[str, Orchestrator]:
        return dict.fromkeys(EVENT_TYPES, self)

    def process(self, conn: psycopg.Connection, event: EventEnvelope) -> Outcome:
        for _ in range(self._max_conflicts):
            try:
                with tracing.tracer().start_as_current_span(f"orchestrator {event.event_type}"), conn.transaction():
                    if not inbox.register(conn, NAME, event.event_id, enabled=self._inbox_enabled):
                        return Outcome.DUPLICATE
                    self.results = self._decide(conn, event)
                return Outcome.PROCESSED
            except _Conflict:
                continue
        raise ConcurrencyRetriesExhausted(f"{NAME}: conflicto persistente en {event.event_id}")

    def _decide(self, conn: psycopg.Connection, event: EventEnvelope) -> list[CommandResult]:
        incident_id = event.correlation_id
        p = event.payload
        span = trace.get_current_span()
        if event.event_type in ("StepCompleted", "StepFailed"):
            current = self._handler.plan_version(conn, incident_id)
            stale = current is not None and p["plan_version"] < current
            span.set_attribute("eda.stale", stale)
            if stale:
                return []
        if event.event_type == "StepFailed":
            if p["step_id"].endswith(COMPENSATION_SUFFIX):
                return self._run(conn, event, [self._escalate(conn, event, COMPENSATION_FAILED)])
            if p["retryable"]:
                return self._run(conn, event, [self._escalate(conn, event, RETRIES_EXHAUSTED)])
            return []
        commands: list[Command] = []
        if event.event_type == "PlanRevised":
            commands.append(
                ApplyRevision(
                    command_id=event.event_id, expected_version=0, producer=NAME, incident_id=incident_id,
                    plan_version=p["plan_version"], step_ids=[s["step_id"] for s in p["steps"]],
                )
            )
        commands.append(AdvancePlan(command_id=event.event_id, expected_version=0, producer=NAME, incident_id=incident_id))
        return self._run(conn, event, commands)

    def _escalate(self, conn: psycopg.Connection, event: EventEnvelope, reason: str) -> EscalateIncident:
        return EscalateIncident(
            command_id=event.event_id, expected_version=0, producer=NAME,
            incident_id=event.correlation_id, reason=reason,
        )

    def _run(self, conn: psycopg.Connection, event: EventEnvelope, commands: list[Command]) -> list[CommandResult]:
        """Envía los comandos en orden, cada uno con la versión que dejó el anterior."""
        results = []
        for command in commands:
            version = self._handler.current_version(conn, event.correlation_id)
            result = self._handler.handle(conn, command.model_copy(update={"expected_version": version or 0}))
            if result.reason == "concurrency_conflict":
                raise _Conflict()
            results.append(result)
        return results
