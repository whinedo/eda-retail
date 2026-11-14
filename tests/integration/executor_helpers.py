"""Acciones de prueba y utilidades para los tests de ejecutores."""

from __future__ import annotations

import threading
import time
import uuid

from eda_retail.command.handler import CommandHandler
from eda_retail.executors.base import Effect, EffectError, Executor, ExecutorSettings, NoPreparation
from eda_retail.executors.retry import Backoff

from .command_helpers import handler, open_incident
from .test_plans import create, step
from .test_saga_commands import advance


class ScriptedClaim(NoPreparation):
    """file_claim que inserta en `claims` y falla según un guion de errores por intento."""

    action = "file_claim"

    def __init__(self, failures: list[EffectError] | None = None) -> None:
        self.failures = list(failures or [])
        self.calls = 0

    def execute(self, conn, request, prepared) -> Effect:
        self.calls += 1
        if self.failures:
            raise self.failures.pop(0)
        claim = f"C-{uuid.uuid4().hex[:8]}"
        conn.execute(
            "INSERT INTO claims (claim_id, incident_id, order_id) VALUES (%s, %s, %s)",
            (claim, request.incident_id, request.params["order_id"]),
        )
        return Effect({"claim_id": claim}, {"order_id": request.params["order_id"], "claim_id": claim})


class SlowNotify(NoPreparation):
    """notify_customer que solo mide cuántas operaciones hay en curso a la vez."""

    action = "notify_customer"

    def __init__(self, seconds: float = 0.02) -> None:
        self.seconds = seconds
        self.in_flight = 0
        self.peak = 0
        self._lock = threading.Lock()

    def execute(self, conn, request, prepared) -> Effect:
        with self._lock:
            self.in_flight += 1
            self.peak = max(self.peak, self.in_flight)
        time.sleep(self.seconds)
        with self._lock:
            self.in_flight -= 1
        message = f"M-{uuid.uuid4().hex[:8]}"
        return Effect({"message_id": message}, {"order_id": request.params["order_id"], "message_id": message})


def executor(action, *, max_attempts: int = 3, cmd_handler: CommandHandler | None = None, **kw) -> tuple[Executor, list]:
    sleeps: list[float] = []
    ex = Executor(action, cmd_handler or handler(), settings=ExecutorSettings(max_attempts=max_attempts),
                  backoff=Backoff(seed=1), sleep=sleeps.append, **kw)
    return ex, sleeps


def requested_steps(conn, steps: list[dict]) -> tuple[str, list]:
    """Incidencia con un plan de `steps`; devuelve (incident_id, StepRequested emitidos)."""
    incident, _ = open_incident(conn)
    assert create(conn, incident, steps).accepted
    return incident, [e for e in advance(conn, incident).events if e.event_type == "StepRequested"]


def claim_plan(order: str = "P1") -> list[dict]:
    return [step("S1", "notify_customer", order_id=order), step("S2", "file_claim", order_id=order)]
