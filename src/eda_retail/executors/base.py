"""Runtime común de los ejecutores (specs step-execution, idempotency y resilience).

Cada ejecutor consume solo los `StepRequested` de su acción. Por evento:

1. `inbox.seen`: un duplicado no paga la preparación (p. ej. el redactor con LLM).
2. `prepare` fuera de toda transacción (supuesto A9); se salta si `effect_keys` ya tiene
   el efecto completado.
3. Una transacción: fila de inbox, clave en `effect_keys`, efecto simulado, `complete` y
   `RecordStepResult` (más un `RecordLlmCall` por llamada al LLM).
   - Clave ya completada: no repite el efecto y devuelve el resultado guardado, sin
     evento de efecto nuevo.
   - `EffectError` reintentable: se deshace todo y se reintenta tras la espera, hasta
     `max_attempts`; agotados, `StepFailed` con `retryable` verdadero y `attempts`
     (supuesto A5). Uno no reintentable termina en `StepFailed` al primer intento.
4. `concurrency_conflict` (otro paso de la misma incidencia a la vez): se deshace y se
   repite sin contar como intento.

El offset de Kafka lo confirma el consumidor después del `COMMIT`.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import psycopg

from eda_retail.command.handler import CommandHandler, CommandResult
from eda_retail.command.models import EscalateIncident, RecordLlmCall, RecordStepResult
from eda_retail.config import Settings
from eda_retail.events.envelope import EventEnvelope
from eda_retail.executors.retry import Backoff
from eda_retail.infra import dead_letters, effect_keys, inbox, tracing
from eda_retail.infra.consumer import Outcome
from eda_retail.llm.generation import LlmCall

log = logging.getLogger("eda_retail.executor")
TIMEOUT = "timeout"
# Rechazos que no son un error del ejecutor: el paso ya no espera este resultado.
_NO_LONGER_EXPECTED = {"step_not_requested", "incident_closed"}


class EffectError(Exception):
    """El efecto no se pudo ejecutar. `retryable` distingue transitorio de negocio."""

    escalate: str | None = None  # motivo con el que, además, se escala la incidencia

    def __init__(self, error: str, *, retryable: bool) -> None:
        super().__init__(error)
        self.error = error
        self.retryable = retryable


class Transient(EffectError):
    def __init__(self, error: str = TIMEOUT) -> None:
        super().__init__(error, retryable=True)


class BusinessError(EffectError):
    def __init__(self, error: str) -> None:
        super().__init__(error, retryable=False)


@dataclass(frozen=True)
class StepRequest:
    event: EventEnvelope
    incident_id: str
    plan_version: int
    step_id: str
    action: str
    params: dict[str, Any]
    idempotency_key: str

    @classmethod
    def of(cls, event: EventEnvelope) -> StepRequest:
        p = event.payload
        return cls(event, p["incident_id"], p["plan_version"], p["step_id"], p["action"], p["params"],
                   p["idempotency_key"])


@dataclass(frozen=True)
class Prepared:
    """Lo que el ejecutor obtiene fuera de la transacción (texto redactado…) y su coste."""

    data: Any = None
    calls: list[LlmCall] = field(default_factory=list)
    error: EffectError | None = None


@dataclass(frozen=True)
class Effect:
    result: dict[str, Any]  # resultado del paso (StepCompleted)
    effect: dict[str, Any]  # payload del evento de efecto (ClaimFiled…)


class StepAction(Protocol):
    action: str

    def prepare(self, conn: psycopg.Connection, request: StepRequest) -> Prepared: ...

    def execute(self, conn: psycopg.Connection, request: StepRequest, prepared: Prepared) -> Effect: ...


class NoPreparation:
    def prepare(self, conn: psycopg.Connection, request: StepRequest) -> Prepared:
        return Prepared()


class _Retry(Exception):
    pass


class EffectBusy(RuntimeError):
    """Otra instancia tiene la clave en curso; el offset no se confirma y Kafka lo reentrega."""


@dataclass
class ExecutorSettings:
    max_attempts: int = 3
    lock_seconds: int = 30
    inbox_enabled: bool = True
    effect_keys_enabled: bool = True
    max_conflicts: int = 10

    @classmethod
    def from_settings(cls, settings: Settings) -> ExecutorSettings:
        return cls(
            max_attempts=settings.step_max_attempts, lock_seconds=settings.effect_lock_seconds,
            inbox_enabled=settings.inbox_enabled, effect_keys_enabled=settings.effect_keys_enabled,
        )


class Executor:
    def __init__(
        self,
        action: StepAction,
        handler: CommandHandler,
        *,
        settings: ExecutorSettings | None = None,
        backoff: Backoff | None = None,
        sleep: Callable[[float], None] = time.sleep,
        on_exhausted: Callable[[psycopg.Connection, StepRequest, EffectError], None] | None = None,
    ) -> None:
        self.action = action
        self.name = f"executor-{action.action}"
        self._handler = handler
        self._settings = settings or ExecutorSettings()
        self._backoff = backoff or Backoff()
        self._sleep = sleep
        self._on_exhausted = on_exhausted
        self.last_result: CommandResult | None = None

    @property
    def handlers(self) -> dict[str, Executor]:
        return {"StepRequested": self}

    def process(self, conn: psycopg.Connection, event: EventEnvelope) -> Outcome:
        if event.payload.get("action") != self.action.action:
            return Outcome.IGNORED  # enrutado por acción: otro ejecutor lo procesa
        s = self._settings
        with conn.transaction():
            if inbox.seen(conn, self.name, event.event_id, enabled=s.inbox_enabled):
                return Outcome.DUPLICATE
        request = StepRequest.of(event)
        attempt = 1
        conflicts = 0
        with tracing.tracer().start_as_current_span(f"executor {self.action.action}") as span:
            span.set_attributes({"eda.step_id": request.step_id, "eda.incident_id": request.incident_id})
            while True:
                try:
                    outcome = self._attempt(conn, request, attempt)
                    span.set_attribute("eda.attempts", attempt)
                    return outcome
                except _Retry:
                    conflicts += 1
                    if conflicts > s.max_conflicts:
                        raise
                    continue
                except EffectError as exc:
                    if exc.retryable and attempt < s.max_attempts:
                        span.add_event("eda.retry", {"eda.attempt": attempt, "eda.error": exc.error})
                        delay = self._backoff.delay(attempt)
                        log.warning("%s %s intento %d: %s; reintento en %.2f s", request.incident_id,
                                    request.step_id, attempt, exc.error, delay)
                        self._sleep(delay)
                        attempt += 1
                        continue
                    span.set_attribute("eda.attempts", attempt)
                    return self._fail(conn, request, exc, attempt)

    def _completed_before(self, conn: psycopg.Connection, request: StepRequest) -> bool:
        if not self._settings.effect_keys_enabled:
            return False
        row = conn.execute(
            "SELECT status FROM effect_keys WHERE idempotency_key = %s", (request.idempotency_key,)
        ).fetchone()
        conn.commit()
        return row is not None and row["status"] == "completed"

    def _attempt(self, conn: psycopg.Connection, request: StepRequest, attempt: int) -> Outcome:
        s = self._settings
        prepared = Prepared() if self._completed_before(conn, request) else self.action.prepare(conn, request)
        if prepared.error is not None:
            with conn.transaction():  # el coste del LLM ya se pagó: se registra aunque el paso falle
                self._record_calls(conn, request, prepared.calls)
            raise prepared.error
        with conn.transaction():
            if not inbox.register(conn, self.name, request.event.event_id, enabled=s.inbox_enabled):
                self._record_calls(conn, request, prepared.calls)
                return Outcome.DUPLICATE
            claim = effect_keys.acquire(
                conn, key=request.idempotency_key, incident_id=request.incident_id, step_id=request.step_id,
                action=request.action, lock_seconds=s.lock_seconds, enabled=s.effect_keys_enabled,
            )
            if claim.status is effect_keys.ClaimStatus.BUSY:
                raise EffectBusy(f"{request.idempotency_key} en curso en otra instancia")
            if claim.status is effect_keys.ClaimStatus.COMPLETED:
                self._record(conn, request, outcome="completed", result=claim.result, attempts=attempt)
                return Outcome.PROCESSED
            done = self.action.execute(conn, request, prepared)
            effect_keys.complete(conn, request.idempotency_key, done.result, enabled=s.effect_keys_enabled)
            self._record(conn, request, outcome="completed", result=done.result, effect=done.effect,
                         attempts=attempt)
            self._record_calls(conn, request, prepared.calls)
        return Outcome.PROCESSED

    def _fail(self, conn: psycopg.Connection, request: StepRequest, exc: EffectError, attempt: int) -> Outcome:
        while True:
            try:
                with conn.transaction():
                    if not inbox.register(conn, self.name, request.event.event_id, enabled=self._settings.inbox_enabled):
                        return Outcome.DUPLICATE
                    self._record(conn, request, outcome="failed", error=exc.error, retryable=exc.retryable,
                                 attempts=attempt)
                    if exc.escalate is not None:
                        self._escalate(conn, request, exc.escalate)
                    if exc.retryable and self._on_exhausted is not None:
                        self._on_exhausted(conn, request, exc)
                return Outcome.PROCESSED
            except _Retry:
                continue

    def _record(self, conn: psycopg.Connection, request: StepRequest, **fields: Any) -> None:
        version = self._handler.current_version(conn, request.incident_id)
        result = self._handler.handle(
            conn,
            RecordStepResult(
                command_id=request.event.event_id, expected_version=version or 0, producer=self.name,
                incident_id=request.incident_id, step_id=request.step_id, plan_version=request.plan_version,
                **fields,
            ),
        )
        if result.reason == "concurrency_conflict":
            raise _Retry()
        if not result.accepted and result.reason not in _NO_LONGER_EXPECTED:
            raise RuntimeError(f"{self.name}: RecordStepResult rechazado: {result.reason} {result.detail}")
        self.last_result = result

    def _escalate(self, conn: psycopg.Connection, request: StepRequest, reason: str) -> None:
        version = self._handler.current_version(conn, request.incident_id)
        result = self._handler.handle(
            conn,
            EscalateIncident(command_id=request.event.event_id, expected_version=version or 0, producer=self.name,
                             incident_id=request.incident_id, reason=reason),
        )
        if result.reason == "concurrency_conflict":
            raise _Retry()

    def _record_calls(self, conn: psycopg.Connection, request: StepRequest, calls: list[LlmCall]) -> None:
        for call in calls:
            self._handler.handle(
                conn,
                RecordLlmCall(
                    command_id=request.event.event_id, expected_version=0, producer=self.name,
                    incident_id=request.incident_id, agent=call.agent, model=call.model, tokens_in=call.tokens_in,
                    tokens_out=call.tokens_out, latency_ms=call.latency_ms,
                    validation_passed=call.validation_passed, attempt=call.attempt,
                ),
            )


def dead_letter_on_exhaustion(consumer: str) -> Callable[[psycopg.Connection, StepRequest, EffectError], None]:
    """`on_exhausted` que manda a la dead letter queue el StepRequested que agotó sus intentos."""

    def on_exhausted(conn: psycopg.Connection, request: StepRequest, exc: EffectError) -> None:
        dead_letters.record(
            conn, consumer=consumer, reason=dead_letters.RETRIES_EXHAUSTED, event_id=request.event.event_id,
            correlation_id=request.incident_id, raw=request.event.model_dump(mode="json") | {"error": exc.error},
        )

    return on_exhausted
