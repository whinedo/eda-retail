"""Runtime común de los ejecutores con command handler REST (T9; specs step-execution, idempotency y resilience).

La saga invoca `ExecuteStep` en `EXECUTOR_<ACCIÓN>_URL`. Es un comando con trabajo largo (design.md §4.4):
la petición solo lo acepta (`accept_step` rechaza otra acción, otra incidencia o un paso sin la causa que
necesita el redactor) y responde 202; el worker del ejecutor hace después:

1. `prepare`, fuera de la transacción: si `effect_keys` ya tiene la clave completada, devuelve su
   resultado sin preparar nada. Si no, la preparación de la acción (pedido en `view_orders`, borrador
   del redactor) y la llamada al servicio con la `idempotency_key`, reintentando los errores
   transitorios con espera exponencial y jitter hasta `STEP_MAX_ATTEMPTS`. El servicio reconoce la
   clave, así que una llamada repetida no repite el efecto.
2. `handle`, en la transacción del trabajo: un `LlmCallRecorded` (y su fila) por llamada del
   redactor; un error termina en `StepFailed` con `retryable` y `attempts` (y antes
   `IncidentEscalated` si el error escala la incidencia); si no, la clave en `effect_keys`, la fila
   del efecto, el evento del efecto y `StepCompleted`. Una clave ya completada devuelve el resultado
   guardado sin un evento de efecto nuevo.

Un fallo de negocio es un comando aceptado cuyo resultado es `StepFailed`; un comando inválido para
este ejecutor (otra acción, otra incidencia, sin la causa que necesita el redactor) se rechaza.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

import psycopg
from pydantic import BaseModel, ConfigDict, Field

from eda_retail.config import Settings
from eda_retail.events.catalog import build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import effect_keys
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.commands import Accepted, CommandEnvelope, Rejected
from eda_retail.llm.generation import LlmCall
from eda_retail.step_executors.errors import EffectError

ACTION_NOT_HANDLED = "action_not_handled"
INCIDENT_MISMATCH = "incident_mismatch"
MISSING_CONTEXT = "missing_context"


class ExecuteStep(BaseModel):
    """El paso que pide la saga. `cause` y `estimated_delay_hours` solo llegan a las acciones redactadas."""

    model_config = ConfigDict(extra="forbid")

    incident_id: str = Field(min_length=1)
    plan_version: int = Field(ge=1)
    step_id: str = Field(min_length=1)
    action: str = Field(min_length=1)
    params: dict[str, Any]
    idempotency_key: str = Field(min_length=1)
    cause: str | None = None
    estimated_delay_hours: int | None = Field(default=None, ge=0)


@dataclass(frozen=True)
class Preparation:
    """Lo que la acción prepara antes del efecto (el borrador y sus llamadas al LLM) o el error."""

    text: str | None = None
    calls: list[LlmCall] = field(default_factory=list)
    error: EffectError | None = None


@dataclass(frozen=True)
class Prepared:
    stored: dict[str, Any] | None = None  # resultado de una clave ya completada
    external_id: str | None = None  # identificador que devolvió el servicio
    text: str | None = None
    calls: list[LlmCall] = field(default_factory=list)
    error: EffectError | None = None
    attempts: int = 0


class StepAction(Protocol):
    action: str
    needs_context: bool

    def prepare(self, step: ExecuteStep) -> Preparation: ...

    def call(self, step: ExecuteStep, preparation: Preparation) -> str: ...


class RecordingAction(StepAction, Protocol):
    """La parte de la acción que escribe en su esquema (vive en el paquete del componente)."""

    schema: str
    effect_event: str

    def record(self, conn: psycopg.Connection, step: ExecuteStep,
               prepared: Prepared) -> tuple[dict[str, Any], dict[str, Any]]: ...

    def record_calls(self, conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> None: ...


class EffectInProgress(RuntimeError):
    """La clave del efecto está `in_progress` en otra instancia con su plazo vigente: el trabajo se reintenta."""


def _missing_context(action: StepAction, step: ExecuteStep) -> bool:
    return action.needs_context and (step.cause is None or step.estimated_delay_hours is None)


def _rejection(action: StepAction, command: CommandEnvelope, step: ExecuteStep) -> Rejected | None:
    """Lo que hace inválido el comando para este ejecutor; lo mismo en la aceptación y en el trabajo."""
    if step.action != action.action:
        return Rejected(ACTION_NOT_HANDLED)
    if command.correlation_id != step.incident_id:
        return Rejected(INCIDENT_MISMATCH)
    if _missing_context(action, step):
        return Rejected(MISSING_CONTEXT)  # el redactor necesita cause y estimated_delay_hours
    return None


def accept_step(action: StepAction):
    """`accept` del comando: comprueba el comando en la petición, sin base de datos ni servicio."""

    def accept(conn: psycopg.Connection, command: CommandEnvelope, step: ExecuteStep) -> Rejected | None:
        return _rejection(action, command, step)

    return accept


class StepPreparer:
    """`prepare` del comando: lo que se hace fuera de la transacción."""

    def __init__(self, action: StepAction, *, completed: Callable[[str], dict[str, Any] | None], max_attempts: int,
                 backoff: Backoff, sleep: Callable[[float], Any] = time.sleep,
                 existing: Callable[[ExecuteStep], dict[str, Any] | None] | None = None) -> None:
        """`existing`: el resultado de un efecto que ya existe por la clave de negocio de la acción, aunque la
        `idempotency_key` sea otra (p. ej. la reclamación de la misma incidencia y pedido en `file_claim`)."""
        self._action = action
        self._completed = completed
        self._existing = existing
        self._max_attempts = max_attempts
        self._backoff = backoff
        self._sleep = sleep

    def __call__(self, command: CommandEnvelope, step: ExecuteStep) -> Prepared:
        if step.action != self._action.action or _missing_context(self._action, step):
            return Prepared()  # handle lo rechaza
        stored = self._completed(step.idempotency_key)
        if stored is not None:
            return Prepared(stored=stored)  # no se paga otra vez el redactor ni el servicio
        if self._existing is not None and (found := self._existing(step)) is not None:
            return Prepared(stored=found)  # el efecto ya existe con otra clave: no se repite
        preparation = self._action.prepare(step)
        if preparation.error is not None:
            return Prepared(calls=preparation.calls, error=preparation.error, attempts=1)
        attempts = 0
        while True:
            attempts += 1
            try:
                external_id = self._action.call(step, preparation)
            except EffectError as exc:
                if exc.retryable and attempts < self._max_attempts:
                    self._sleep(self._backoff.delay(attempts))
                    continue
                return Prepared(calls=preparation.calls, error=exc, attempts=attempts)
            return Prepared(external_id=external_id, text=preparation.text, calls=preparation.calls,
                            attempts=attempts)


def _event(command: CommandEnvelope, producer: str, event_type: str, payload: dict[str, Any]) -> EventEnvelope:
    return build_event(event_type, payload, correlation_id=command.correlation_id,
                       causation_id=command.command_id, producer=producer)


def execute_step(action: RecordingAction, settings: Settings):
    """`handle` del comando `ExecuteStep` de una acción."""
    producer = f"executor-{action.action}"

    def handle(conn: psycopg.Connection, command: CommandEnvelope, step: ExecuteStep,
               prepared: Prepared) -> Accepted | Rejected:
        if (rejected := _rejection(action, command, step)) is not None:
            return rejected

        def event(event_type: str, payload: dict[str, Any]) -> EventEnvelope:
            return _event(command, producer, event_type, payload)

        events = []
        if prepared.calls:
            action.record_calls(conn, command, prepared.calls)
            events += [event("LlmCallRecorded", {
                "incident_id": step.incident_id, "agent": call.agent, "model": call.model,
                "tokens_in": call.tokens_in, "tokens_out": call.tokens_out, "latency_ms": call.latency_ms,
                "validation_passed": call.validation_passed, "attempt": call.attempt,
            }) for call in prepared.calls]
        if prepared.error is not None:
            if prepared.error.escalate:  # la saga escala antes de recibir el fallo
                events.append(event("IncidentEscalated", {"incident_id": step.incident_id,
                                                          "reason": prepared.error.escalate}))
            events.append(event("StepFailed", {
                "step_id": step.step_id, "plan_version": step.plan_version, "error": prepared.error.error,
                "retryable": prepared.error.retryable, "attempts": prepared.attempts,
            }))
            return Accepted(events)
        claim = effect_keys.acquire(
            conn, key=step.idempotency_key, incident_id=step.incident_id, step_id=step.step_id, action=step.action,
            lock_seconds=settings.effect_lock_seconds, enabled=settings.effect_keys_enabled, schema=action.schema,
        )
        if claim.status is effect_keys.ClaimStatus.COMPLETED:
            result = claim.result or {}
        elif claim.status is effect_keys.ClaimStatus.BUSY:
            # Clave y efecto se confirman juntos: solo otra transacción en curso. El cliente reintenta.
            raise EffectInProgress(f"efecto {step.idempotency_key} en curso")
        elif prepared.stored is not None:
            # Resultado guardado de esta clave, o de un efecto que ya existía con otra clave: se registra en
            # la clave de este paso, sin evento de efecto nuevo.
            result = prepared.stored
            effect_keys.complete(conn, step.idempotency_key, result, enabled=settings.effect_keys_enabled,
                                 schema=action.schema)
        else:
            result, effect = action.record(conn, step, prepared)
            effect_keys.complete(conn, step.idempotency_key, result, enabled=settings.effect_keys_enabled,
                                 schema=action.schema)
            events.append(event(action.effect_event, effect))
        events.append(event("StepCompleted", {"step_id": step.step_id, "plan_version": step.plan_version,
                                              "result": result}))
        return Accepted(events)

    return handle
