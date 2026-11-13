"""Command handler (spec cqrs-views): único punto de escritura del dominio.

`handle` valida el comando contra el estado de escritura y, si se acepta, actualiza ese
estado y añade los eventos al log y al outbox. Se ejecuta dentro de la transacción del
llamante (la del consumidor, con su fila de inbox), en un savepoint propio: un comando
rechazado no deja nada escrito.

Concurrencia optimista: `UPDATE incidents SET version = version + 1 WHERE version =
expected_version`; si no actualiza ninguna fila, se rechaza con `concurrency_conflict`.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

import psycopg
from opentelemetry.trace import Span

from eda_retail.command.models import (
    CloseIncident,
    Command,
    EscalateIncident,
    IncidentCommand,
    OpenIncident,
    RecordLlmCall,
    RegisterCarrierNotice,
)
from eda_retail.command.rules import BusinessRules
from eda_retail.events.catalog import Cause, PayloadError, PersonalDataError, Severity, build_event
from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import tracing
from eda_retail.infra.event_store import append_events

_NOTICE_NAMESPACE = uuid.UUID("6f1d7c0e-2b8a-4c55-9d3e-7a1b2c3d4e5f")
FINAL_STEP_STATUSES = ("completed", "cancelled", "compensated")
# Comandos que cargan la incidencia por su cuenta (pueden no exigir que exista o esté abierta).
_SELF_LOADING = (OpenIncident, RecordLlmCall, EscalateIncident)


def incident_id_for_notice(carrier_notice_id: str) -> str:
    """Un aviso da lugar como mucho a una incidencia: su id se deriva del aviso."""
    return f"I-{uuid.uuid5(_NOTICE_NAMESPACE, carrier_notice_id)}"


@dataclass(frozen=True)
class CommandResult:
    accepted: bool
    reason: str | None = None
    detail: str | None = None
    events: list[EventEnvelope] = field(default_factory=list)
    version: int | None = None


class Rejected(Exception):
    def __init__(self, reason: str, detail: str | None = None) -> None:
        super().__init__(reason)
        self.reason = reason
        self.detail = detail


@dataclass
class Context:
    """Lo que necesita cada comando: conexión, reglas, incidencia y eventos a emitir."""

    conn: psycopg.Connection
    command: Command
    rules: BusinessRules
    incident: dict[str, Any] | None = None
    events: list[EventEnvelope] = field(default_factory=list)
    version: int | None = None

    def emit(self, event_type: str, payload: dict[str, Any], *, correlation_id: str | None = None) -> EventEnvelope:
        cmd = self.command
        causation = None if isinstance(cmd, RegisterCarrierNotice) else cmd.command_id
        try:
            ev = build_event(
                event_type,
                payload,
                correlation_id=correlation_id or cmd.incident_id,  # type: ignore[attr-defined]
                causation_id=causation,
                producer=cmd.producer,
            )
        except PersonalDataError as exc:
            raise Rejected("personal_data_in_event", str(exc)) from exc
        except PayloadError as exc:
            raise Rejected("invalid_command", str(exc)) from exc
        self.events.append(ev)
        return ev

    def bump_version(self) -> int:
        """Concurrencia optimista: incrementa la versión solo si sigue siendo la esperada."""
        cmd = self.command
        row = self.conn.execute(
            "UPDATE incidents SET version = version + 1 WHERE incident_id = %s AND version = %s RETURNING version",
            (cmd.incident_id, cmd.expected_version),  # type: ignore[attr-defined]
        ).fetchone()
        if row is None:
            raise Rejected("concurrency_conflict")
        self.version = row["version"]
        return self.version


Executor = Callable[[Context], None]
_EXECUTORS: dict[type[Command], Executor] = {}


def executes(command_type: type[Command]) -> Callable[[Executor], Executor]:
    def decorate(fn: Executor) -> Executor:
        _EXECUTORS[command_type] = fn
        return fn

    return decorate


class CommandHandler:
    def __init__(self, rules: BusinessRules | None = None) -> None:
        self._rules = rules or BusinessRules()

    def check_budget(self, conn: psycopg.Connection, incident_id: str) -> str | None:
        """`budget_exhausted` si la incidencia ha consumido sus tokens o sus llamadas al LLM.

        Se consulta antes de pagar otra llamada; una incidencia que aún no existe no ha gastado nada.
        """
        row = conn.execute(
            "SELECT tokens_used, llm_calls FROM incidents WHERE incident_id = %s", (incident_id,)
        ).fetchone()
        if row is None:
            return None
        if row["tokens_used"] >= self._rules.max_tokens_per_incident:
            return "budget_exhausted"
        if row["llm_calls"] >= self._rules.max_llm_calls_per_incident:
            return "budget_exhausted"
        return None

    def handle(self, conn: psycopg.Connection, command: Command) -> CommandResult:
        with tracing.tracer().start_as_current_span(
            f"command {command.name}",
            attributes={
                "eda.command": command.name,
                "eda.command_id": str(command.command_id),
                "eda.incident_id": getattr(command, "incident_id", ""),
                "eda.expected_version": command.expected_version,
            },
        ) as span:
            result = self._handle(conn, command)
            _record(span, result)
            return result

    def _handle(self, conn: psycopg.Connection, command: Command) -> CommandResult:
        ctx = Context(conn, command, self._rules)
        try:
            with conn.transaction():  # savepoint si el llamante ya tiene una transacción
                if isinstance(command, IncidentCommand) and not isinstance(command, _SELF_LOADING):
                    _load_open_incident(ctx)
                executor = _EXECUTORS.get(type(command))
                if executor is None:
                    raise Rejected("invalid_command", f"comando no soportado: {command.name}")
                executor(ctx)
                append_events(conn, ctx.events)
        except Rejected as rej:
            return CommandResult(False, rej.reason, rej.detail)
        return CommandResult(True, events=list(ctx.events), version=ctx.version)


def _record(span: Span, result: CommandResult) -> None:
    span.set_attribute("eda.accepted", result.accepted)
    if result.reason:
        span.set_attribute("eda.reason", result.reason)


def _load_open_incident(ctx: Context) -> None:
    cmd = ctx.command
    row = ctx.conn.execute(
        "SELECT * FROM incidents WHERE incident_id = %s", (cmd.incident_id,)  # type: ignore[attr-defined]
    ).fetchone()
    if row is None:
        raise Rejected("incident_not_found")
    if row["status"] != "open":
        raise Rejected("incident_closed", f"estado {row['status']}")
    if row["version"] != cmd.expected_version:
        raise Rejected("concurrency_conflict", f"versión actual {row['version']}")
    ctx.incident = row


# --- avisos e incidencias -------------------------------------------------------------


@executes(RegisterCarrierNotice)
def _register_notice(ctx: Context) -> None:
    cmd: RegisterCarrierNotice = ctx.command  # type: ignore[assignment]
    if cmd.expected_version != 0:
        raise Rejected("concurrency_conflict", "un aviso nuevo se registra con expected_version 0")
    incident_id = incident_id_for_notice(cmd.carrier_notice_id)
    inserted = ctx.conn.execute(
        "INSERT INTO carrier_notices (carrier_notice_id, incident_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (cmd.carrier_notice_id, incident_id),
    ).rowcount
    if inserted == 0:
        raise Rejected("duplicate_notice")
    ctx.emit(
        "CarrierNoticeReceived",
        {"carrier_notice_id": cmd.carrier_notice_id, "carrier": cmd.carrier, "raw_text": cmd.raw_text},
        correlation_id=incident_id,
    )


@executes(OpenIncident)
def _open_incident(ctx: Context) -> None:
    cmd: OpenIncident = ctx.command  # type: ignore[assignment]
    notice = ctx.conn.execute(
        "SELECT incident_id FROM carrier_notices WHERE carrier_notice_id = %s", (cmd.carrier_notice_id,)
    ).fetchone()
    if notice is None:
        raise Rejected("notice_not_found")
    if notice["incident_id"] != cmd.incident_id:
        raise Rejected("invalid_command", "el incident_id no corresponde al aviso")
    exists = ctx.conn.execute("SELECT 1 FROM incidents WHERE incident_id = %s", (cmd.incident_id,)).fetchone()
    if exists is not None:
        raise Rejected("duplicate_notice")
    if cmd.expected_version != 0:
        raise Rejected("concurrency_conflict", "una incidencia nueva se abre con expected_version 0")
    if cmd.cause not in set(Cause):
        raise Rejected("invalid_command", f"cause no válida: {cmd.cause}")
    if cmd.severity not in set(Severity):
        raise Rejected("invalid_command", f"severity no válida: {cmd.severity}")
    if cmd.cause == Cause.UNKNOWN and Severity(cmd.severity).rank < Severity.MEDIUM.rank:
        raise Rejected("invalid_severity", "un aviso de causa unknown exige severity medium o superior")

    known = {
        r["order_id"]
        for r in ctx.conn.execute(
            "SELECT order_id FROM orders WHERE order_id = ANY(%s)", (cmd.order_ids,)
        ).fetchall()
    }
    order_ids = [o for o in dict.fromkeys(cmd.order_ids) if o in known]
    status = "open" if order_ids else "escalated"
    # ON CONFLICT: dos instancias que abren la misma incidencia a la vez no fallan con
    # UniqueViolation; la segunda espera a la primera y se rechaza como duplicado.
    inserted = ctx.conn.execute(
        """
        INSERT INTO incidents (incident_id, status, severity, cause, version, closed_at)
        VALUES (%s, %s, %s, %s, 1, CASE WHEN %s = 'escalated' THEN now() END)
        ON CONFLICT (incident_id) DO NOTHING
        """,
        (cmd.incident_id, status, cmd.severity, cmd.cause, status),
    ).rowcount
    if inserted == 0:
        raise Rejected("duplicate_notice")
    ctx.version = 1
    if not order_ids:
        ctx.emit("IncidentEscalated", {"incident_id": cmd.incident_id, "reason": "no_valid_orders"})
        return
    ctx.emit(
        "ShipmentDelayed",
        {
            "incident_id": cmd.incident_id,
            "order_ids": order_ids,
            "cause": cmd.cause,
            "estimated_delay_hours": cmd.estimated_delay_hours,
            "severity": cmd.severity,
        },
    )


@executes(EscalateIncident)
def _escalate(ctx: Context) -> None:
    cmd: EscalateIncident = ctx.command  # type: ignore[assignment]
    exists = ctx.conn.execute("SELECT 1 FROM incidents WHERE incident_id = %s", (cmd.incident_id,)).fetchone()
    if exists is None:
        _escalate_from_notice(ctx)
        return
    _load_open_incident(ctx)
    ctx.bump_version()
    ctx.conn.execute(
        "UPDATE incidents SET status = 'escalated', closed_at = now() WHERE incident_id = %s", (cmd.incident_id,)
    )
    ctx.emit("IncidentEscalated", {"incident_id": cmd.incident_id, "reason": cmd.reason})


def _escalate_from_notice(ctx: Context) -> None:
    """Supuesto A5: el intérprete agota sus intentos antes de abrir la incidencia.

    Se acepta si la incidencia corresponde a un aviso registrado; se crea ya escalada, con
    causa `unknown` (y por tanto gravedad `medium`, la mínima que admite esa causa).
    """
    cmd: EscalateIncident = ctx.command  # type: ignore[assignment]
    notice = ctx.conn.execute(
        "SELECT 1 FROM carrier_notices WHERE incident_id = %s", (cmd.incident_id,)
    ).fetchone()
    if notice is None:
        raise Rejected("incident_not_found")
    if cmd.expected_version != 0:
        raise Rejected("concurrency_conflict", "una incidencia nueva se escala con expected_version 0")
    inserted = ctx.conn.execute(
        """
        INSERT INTO incidents (incident_id, status, severity, cause, version, closed_at)
        VALUES (%s, 'escalated', %s, %s, 1, now())
        ON CONFLICT (incident_id) DO NOTHING
        """,
        (cmd.incident_id, Severity.MEDIUM.value, Cause.UNKNOWN.value),
    ).rowcount
    if inserted == 0:
        raise Rejected("concurrency_conflict", "la incidencia se creó a la vez")
    ctx.version = 1
    ctx.emit("IncidentEscalated", {"incident_id": cmd.incident_id, "reason": cmd.reason})


@executes(RecordLlmCall)
def _record_llm_call(ctx: Context) -> None:
    """Supuesto A6: sin concurrencia optimista; el coste ya se produjo aunque esté cerrada."""
    cmd: RecordLlmCall = ctx.command  # type: ignore[assignment]
    row = ctx.conn.execute(
        """
        UPDATE incidents SET tokens_used = tokens_used + %s, llm_calls = llm_calls + 1
        WHERE incident_id = %s RETURNING version
        """,
        (cmd.tokens_in + cmd.tokens_out, cmd.incident_id),
    ).fetchone()
    if row is None:
        raise Rejected("incident_not_found")
    ctx.version = row["version"]
    ctx.emit(
        "LlmCallRecorded",
        {
            "incident_id": cmd.incident_id, "agent": cmd.agent, "model": cmd.model,
            "tokens_in": cmd.tokens_in, "tokens_out": cmd.tokens_out, "latency_ms": cmd.latency_ms,
            "validation_passed": cmd.validation_passed, "attempt": cmd.attempt,
        },
    )


@executes(CloseIncident)
def _close(ctx: Context) -> None:
    cmd: CloseIncident = ctx.command  # type: ignore[assignment]
    if ctx.incident["plan_version"] == 0:  # type: ignore[index]
        raise Rejected("no_plan")
    pending = ctx.conn.execute(
        "SELECT count(*) AS n FROM plan_steps WHERE incident_id = %s AND status <> ALL(%s)",
        (cmd.incident_id, list(FINAL_STEP_STATUSES)),
    ).fetchone()["n"]
    if pending:
        raise Rejected("steps_pending", f"{pending} pasos sin terminar")
    ctx.bump_version()
    ctx.conn.execute(
        "UPDATE incidents SET status = 'resolved', closed_at = now() WHERE incident_id = %s", (cmd.incident_id,)
    )
    ctx.emit("IncidentResolved", {"incident_id": cmd.incident_id, "outcome": cmd.outcome})


# Los comandos de planes y pasos se registran al importar sus módulos.
from eda_retail.command import plans, steps  # noqa: E402,F401
