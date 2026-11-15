"""Comandos REST idempotentes de cada sistema (design.md §4.4, spec event-backbone
"Invocación de comandos").

Cada sistema expone su command handler como `POST /commands/<Comando>`. La respuesta solo dice si
el Aggregate acepta el comando (202) o lo rechaza (422); el resultado de negocio llega después
como evento por Kafka. Todo comando es idempotente:

1. Payload que no cumple el esquema del comando: 422 sin escribir nada.
2. Lectura previa del inbox: un `command_id` ya respondido devuelve la respuesta guardada (con
   `Idempotent-Replayed: true`) sin pagar otra vez `prepare` (p. ej. la llamada al LLM).
3. `prepare`, fuera de la transacción (regla 6 de CLAUDE.md: el LLM antes de abrirla).
4. En una transacción: inbox (`message_id` = `command_id`, `kind` = 'command'), el handler (valida
   sus invariantes y escribe su estado), sus eventos en el outbox y la respuesta en el inbox. Si
   otra petición con el mismo `command_id` va por delante, el INSERT espera a su COMMIT y esta
   devuelve la respuesta guardada.

Si el handler falla, la transacción se deshace y nada queda registrado: el reintento con el mismo
`command_id` se procesa de nuevo.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import psycopg
from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse
from opentelemetry.trace import SpanKind
from psycopg.types.json import Jsonb
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import inbox, outbox, tracing
from eda_retail.infra.schemas import qualified

ACCEPTED = 202
REJECTED = 422
REPLAYED_HEADER = "Idempotent-Replayed"


class CommandEnvelope(BaseModel):
    """Estructura de todo comando. No se publica en Kafka."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    command_id: uuid.UUID
    command_type: str = Field(min_length=1)
    correlation_id: str = Field(min_length=1)
    causation_id: uuid.UUID | None
    payload: dict[str, Any]


@dataclass(frozen=True)
class Accepted:
    """El Aggregate acepta el comando: estos eventos van a su outbox en la misma transacción."""

    events: list[EventEnvelope]
    body: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class Rejected:
    """El Aggregate rechaza el comando por sus invariantes; no escribe eventos."""

    reason: str


# (conexión, comando, payload validado, resultado de prepare) -> Accepted | Rejected
Handle = Callable[[psycopg.Connection, CommandEnvelope, BaseModel, Any], Accepted | Rejected]
Prepare = Callable[[CommandEnvelope, BaseModel], Any]


@dataclass(frozen=True)
class CommandSpec:
    payload_model: type[BaseModel]
    handle: Handle
    prepare: Prepare | None = None


@dataclass(frozen=True)
class CommandResponse:
    status_code: int
    body: dict[str, Any]
    replayed: bool = False


def _stored(conn: psycopg.Connection, schema: str, command_id: uuid.UUID) -> CommandResponse | None:
    row = conn.execute(
        f"SELECT status_code, response FROM {qualified(schema, 'inbox')}"
        " WHERE message_id = %s AND kind = 'command' AND status_code IS NOT NULL",
        (command_id,),
    ).fetchone()
    if row is None:
        return None
    return CommandResponse(row["status_code"], row["response"], replayed=True)


def _check_causation(command: CommandEnvelope, events: list[EventEnvelope]) -> None:
    for e in events:
        if e.causation_id != command.command_id or e.correlation_id != command.correlation_id:
            raise ValueError(
                f"{e.event_type} debe llevar causation_id={command.command_id} y correlation_id="
                f"{command.correlation_id} (comando {command.command_type})"
            )


def execute_command(
    conn: psycopg.Connection, schema: str, command: CommandEnvelope, spec: CommandSpec, *,
    inbox_enabled: bool = True,
) -> CommandResponse:
    """Ejecuta el comando con el handler del sistema `schema`. Idempotente por `command_id`.

    Con `inbox_enabled` falso (solo el experimento E1, para medir el sistema sin idempotencia) no
    consulta ni escribe el inbox: un comando repetido se vuelve a ejecutar.

    `conn` debe estar sin transacción abierta: con una abierta, los bloques serían savepoints y nada
    se confirmaría aquí. El router abre una conexión por petición.
    """
    base = {"command_id": str(command.command_id)}
    try:
        payload = spec.payload_model.model_validate(command.payload)
    except ValidationError as exc:
        return CommandResponse(
            REJECTED, base | {"status": "rejected", "reason": "invalid_payload", "detail": exc.errors(include_url=False, include_context=False)}
        )

    if inbox_enabled:
        with conn.transaction():
            stored = _stored(conn, schema, command.command_id)
        if stored is not None:
            return stored

    prepared = spec.prepare(command, payload) if spec.prepare is not None else None

    with conn.transaction():
        if inbox_enabled and not inbox.register(conn, schema, command.command_id, schema=schema, kind="command"):
            stored = _stored(conn, schema, command.command_id)
            if stored is None:  # no debería ocurrir: la fila y su respuesta se confirman juntas
                raise RuntimeError(f"comando {command.command_id} registrado sin respuesta")
            return stored
        outcome = spec.handle(conn, command, payload, prepared)
        if isinstance(outcome, Accepted):
            _check_causation(command, outcome.events)
            outbox.append(conn, schema, outcome.events)
            response = CommandResponse(ACCEPTED, outcome.body | {"status": "accepted"} | base)
        else:
            response = CommandResponse(REJECTED, {"status": "rejected"} | base | {"reason": outcome.reason})
        if inbox_enabled:
            conn.execute(
                f"UPDATE {qualified(schema, 'inbox')} SET status_code = %s, response = %s WHERE message_id = %s",
                (response.status_code, Jsonb(response.body), command.command_id),
            )
    return response


Executor = Callable[[psycopg.Connection, str, CommandEnvelope, CommandSpec], CommandResponse]


def command_router(
    schema: str,
    specs: Mapping[str, CommandSpec],
    connect: Callable[[], psycopg.Connection],
    *,
    executor: Executor = execute_command,
) -> APIRouter:
    """Router FastAPI con `POST /commands/<Comando>` para los comandos que atiende el sistema."""
    qualified(schema, "inbox")  # valida el nombre del esquema al construir el router
    router = APIRouter()

    @router.post("/commands/{command_type}")
    def post_command(command_type: str, request: Request, body: dict[str, Any] = Body(...)) -> JSONResponse:  # noqa: B008
        spec = specs.get(command_type)
        if spec is None:
            return JSONResponse({"status": "unknown_command", "command_type": command_type}, status_code=404)
        try:
            command = CommandEnvelope.model_validate(body | {"command_type": command_type})
        except ValidationError as exc:
            return JSONResponse(
                {"status": "rejected", "reason": "invalid_command", "detail": exc.errors(include_url=False,
                                                                                         include_context=False)},
                status_code=REJECTED,
            )
        attributes = {
            "eda.command_id": str(command.command_id),
            "eda.command_type": command_type,
            "eda.incident_id": command.correlation_id,
            "eda.component": schema,
        }
        with tracing.tracer().start_as_current_span(
            f"{command_type} handle",
            context=tracing.extract(dict(request.headers)),
            kind=SpanKind.SERVER,
            attributes=attributes,
        ) as span:
            conn = connect()
            try:
                response = executor(conn, schema, command, spec)
            finally:
                conn.close()
            span.set_attribute("http.response.status_code", response.status_code)
            span.set_attribute("eda.replayed", response.replayed)
        headers = {REPLAYED_HEADER: "true"} if response.replayed else None
        return JSONResponse(response.body, status_code=response.status_code, headers=headers)

    return router
