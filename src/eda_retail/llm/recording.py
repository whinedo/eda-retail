"""Registro común de las llamadas al LLM (spec agent-harness, «Registro común de las llamadas con la versión del
prompt»): la fila de `<esquema>.llm_calls` y el `LlmCallRecorded`, en la transacción del componente.

El evento no lleva el prompt renderizado ni la salida: solo el agente, el modelo, los tokens, la latencia, si pasó la
validación, el intento y la versión y la huella del prompt.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import psycopg

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra.commands import CommandEnvelope
from eda_retail.llm.generation import LlmCall

# Esquemas con tabla `llm_calls`: los de los cuatro agentes (el redactor, en los dos ejecutores que lo usan).
LLM_CALL_SCHEMAS = ("interpreter", "planner", "replanner", "executor_notify_customer", "executor_file_claim")


def llm_call_payload(incident_id: str, call: LlmCall) -> dict[str, Any]:
    return {
        "incident_id": incident_id, "agent": call.agent, "model": call.model,
        "tokens_in": call.tokens_in, "tokens_out": call.tokens_out, "latency_ms": call.latency_ms,
        "validation_passed": call.validation_passed, "attempt": call.attempt,
        "prompt_version": call.prompt_version, "prompt_hash": call.prompt_hash,
    }


def insert_llm_calls(conn: psycopg.Connection, schema: str, command: CommandEnvelope, calls: list[LlmCall]) -> None:
    if schema not in LLM_CALL_SCHEMAS:
        raise ValueError(f"el esquema {schema!r} no tiene llm_calls")
    for call in calls:
        conn.execute(
            f"INSERT INTO {schema}.llm_calls (incident_id, command_id, model, tokens_in, tokens_out, latency_ms,"
            " validation_passed, attempt, prompt_version, prompt_hash)"
            " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (command.correlation_id, command.command_id, call.model, call.tokens_in, call.tokens_out,
             call.latency_ms, call.validation_passed, call.attempt, call.prompt_version, call.prompt_hash),
        )


def record_llm_calls(conn: psycopg.Connection, schema: str, command: CommandEnvelope, calls: list[LlmCall],
                     event: Callable[[str, dict[str, Any]], EventEnvelope]) -> list[EventEnvelope]:
    """Escribe las filas y devuelve un `LlmCallRecorded` por llamada para el outbox del componente."""
    insert_llm_calls(conn, schema, command, calls)
    return [event("LlmCallRecorded", llm_call_payload(command.correlation_id, call)) for call in calls]
