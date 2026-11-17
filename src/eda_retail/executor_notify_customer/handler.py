"""Ejecutor de `notify_customer` (T9): el aviso al cliente, redactado por el LLM, en `customer_messages`.

El correo lo envía su `Mailer`: el servicio simulado en proceso (versión 1) o el servidor de correo MCP
(versión 2, spec mcp-mail). `CustomerNotified` lleva el `message_id` del envío y ningún dato personal.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

import psycopg

from eda_retail.executor_notify_customer.mailer import Mailer
from eda_retail.infra.commands import CommandEnvelope
from eda_retail.llm.generation import Budget, LlmCall
from eda_retail.llm.prompts.writer import Writer
from eda_retail.step_executors.drafted import Drafted
from eda_retail.step_executors.runtime import ExecuteStep, Preparation, Prepared

SCHEMA = "executor_notify_customer"


class NotifyCustomer(Drafted):
    action = "notify_customer"
    kind = "customer_notice"
    schema = SCHEMA
    effect_event = "CustomerNotified"

    def __init__(self, writer: Writer, mailer: Mailer, *, order_known: Callable[[str], bool],
                 budget: Callable[[str], Budget]) -> None:
        super().__init__(writer, None, order_known=order_known, budget=budget)
        self.mailer = mailer

    def prepare(self, step: ExecuteStep) -> Preparation:
        if (error := self.mailer.check(step)) is not None:  # sin redactar: no se pagará el LLM
            return Preparation(error=error)
        return super().prepare(step)

    def call(self, step: ExecuteStep, preparation: Preparation) -> str:
        return self.mailer.send(step, str(preparation.text))

    def record(self, conn: psycopg.Connection, step: ExecuteStep,
               prepared: Prepared) -> tuple[dict[str, Any], dict[str, Any]]:
        order_id, message_id = step.params["order_id"], prepared.external_id
        conn.execute(
            "INSERT INTO executor_notify_customer.customer_messages"
            " (message_id, incident_id, order_id, step_id, plan_version, text) VALUES (%s, %s, %s, %s, %s, %s)",
            (message_id, step.incident_id, order_id, step.step_id, step.plan_version, prepared.text),
        )
        return {"message_id": message_id}, {"order_id": order_id, "message_id": message_id}

    def record_calls(self, conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> None:
        record_llm_calls(conn, command, calls)


def sent_count(conn: psycopg.Connection, incident_id: str) -> int:
    """Correos ya enviados a la incidencia (límite `MCP_MAX_EMAILS_PER_INCIDENT`), en su propio esquema."""
    row = conn.execute("SELECT count(*) AS n FROM executor_notify_customer.customer_messages WHERE incident_id = %s",
                       (incident_id,)).fetchone()
    return int(row["n"])


def existing_message(conn: psycopg.Connection, step: ExecuteStep) -> dict[str, Any] | None:
    """El aviso ya registrado para el paso o para el mismo pedido en la misma versión del plan (spec idempotency,
    «Un aviso por cliente y pedido»), aunque el `step_id` o la `idempotency_key` sean otros:
    el ejecutor lo devuelve en lugar de enviar otro correo. `plan_version` es la versión con la que la saga pidió
    el paso."""
    row = conn.execute(
        "SELECT message_id FROM executor_notify_customer.customer_messages WHERE incident_id = %s"
        " AND (step_id = %s OR (order_id = %s AND plan_version = %s)) ORDER BY created_at LIMIT 1",
        (step.incident_id, step.step_id, str(step.params.get("order_id")), step.plan_version),
    ).fetchone()
    return None if row is None else {"message_id": row["message_id"]}


def record_llm_calls(conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> None:
    for call in calls:
        conn.execute(
            "INSERT INTO executor_notify_customer.llm_calls (incident_id, command_id, model, tokens_in, tokens_out,"
            " latency_ms, validation_passed, attempt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (command.correlation_id, command.command_id, call.model, call.tokens_in, call.tokens_out,
             call.latency_ms, call.validation_passed, call.attempt),
        )
