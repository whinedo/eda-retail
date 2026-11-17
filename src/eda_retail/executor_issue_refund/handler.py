"""Ejecutor de `issue_refund` (T9): el reembolso en `refunds`, con el tope de importe en código."""

from __future__ import annotations

from decimal import Decimal
from typing import Any

import psycopg

from eda_retail.infra.commands import CommandEnvelope
from eda_retail.llm.generation import LlmCall
from eda_retail.step_executors.runtime import ExecuteStep, Prepared
from eda_retail.step_executors.simple import Refund

SCHEMA = "executor_issue_refund"


class IssueRefund(Refund):
    action = "issue_refund"
    schema = SCHEMA
    effect_event = "RefundIssued"

    def record(self, conn: psycopg.Connection, step: ExecuteStep,
               prepared: Prepared) -> tuple[dict[str, Any], dict[str, Any]]:
        order_id, refund_id = step.params["order_id"], prepared.external_id
        amount = Decimal(str(step.params["amount"]))
        conn.execute(
            "INSERT INTO executor_issue_refund.refunds (refund_id, incident_id, order_id, step_id, amount)"
            " VALUES (%s, %s, %s, %s, %s)",
            (refund_id, step.incident_id, order_id, step.step_id, amount),
        )
        return {"refund_id": refund_id}, {"order_id": order_id, "amount": str(amount), "refund_id": refund_id}

    def record_calls(self, conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> None:
        pass  # sin LLM


def existing_refund(conn: psycopg.Connection, step: ExecuteStep) -> dict[str, Any] | None:
    """El reembolso ya registrado para el paso o para el mismo pedido de la incidencia (spec idempotency, «Reservas,
    reenvíos y reembolsos sin duplicar»), aunque el `step_id` o la `idempotency_key` sean otros: una revisión del
    plan puede volver a pedir `issue_refund`, y el ejecutor devuelve el existente en lugar de reembolsar otra vez."""
    row = conn.execute(
        "SELECT refund_id FROM executor_issue_refund.refunds WHERE incident_id = %s AND (step_id = %s OR order_id = %s)"
        " ORDER BY created_at LIMIT 1",
        (step.incident_id, step.step_id, str(step.params.get("order_id"))),
    ).fetchone()
    return None if row is None else {"refund_id": row["refund_id"]}
