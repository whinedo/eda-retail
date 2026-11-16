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
    """El reembolso ya registrado para el paso (`refunds (incident_id, step_id)`, design.md §8), aunque la
    `idempotency_key` sea otra: el ejecutor lo devuelve en lugar de reembolsar otra vez."""
    row = conn.execute("SELECT refund_id FROM executor_issue_refund.refunds WHERE incident_id = %s AND step_id = %s",
                       (step.incident_id, step.step_id)).fetchone()
    return None if row is None else {"refund_id": row["refund_id"]}
