"""Ejecutor de `file_claim` (T9): la reclamación, redactada por el LLM, en `claims`."""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.infra.commands import CommandEnvelope
from eda_retail.llm.generation import LlmCall
from eda_retail.step_executors.drafted import Drafted
from eda_retail.step_executors.runtime import ExecuteStep, Prepared

SCHEMA = "executor_file_claim"


class FileClaim(Drafted):
    action = "file_claim"
    kind = "claim"
    schema = SCHEMA
    effect_event = "ClaimFiled"

    def record(self, conn: psycopg.Connection, step: ExecuteStep,
               prepared: Prepared) -> tuple[dict[str, Any], dict[str, Any]]:
        order_id, claim_id = step.params["order_id"], prepared.external_id
        conn.execute(
            "INSERT INTO executor_file_claim.claims (claim_id, incident_id, order_id, step_id, text)"
            " VALUES (%s, %s, %s, %s, %s)",
            (claim_id, step.incident_id, order_id, step.step_id, prepared.text),
        )
        return {"claim_id": claim_id}, {"order_id": order_id, "claim_id": claim_id}

    def record_calls(self, conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> None:
        record_llm_calls(conn, command, calls)


def record_llm_calls(conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> None:
    for call in calls:
        conn.execute(
            "INSERT INTO executor_file_claim.llm_calls (incident_id, command_id, model, tokens_in, tokens_out,"
            " latency_ms, validation_passed, attempt) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            (command.correlation_id, command.command_id, call.model, call.tokens_in, call.tokens_out,
             call.latency_ms, call.validation_passed, call.attempt),
        )
