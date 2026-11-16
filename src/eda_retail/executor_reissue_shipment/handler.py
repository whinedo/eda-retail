"""Ejecutor de `reissue_shipment` (T9): el reenvío del transportista en `shipments`."""

from __future__ import annotations

from typing import Any

import psycopg

from eda_retail.infra.commands import CommandEnvelope
from eda_retail.llm.generation import LlmCall
from eda_retail.step_executors.runtime import ExecuteStep, Prepared
from eda_retail.step_executors.simple import Simple

SCHEMA = "executor_reissue_shipment"


class ReissueShipment(Simple):
    action = "reissue_shipment"
    schema = SCHEMA
    effect_event = "ShipmentReissued"

    def record(self, conn: psycopg.Connection, step: ExecuteStep,
               prepared: Prepared) -> tuple[dict[str, Any], dict[str, Any]]:
        order_id, shipment_id = step.params["order_id"], prepared.external_id
        conn.execute(
            "INSERT INTO executor_reissue_shipment.shipments (shipment_id, incident_id, order_id, step_id)"
            " VALUES (%s, %s, %s, %s)",
            (shipment_id, step.incident_id, order_id, step.step_id),
        )
        return {"new_shipment_id": shipment_id}, {"order_id": order_id, "new_shipment_id": shipment_id}

    def record_calls(self, conn: psycopg.Connection, command: CommandEnvelope, calls: list[LlmCall]) -> None:
        pass  # sin LLM
