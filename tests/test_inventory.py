"""Command handler de `inventory`: lo que se decide antes de tocar su estado (T4)."""

from __future__ import annotations

import uuid

from eda_retail.config import Settings
from eda_retail.infra.commands import CommandEnvelope, Rejected
from eda_retail.inventory import handler


class _Conn:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, sql, params=()):
        self.statements.append(sql)
        raise AssertionError("no debe tocar la base de datos")


def _execute_step(action: str, *, correlation_id: str = "I-1") -> tuple[_Conn, object]:
    payload = {"incident_id": "I-1", "plan_version": 1, "step_id": action, "action": action,
               "params": {"order_id": "P-1001"}, "idempotency_key": "k1"}
    command = CommandEnvelope(command_id=uuid.uuid4(), command_type="ExecuteStep", correlation_id=correlation_id,
                              causation_id=uuid.uuid4(), payload=payload)
    spec = handler.specs(Settings())["ExecuteStep"]
    conn = _Conn()
    return conn, spec.handle(conn, command, spec.payload_model.model_validate(payload), None)


def test_inventory_attends_seed_sample_data_and_execute_step():
    assert set(handler.specs(Settings())) == {"SeedSampleData", "ExecuteStep"}


def test_execute_step_of_another_action_is_rejected():
    """Enrutado por acción: inventory solo atiende reserve_stock y release_stock"""
    conn, outcome = _execute_step("file_claim")
    assert outcome == Rejected("action_not_handled")
    assert conn.statements == []


def test_execute_step_must_belong_to_the_incident_it_is_correlated_with():
    conn, outcome = _execute_step("reserve_stock", correlation_id="I-2")
    assert outcome == Rejected("incident_mismatch")
    assert conn.statements == []


def test_a_compensation_releases_the_reservation_of_its_original_step():
    assert handler.original_step("reserve_stock-comp") == "reserve_stock"
    assert handler.original_step("s2-comp") == "s2"
    assert handler.original_step("s2") == "s2"
