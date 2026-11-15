"""Comandos con trabajo largo de cada componente (odd/tasks/eda-async-commands.md S1; design.md §4.4).

`CreatePlan`, `RevisePlan` y el `ExecuteStep` de los cuatro ejecutores responden 202 al aceptar y hacen el
trabajo (LLM, servicio externo) en el worker; sus invariantes sin LLM se comprueban en la aceptación (422).
`inventory` sigue respondiendo con el resultado en la petición: no hace E/S externa. Sin PostgreSQL.
"""

from __future__ import annotations

import importlib.util
import pathlib
import uuid

import pytest

from eda_retail.config import Settings
from eda_retail.infra import schemas
from eda_retail.infra.commands import CommandEnvelope, Rejected
from eda_retail.inventory import handler as inventory
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.planner import handler as planner
from eda_retail.replanner import handler as replanner
from eda_retail.rules import BusinessRules
from eda_retail.step_executors import component as executors
from eda_retail.step_executors.runtime import ExecuteStep

RULES = BusinessRules()
ROOT = pathlib.Path(__file__).parents[1]
EXECUTOR_ACTIONS = ("notify_customer", "file_claim", "reissue_shipment", "issue_refund")


class _Row:
    def __init__(self, row):
        self._row = row

    def fetchone(self):
        return self._row


class _Conn:
    """Conexión falsa: responde `found` a la consulta de aceptación y guarda las sentencias."""

    def __init__(self, found: bool) -> None:
        self.found = found
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, sql, params=()):
        self.statements.append((" ".join(sql.split()), params))
        return _Row({"found": 1} if self.found else None)


def _command(command_type: str, correlation_id: str, payload: dict) -> CommandEnvelope:
    return CommandEnvelope(command_id=uuid.uuid4(), command_type=command_type, correlation_id=correlation_id,
                           causation_id=uuid.uuid4(), payload=payload)


def _create_plan():
    spec = planner.specs(Settings(), FakeLlmClient([]), RULES)["CreatePlan"]
    command = _command("CreatePlan", "I-1", {"order_ids": ["P-1"], "cause": "loss", "estimated_delay_hours": 24,
                                             "severity": "high"})
    return spec, command, spec.payload_model.model_validate(command.payload)


def _revise_plan():
    spec = replanner.specs(Settings(), FakeLlmClient([]), RULES)["RevisePlan"]
    command = _command("RevisePlan", "I-1", {
        "plan_version": 2, "revisions": 1, "failed_step_id": "S3", "error": "out_of_stock",
        "steps": [{"step_id": "S3", "action": "reserve_stock", "params": {"order_id": "P-1"}, "depends_on": [],
                   "status": "failed", "origin_plan_version": 1}]})
    return spec, command, spec.payload_model.model_validate(command.payload)


def test_create_plan_is_deferred_and_rejects_an_incident_already_planned_or_being_planned():
    """CreatePlan: 202 sin esperar al LLM; plan_exists en la aceptación si hay plan o un trabajo abierto"""
    spec, command, payload = _create_plan()
    assert spec.deferred and spec.prepare is not None

    busy = _Conn(found=True)
    assert spec.accept(busy, command, payload) == Rejected("plan_exists")
    [(sql, params)] = busy.statements
    assert "planner.plans" in sql and "planner.command_jobs" in sql and "'pending', 'running'" in sql
    assert params == ("I-1", "I-1")
    assert spec.accept(_Conn(found=False), command, payload) is None


def test_revise_plan_is_deferred_and_rejects_a_version_already_revised_or_being_revised():
    """RevisePlan: 202 sin esperar al LLM; revision_exists en la aceptación si esa versión ya se revisó o se
    está revisando"""
    spec, command, payload = _revise_plan()
    assert spec.deferred and spec.prepare is not None

    busy = _Conn(found=True)
    assert spec.accept(busy, command, payload) == Rejected("revision_exists")
    [(sql, params)] = busy.statements
    assert "replanner.revisions" in sql and "replanner.command_jobs" in sql
    assert params == ("I-1", 2, "I-1", 2)
    assert spec.accept(_Conn(found=False), command, payload) is None


def _execute_step(action: str, **overrides) -> tuple[CommandEnvelope, ExecuteStep]:
    step = {"incident_id": "I-1", "plan_version": 1, "step_id": "S1", "action": action,
            "params": {"order_id": "P-1", "amount": "10.00"}, "idempotency_key": "K-1",
            "cause": "loss", "estimated_delay_hours": 24} | overrides
    command = _command("ExecuteStep", "I-1", step)
    return command, ExecuteStep.model_validate(step)


@pytest.mark.parametrize("action", EXECUTOR_ACTIONS)
def test_execute_step_of_an_executor_is_deferred_and_checks_the_command_on_acceptance(action):
    """ExecuteStep de un ejecutor: 202 sin esperar al redactor ni al servicio; action_not_handled,
    incident_mismatch y missing_context se rechazan en la aceptación, sin tocar la base de datos"""
    spec = executors.specs(Settings(), action, FakeLlmClient([]), RULES)["ExecuteStep"]
    assert spec.deferred and spec.prepare is not None
    conn = _Conn(found=False)

    assert spec.accept(conn, *_execute_step(action)) is None
    other = "issue_refund" if action != "issue_refund" else "notify_customer"
    assert spec.accept(conn, *_execute_step(other)) == Rejected("action_not_handled")
    command, step = _execute_step(action)
    assert spec.accept(conn, command.model_copy(update={"correlation_id": "I-2"}), step) == \
        Rejected("incident_mismatch")
    if action in ("notify_customer", "file_claim"):
        assert spec.accept(conn, *_execute_step(action, cause=None)) == Rejected("missing_context")
    assert conn.statements == []


def test_inventory_execute_step_still_answers_with_its_result():
    """inventory (reserve_stock, release_stock) no hace E/S externa: su ExecuteStep sigue en la petición"""
    specs = inventory.specs(Settings())
    assert not any(spec.deferred for spec in specs.values())


def _migration_components() -> tuple[str, ...]:
    path = ROOT / "migrations" / "versions" / "0019_command_jobs.py"
    module_spec = importlib.util.spec_from_file_location("migration_0019", path)
    module = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(module)  # type: ignore[union-attr]
    return module.COMPONENTS


def test_every_component_with_deferred_commands_has_its_command_jobs_table():
    """Cada componente con comandos de trabajo largo tiene su `command_jobs` (migración 0019)"""
    deferred = {planner.SCHEMA, replanner.SCHEMA, *(f"executor_{a}" for a in EXECUTOR_ACTIONS)}
    assert set(schemas.JOB_COMPONENTS) == deferred == set(_migration_components())
