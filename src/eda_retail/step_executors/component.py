"""El command handler `ExecuteStep` de un ejecutor (con trabajo largo: 202 al aceptar) y las consultas de su
`prepare`."""

from __future__ import annotations

from collections.abc import Callable

from eda_retail.config import Settings
from eda_retail.infra import effect_keys
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.commands import CommandSpec
from eda_retail.infra.db import connect
from eda_retail.llm.generation import Budget, LlmClient
from eda_retail.queries.cost_per_incident import Prices, budget_from_cost, get_incident_cost
from eda_retail.queries.orders import get_orders
from eda_retail.rules import BusinessRules
from eda_retail.step_executors.actions import build_action
from eda_retail.step_executors.runtime import ExecuteStep, RecordingAction, StepPreparer, accept_step, execute_step
from eda_retail.step_executors.services import FakeService


def _with_conn(settings: Settings, query: Callable):
    def run(*args):
        conn = connect(settings.database_url)
        try:
            return query(conn, *args)
        finally:
            conn.close()

    return run


def specs(settings: Settings, action_name: str, llm: LlmClient, rules: BusinessRules, *,
          service: FakeService | None = None) -> dict[str, CommandSpec]:
    """`ExecuteStep` de la acción; consulta `view_orders` y `cost_per_incident` (lado de lectura)."""
    prices = Prices.from_settings(settings)
    order_known = _with_conn(settings, lambda conn, order_id: order_id in get_orders(conn, [order_id]).found)
    budget: Callable[[str], Budget] = _with_conn(
        settings, lambda conn, incident_id: budget_from_cost(get_incident_cost(conn, incident_id, prices), rules))
    action: RecordingAction = build_action(action_name, llm=llm, rules=rules, service=service,
                                           order_known=order_known, budget=budget)
    completed = _with_conn(settings, lambda conn, key: effect_keys.stored_result(conn, key, schema=action.schema))
    prepare = StepPreparer(action, completed=completed, max_attempts=settings.step_max_attempts,
                           backoff=Backoff(settings.step_backoff_base_seconds, settings.step_backoff_max_seconds))
    return {"ExecuteStep": CommandSpec(ExecuteStep, execute_step(action, settings), prepare=prepare, deferred=True,
                                       accept=accept_step(action))}


def url(settings: Settings, action_name: str) -> str:
    return getattr(settings, f"executor_{action_name}_url")
