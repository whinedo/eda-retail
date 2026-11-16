"""El command handler `ExecuteStep` de un ejecutor (con trabajo largo: 202 al aceptar) y las consultas de su
`prepare`."""

from __future__ import annotations

from collections.abc import Callable

from mcp.server.mcpserver import MCPServer

from eda_retail.config import Settings
from eda_retail.executor_file_claim.handler import existing_claim
from eda_retail.executor_issue_refund.handler import existing_refund
from eda_retail.executor_notify_customer.handler import existing_message, sent_count
from eda_retail.executor_notify_customer.mail_client import McpMailClient
from eda_retail.executor_notify_customer.mailer import Mailer, McpMailer
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

EXISTING_EFFECT: dict[str, Callable] = {
    "file_claim": existing_claim,
    "notify_customer": existing_message,
    "issue_refund": existing_refund,
}


def _with_conn(settings: Settings, query: Callable):
    def run(*args):
        conn = connect(settings.database_url)
        try:
            return query(conn, *args)
        finally:
            conn.close()

    return run


def _customer_of(conn, order_id: str) -> str | None:
    order = get_orders(conn, [order_id]).found.get(order_id)
    return order.customer_ref if order is not None else None


def build_mailer(settings: Settings, action_name: str, *, mail_server: str | MCPServer | None = None) -> Mailer | None:
    """El cliente MCP del servidor de correo, solo para `notify_customer` con `POC_VERSION=2`. Con la versión 1
    devuelve None: la acción usa el servicio simulado en proceso y nunca conecta con MCP.

    `mail_server`: el servidor (URL o `MCPServer` en proceso, en el arnés); por defecto `MAIL_SERVER_URL`."""
    if action_name != "notify_customer" or settings.poc_version != 2:
        return None
    client = McpMailClient(mail_server or settings.mail_server_url, timeout=settings.command_http_timeout_s)
    return McpMailer(client, customer_of=_with_conn(settings, _customer_of),
                     sent_count=_with_conn(settings, sent_count),
                     max_emails=settings.mcp_max_emails_per_incident)


def specs(settings: Settings, action_name: str, llm: LlmClient, rules: BusinessRules, *,
          service: FakeService | None = None, mail_server: str | MCPServer | None = None) -> dict[str, CommandSpec]:
    """`ExecuteStep` de la acción; consulta `view_orders` y `cost_per_incident` (lado de lectura)."""
    prices = Prices.from_settings(settings)
    order_known = _with_conn(settings, lambda conn, order_id: order_id in get_orders(conn, [order_id]).found)
    budget: Callable[[str], Budget] = _with_conn(
        settings, lambda conn, incident_id: budget_from_cost(get_incident_cost(conn, incident_id, prices), rules))
    action: RecordingAction = build_action(action_name, llm=llm, rules=rules, service=service,
                                           order_known=order_known, budget=budget,
                                           mailer=build_mailer(settings, action_name, mail_server=mail_server))
    completed = _with_conn(settings, lambda conn, key: effect_keys.stored_result(conn, key, schema=action.schema))
    # Clave de negocio del efecto (design.md §8): una reclamación por incidencia y pedido (spec idempotency), y un
    # aviso y un reembolso por paso. Es parte de la idempotencia de los efectos, así que se apaga con
    # EFFECT_KEYS_ENABLED=false (E1 sin idempotencia).
    lookup = EXISTING_EFFECT.get(action_name)
    existing = _with_conn(settings, lookup) if lookup is not None and settings.effect_keys_enabled else None
    prepare = StepPreparer(action, completed=completed, max_attempts=settings.step_max_attempts,
                           backoff=Backoff(settings.step_backoff_base_seconds, settings.step_backoff_max_seconds),
                           existing=existing)
    return {"ExecuteStep": CommandSpec(ExecuteStep, execute_step(action, settings), prepare=prepare, deferred=True,
                                       accept=accept_step(action))}


def url(settings: Settings, action_name: str) -> str:
    return getattr(settings, f"executor_{action_name}_url")
