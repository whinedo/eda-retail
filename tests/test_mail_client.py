"""Cliente MCP del ejecutor de notify_customer y sus guardrails (spec mcp-mail): sin PostgreSQL. El servidor
de correo corre en proceso (cliente en memoria del SDK de MCP) con el buzón en memoria."""

from __future__ import annotations

import logging
import uuid
from dataclasses import replace

import pytest
from mcp.server.mcpserver import MCPServer
from pydantic import BaseModel

from eda_retail.config import Settings
from eda_retail.executor_notify_customer import mail_client
from eda_retail.executor_notify_customer.handler import NotifyCustomer
from eda_retail.executor_notify_customer.mail_client import McpMailClient, SendEmailInput
from eda_retail.executor_notify_customer.mailer import McpMailer, ServiceMailer
from eda_retail.infra.backoff import Backoff
from eda_retail.infra.commands import Accepted, CommandEnvelope
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.mail_server.directory import address_for
from eda_retail.mail_server.server import build_server
from eda_retail.mail_server.store import MemoryMailStore
from eda_retail.queries.cost_per_incident import budget_from_cost
from eda_retail.rules import BusinessRules
from eda_retail.step_executors import component, runtime
from eda_retail.step_executors.actions import build_action
from eda_retail.step_executors.services import FakeService

RULES = BusinessRules()
NOTICE = ("Hemos sentido mucho el retraso de tu pedido. El transportista nos ha avisado y "
          "te escribiremos en cuanto tengamos noticias de la entrega.")
CUSTOMER = "CUST-0042"
DOWN = "http://127.0.0.1:1/mcp"  # puerto cerrado: el servidor no responde


class FakeConn:
    """Conexión falsa: guarda las sentencias (los INSERT del efecto y de las llamadas al LLM)."""

    def __init__(self) -> None:
        self.statements: list[tuple[str, tuple]] = []

    def execute(self, sql: str, params: tuple = ()) -> None:
        self.statements.append((sql, params))


def _step(**extra) -> runtime.ExecuteStep:
    return runtime.ExecuteStep(incident_id="I-1", plan_version=1, step_id="S1", action="notify_customer",
                               params={"order_id": "P-1"}, idempotency_key=extra.pop("key", "K-S1"),
                               cause="delay", estimated_delay_hours=24, **extra)


def _command(step: runtime.ExecuteStep) -> CommandEnvelope:
    return CommandEnvelope(command_id=uuid.uuid4(), command_type="ExecuteStep", correlation_id=step.incident_id,
                           causation_id=uuid.uuid4(), payload=step.model_dump())


def _store() -> MemoryMailStore:
    return MemoryMailStore({CUSTOMER: address_for(CUSTOMER)})


def _mailer(server, *, sent: int = 0, max_emails: int = 3) -> McpMailer:
    return McpMailer(McpMailClient(server, timeout=2), customer_of=lambda order_id: CUSTOMER,
                     sent_count=lambda incident_id: sent, max_emails=max_emails)


def _action(mailer, llm=None) -> NotifyCustomer:
    return build_action("notify_customer", llm=llm or FakeLlmClient([{"text": NOTICE}] * 3), rules=RULES,
                        service=None, order_known=lambda order_id: True,
                        budget=lambda incident_id: budget_from_cost(None, RULES), mailer=mailer)


def _run(action, step=None, *, stored=None, sleeps=None):
    """El trabajo del ejecutor: prepare (fuera de la transacción) y handle (con una conexión falsa)."""
    step = step or _step()
    command = _command(step)
    prepared = runtime.StepPreparer(action, completed=lambda key: stored, max_attempts=3,
                                    backoff=Backoff(0.5, 10, seed=1),
                                    sleep=(sleeps if sleeps is not None else []).append)(command, step)
    settings = replace(Settings(), effect_keys_enabled=False)
    outcome = runtime.execute_step(action, settings)(FakeConn(), command, step, prepared)
    assert isinstance(outcome, Accepted)
    return prepared, outcome.events


def _types(events) -> list[str]:
    return [e.event_type for e in events]


def _failed(events) -> dict:
    return next(e.payload for e in events if e.event_type == "StepFailed")


# --- El ejecutor de notify_customer es el cliente MCP -----------------------------------------


def test_notification_step_completed():
    """Paso de notificación completado"""
    store = _store()
    _prepared, events = _run(_action(_mailer(build_server(store))))
    [mail] = store.mailbox
    assert mail.idempotency_key == "K-S1" and mail.body == NOTICE
    assert _types(events) == ["LlmCallRecorded", "CustomerNotified", "StepCompleted"]
    notified = events[1].payload
    assert notified == {"order_id": "P-1", "message_id": mail.message_id}
    assert address_for(CUSTOMER) not in str([e.payload for e in events])


def test_mail_server_down():
    """Servidor de correo caído"""
    sleeps: list[float] = []
    prepared, events = _run(_action(_mailer(DOWN)), sleeps=sleeps)
    assert prepared.attempts == 3 and len(sleeps) == 2  # espera exponencial entre intentos
    assert _types(events)[-1] == "StepFailed"
    assert _failed(events) == {"step_id": "S1", "plan_version": 1, "error": "mail_unavailable",
                               "retryable": True, "attempts": 3}


def test_retry_after_sending_the_mail():
    """Reintento tras enviar el correo"""
    store = _store()
    action = _action(_mailer(build_server(store)))
    first, _ = _run(action)  # el worker cae antes de confirmar su transacción: effect_keys sin la clave
    again, events = _run(action)
    assert again.external_id == first.external_id
    assert [m.idempotency_key for m in store.mailbox] == ["K-S1"]
    assert "CustomerNotified" in _types(events)


def test_a_completed_key_does_not_call_the_mail_server():
    """effect_keys sigue mandando: un paso ya completado no vuelve a invocar send_email"""
    store = _store()
    _prepared, events = _run(_action(_mailer(build_server(store))), stored={"message_id": "M-1"})
    assert store.mailbox == []
    assert _types(events) == ["StepCompleted"]


def test_unknown_customer_is_not_retried():
    """Cliente desconocido: StepFailed no reintentable al primer intento"""
    mailer = McpMailer(McpMailClient(build_server(_store())), customer_of=lambda order_id: "CUST-9999",
                       sent_count=lambda incident_id: 0, max_emails=3)
    prepared, events = _run(_action(mailer))
    assert (_failed(events)["error"], _failed(events)["retryable"], prepared.attempts) == (
        "unknown_customer", False, 1)


# --- Guardrails del cliente MCP --------------------------------------------------------------


def _server_with(tool) -> tuple[MCPServer, dict]:
    calls = {"delete_mailbox": 0}
    server = MCPServer("mail-server")
    server.tool(name="send_email")(tool)

    @server.tool()
    def delete_mailbox() -> str:
        calls["delete_mailbox"] += 1
        return "deleted"

    return server, calls


def test_tool_not_allowed():
    """Herramienta no permitida"""
    store = _store()
    server, calls = _server_with(build_server(store)._tool_manager.get_tool("send_email").fn)
    client = McpMailClient(server)
    mailer = McpMailer(client, customer_of=lambda order_id: CUSTOMER, sent_count=lambda incident_id: 0,
                       max_emails=3)
    _, events = _run(_action(mailer))
    assert client.tools == {"send_email"}
    assert "CustomerNotified" in _types(events)
    assert calls["delete_mailbox"] == 0
    public = [name for name in dir(client) if not name.startswith("_")]
    assert sorted(public) == ["send_email", "tools"]  # ninguna ruta para invocar otra herramienta


def test_a_server_without_the_expected_send_email_is_rejected(caplog):
    """Lista blanca: send_email con otra entrada no es invocable"""

    def send_email(customer_ref: str, subject: str, body: str, idempotency_key: str, bcc: str) -> str:
        raise AssertionError("no debe invocarse")

    server, _ = _server_with(send_email)
    client = McpMailClient(server)
    with caplog.at_level(logging.WARNING):
        _, events = _run(_action(McpMailer(client, customer_of=lambda o: CUSTOMER, sent_count=lambda i: 0,
                                           max_emails=3)))
    assert client.tools == frozenset()
    assert (_failed(events)["error"], _failed(events)["retryable"]) == ("mcp_tool_not_allowed", False)
    assert "guardrail MCP mcp_tool_not_allowed" in caplog.text


class NoMessageId(BaseModel):
    accepted_at: str


class WithAddress(BaseModel):
    message_id: str
    accepted_at: str
    to: str


def test_output_out_of_schema(caplog):
    """Salida fuera de esquema"""

    def send_email(customer_ref: str, subject: str, body: str, idempotency_key: str) -> NoMessageId:
        return NoMessageId(accepted_at="2026-11-15T10:00:00Z")

    server, _ = _server_with(send_email)
    with caplog.at_level(logging.WARNING):
        prepared, events = _run(_action(_mailer(server)))
    assert (_failed(events)["error"], _failed(events)["retryable"], prepared.attempts) == (
        "mcp_output_invalid", False, 1)
    assert "CustomerNotified" not in _types(events)
    assert "guardrail MCP mcp_output_invalid" in caplog.text and "trace_id=" in caplog.text


def test_too_many_mails_for_an_incident(caplog):
    """Demasiados correos para una incidencia"""
    store = _store()
    llm = FakeLlmClient([{"text": NOTICE}])
    with caplog.at_level(logging.WARNING):
        _prepared, events = _run(_action(_mailer(build_server(store), sent=3, max_emails=3), llm))
    assert store.mailbox == [] and llm.requests == []  # ni send_email ni el redactor
    assert _types(events) == ["StepFailed"]
    assert (_failed(events)["error"], _failed(events)["retryable"]) == ("mail_limit_exceeded", False)
    assert "guardrail MCP mail_limit_exceeded" in caplog.text


def test_address_in_the_response(caplog):
    """Dirección en la respuesta"""
    address = address_for(CUSTOMER)

    def send_email(customer_ref: str, subject: str, body: str, idempotency_key: str) -> WithAddress:
        return WithAddress(message_id="M-1", accepted_at="2026-11-15T10:00:00Z", to=address)

    server, _ = _server_with(send_email)
    with caplog.at_level(logging.DEBUG, logger="eda_retail"):
        prepared, events = _run(_action(_mailer(server)))
    assert (_failed(events)["error"], _failed(events)["retryable"]) == ("mcp_output_invalid", False)
    assert prepared.external_id is None
    assert address not in caplog.text
    assert address not in str([e.payload for e in events])


def test_invalid_input_is_not_sent():
    """Validación de la entrada: un correo fuera de esquema no se envía"""
    store = _store()
    client = McpMailClient(build_server(store))
    with pytest.raises(mail_client.BusinessError) as raised:
        client.send_email(SendEmailInput.model_construct(customer_ref=CUSTOMER, subject="", body="x",
                                                         idempotency_key="K"))
    assert raised.value.error == "mcp_input_invalid" and store.mailbox == []


# --- Versión de la PoC -------------------------------------------------------------------------


@pytest.fixture
def no_mcp(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError("la versión 1 no conecta con MCP")

    monkeypatch.setattr(component, "McpMailClient", forbidden)


def test_version_one_keeps_the_in_process_mail_service(no_mcp):
    """POC_VERSION=1: notify_customer usa el servicio simulado en proceso y nunca conecta con MCP"""
    settings = replace(Settings(), poc_version=1)
    assert component.build_mailer(settings, "notify_customer") is None
    component.specs(settings, "notify_customer", FakeLlmClient([]), RULES)  # el cableado no crea el cliente


def test_version_one_flow_is_unchanged(no_mcp):
    """POC_VERSION=1: el paso llama al servicio simulado con la clave y CustomerNotified lleva su identificador"""
    mail = FakeService("mail", "M")
    action = build_action("notify_customer", llm=FakeLlmClient([{"text": NOTICE}]), rules=RULES, service=mail,
                          order_known=lambda order_id: True, budget=lambda incident_id: budget_from_cost(None, RULES))
    assert isinstance(action.mailer, ServiceMailer)
    prepared, events = _run(action)
    assert mail.executions == {"K-S1": 1}
    assert events[1].payload == {"order_id": "P-1", "message_id": prepared.external_id}
    assert prepared.external_id.startswith("M-")


def test_version_two_uses_the_mcp_client():
    """POC_VERSION=2: notify_customer usa el cliente MCP con sus guardrails; el resto de acciones no cambia"""
    settings = replace(Settings(), poc_version=2, mcp_max_emails_per_incident=5)
    mailer = component.build_mailer(settings, "notify_customer")
    assert isinstance(mailer, McpMailer) and isinstance(mailer.client, McpMailClient)
    assert mailer._max_emails == 5
    assert component.build_mailer(settings, "file_claim") is None
