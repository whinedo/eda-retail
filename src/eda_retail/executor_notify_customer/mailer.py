"""Cómo envía el correo el ejecutor de `notify_customer`, según la versión de la PoC.

- `ServiceMailer` (`POC_VERSION=1`): el servicio de correo simulado en proceso (`FakeService`), como en la
  primera versión. No conecta con MCP.
- `McpMailer` (`POC_VERSION=2`): el servidor de correo MCP (spec mcp-mail). Antes de redactar comprueba el
  límite de correos de la incidencia (`MCP_MAX_EMAILS_PER_INCIDENT`, contados en su propio esquema); al
  enviar, resuelve el cliente del pedido en `view_orders` y llama a `send_email` con la `idempotency_key`
  del paso.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from eda_retail.executor_notify_customer.mail_client import (
    MAIL_LIMIT_EXCEEDED,
    McpMailClient,
    SendEmailInput,
    guardrail_failed,
)
from eda_retail.step_executors.errors import BusinessError, EffectError
from eda_retail.step_executors.runtime import ExecuteStep
from eda_retail.step_executors.services import FakeService

ORDER_NOT_FOUND = "order_not_found"


class Mailer(Protocol):
    def check(self, step: ExecuteStep) -> EffectError | None:
        """Lo que impide enviar el correo del paso antes de redactarlo, o None."""

    def send(self, step: ExecuteStep, text: str) -> str:
        """Envía el correo y devuelve el `message_id`. EffectError si no se pudo."""


class ServiceMailer:
    def __init__(self, service: FakeService) -> None:
        self.service = service

    def check(self, step: ExecuteStep) -> EffectError | None:
        return None

    def send(self, step: ExecuteStep, text: str) -> str:
        return self.service.call(step.idempotency_key)


def subject_for(order_id: str) -> str:
    return f"Incidencia en el envío de su pedido {order_id}"


class McpMailer:
    def __init__(self, client: McpMailClient, *, customer_of: Callable[[str], str | None],
                 sent_count: Callable[[str], int], max_emails: int) -> None:
        self.client = client
        self._customer_of = customer_of
        self._sent_count = sent_count
        self._max_emails = max_emails

    def check(self, step: ExecuteStep) -> EffectError | None:
        sent = self._sent_count(step.incident_id)
        if sent >= self._max_emails:
            guardrail_failed(MAIL_LIMIT_EXCEEDED, f"la incidencia {step.incident_id} ya recibió {sent} correos")
            return BusinessError(MAIL_LIMIT_EXCEEDED)
        return None

    def send(self, step: ExecuteStep, text: str) -> str:
        order_id = str(step.params.get("order_id"))
        customer_ref = self._customer_of(order_id)
        if customer_ref is None:
            raise BusinessError(ORDER_NOT_FOUND)
        request = SendEmailInput.model_construct(customer_ref=customer_ref, subject=subject_for(order_id), body=text,
                                                 idempotency_key=step.idempotency_key)
        return self.client.send_email(request).message_id
