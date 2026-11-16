"""Servidor MCP de correo (spec mcp-mail, «Servidor de correo simulado»).

Expone una sola herramienta, `send_email(customer_ref, subject, body, idempotency_key)`, que devuelve
`message_id` y `accepted_at`. Resuelve la dirección en su directorio y guarda el correo en su buzón; la
dirección no sale nunca: ni en la salida ni en los logs (que solo llevan `message_id` y `customer_ref`).
Un cliente que no está en el directorio es el error `unknown_customer`, no reintentable; cualquier otro
fallo es un error de la herramienta que el cliente trata como transitorio.
"""

from __future__ import annotations

import logging
from datetime import datetime

from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError
from pydantic import BaseModel, ConfigDict

from eda_retail.mail_server.store import MailStore

log = logging.getLogger(__name__)

NAME = "mail-server"
UNKNOWN_CUSTOMER = "unknown_customer"


class SendEmailResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message_id: str
    accepted_at: datetime


def build_server(store: MailStore) -> MCPServer:
    server = MCPServer(NAME, instructions="Correo simulado de la PoC: envía avisos a clientes por su referencia.")

    @server.tool()
    def send_email(customer_ref: str, subject: str, body: str, idempotency_key: str) -> SendEmailResult:
        """Envía un correo al cliente `customer_ref`. La misma `idempotency_key` devuelve el mismo correo."""
        mail = store.send(customer_ref=customer_ref, subject=subject, body=body, idempotency_key=idempotency_key)
        if mail is None:
            log.info("send_email rechazado: %s para %s", UNKNOWN_CUSTOMER, customer_ref)
            raise ToolError(UNKNOWN_CUSTOMER)
        log.info("send_email aceptado: %s para %s (clave %s)", mail.message_id, customer_ref, idempotency_key)
        return SendEmailResult(message_id=mail.message_id, accepted_at=mail.accepted_at)

    return server
