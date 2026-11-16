"""Cliente MCP del servidor de correo y sus guardrails (spec mcp-mail, «Guardrails del cliente MCP»).

Es código, no un agente: el redactor solo produce el texto y nunca ve la herramienta. Cada envío abre una
sesión MCP (Streamable HTTP en `MAIL_SERVER_URL`, o un `MCPServer` en proceso en los tests y en el arnés):

- **Lista blanca.** Al conectar comprueba que el servidor ofrece `send_email` con la entrada esperada y solo
  registra esa herramienta (`tools`), aunque el servidor anuncie otras. El cliente no tiene ninguna otra
  forma de invocar una herramienta.
- **Entrada y salida validadas** con pydantic. Una salida fuera de esquema, o con algo que parece una
  dirección de correo, es `mcp_output_invalid`, no reintentable: no se guarda, no se publica ni se registra
  su contenido en el log.
- **Errores.** `unknown_customer` del servidor no es reintentable; un servidor caído o cualquier otro error
  de la herramienta es transitorio (`mail_unavailable`), y el ejecutor lo reintenta con espera exponencial.

Cada fallo de un guardrail se registra con su motivo y su traza (`trace_id` de OpenTelemetry).
"""

from __future__ import annotations

import logging
import re
from datetime import datetime
from typing import Any

import anyio
from mcp import Client
from mcp.server.mcpserver import MCPServer
from opentelemetry import trace
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from eda_retail.step_executors.errors import BusinessError, Transient

log = logging.getLogger(__name__)

TOOL = "send_email"
EXPECTED_INPUT = frozenset({"customer_ref", "subject", "body", "idempotency_key"})
UNKNOWN_CUSTOMER = "unknown_customer"
MAIL_UNAVAILABLE = "mail_unavailable"
MCP_TOOL_NOT_ALLOWED = "mcp_tool_not_allowed"
MCP_INPUT_INVALID = "mcp_input_invalid"
MCP_OUTPUT_INVALID = "mcp_output_invalid"
MAIL_LIMIT_EXCEEDED = "mail_limit_exceeded"
_EMAIL = re.compile(r"[^\s@]+@[^\s@]+\.[A-Za-z]{2,}")


class SendEmailInput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    customer_ref: str = Field(min_length=1, max_length=64)
    subject: str = Field(min_length=1, max_length=200)
    body: str = Field(min_length=1, max_length=5000)
    idempotency_key: str = Field(min_length=1, max_length=128)


class SendEmailOutput(BaseModel):
    model_config = ConfigDict(extra="forbid")

    message_id: str = Field(min_length=1, max_length=128)
    accepted_at: datetime


def trace_id() -> str:
    context = trace.get_current_span().get_span_context()
    return f"{context.trace_id:032x}" if context.trace_id else "-"


def guardrail_failed(reason: str, detail: str) -> None:
    """Registra el fallo de un guardrail: motivo, detalle sin datos del servidor y traza."""
    log.warning("guardrail MCP %s: %s (trace_id=%s)", reason, detail, trace_id())


def _strings(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, dict):
        return [s for k, v in value.items() for s in (*_strings(k), *_strings(v))]
    if isinstance(value, list | tuple):
        return [s for v in value for s in _strings(v)]
    return []


def _allowed(tool: Any) -> bool:
    """`send_email` con la entrada esperada: los cuatro campos, de texto y obligatorios."""
    schema = tool.input_schema or {}
    properties = schema.get("properties") or {}
    return (set(properties) == EXPECTED_INPUT and set(schema.get("required") or ()) == EXPECTED_INPUT
            and all(p.get("type") == "string" for p in properties.values()))


class McpMailClient:
    """Cliente de `send_email`. `server` es la URL del servidor o un `MCPServer` en proceso."""

    def __init__(self, server: str | MCPServer, *, timeout: float = 10.0) -> None:
        self._server = server
        self._timeout = timeout
        self.tools: frozenset[str] = frozenset()  # herramientas invocables tras conectar (solo send_email)

    def send_email(self, request: SendEmailInput) -> SendEmailOutput:
        try:
            arguments = SendEmailInput.model_validate(request.model_dump()).model_dump()
        except ValidationError:
            guardrail_failed(MCP_INPUT_INVALID, "entrada de send_email fuera de esquema")
            raise BusinessError(MCP_INPUT_INVALID) from None
        try:
            allowed, result = anyio.run(self._exchange, arguments)
        except Exception as exc:  # noqa: BLE001 — servidor caído, sin respuesta o error de transporte
            log.warning("servidor de correo no disponible: %s (trace_id=%s)", type(exc).__name__, trace_id())
            raise Transient(MAIL_UNAVAILABLE) from None
        if not allowed:
            guardrail_failed(MCP_TOOL_NOT_ALLOWED, f"el servidor no ofrece {TOOL} con la entrada esperada")
            raise BusinessError(MCP_TOOL_NOT_ALLOWED)
        return self._output(result)

    async def _exchange(self, arguments: dict[str, Any]) -> tuple[bool, Any]:
        """Conecta, aplica la lista blanca e invoca `send_email`. Sin excepciones propias dentro de la sesión."""
        async with Client(self._server, read_timeout_seconds=self._timeout) as client:
            listed = await client.list_tools()
            self.tools = frozenset(t.name for t in listed.tools if t.name == TOOL and _allowed(t))
            if TOOL not in self.tools:
                return False, None
            return True, await client.call_tool(TOOL, arguments)

    def _output(self, result: Any) -> SendEmailOutput:
        texts = [getattr(block, "text", "") or "" for block in result.content or ()]
        if result.is_error:
            if any(UNKNOWN_CUSTOMER in text for text in texts):
                raise BusinessError(UNKNOWN_CUSTOMER)
            log.warning("send_email devolvió un error del servidor (trace_id=%s)", trace_id())
            raise Transient(MAIL_UNAVAILABLE)
        if any(_EMAIL.search(s) for s in (*texts, *_strings(result.structured_content))):
            guardrail_failed(MCP_OUTPUT_INVALID, "la salida de send_email trae una dirección de correo")
            raise BusinessError(MCP_OUTPUT_INVALID)
        try:
            return SendEmailOutput.model_validate(result.structured_content)
        except ValidationError as exc:
            fields = sorted({".".join(map(str, e["loc"])) or "-" for e in exc.errors()})
            guardrail_failed(MCP_OUTPUT_INVALID, f"salida de send_email fuera de esquema en {fields}")
            raise BusinessError(MCP_OUTPUT_INVALID) from None
