"""App A2A del agente del transportista (spec a2a-carrier, «Agente del transportista simulado»).

Con el SDK oficial de A2A (`a2a-sdk`): publica su Agent Card en `/.well-known/agent-card.json` y atiende
tareas por JSON-RPC sobre HTTP en `/`. La card declara un esquema bearer, y toda petición JSON-RPC sin
`Authorization: Bearer <A2A_CARRIER_TOKEN>` responde 401; la card es pública.

La tarea `delivery_options` lleva en una parte de datos `order_id` y `cause` (y, en una ronda
posterior, `tracking_ref`). El agente responde con las reglas de `carrier_agent.rules`: completa la
tarea con un artefacto de datos, la deja en `input-required` con `{"needs": ["tracking_ref"]}` o la
rechaza si falta el pedido o la causa no es conocida. Las tareas viven en memoria: sin base de datos.
"""

from __future__ import annotations

import hmac
from collections.abc import Callable
from datetime import date
from typing import Any

from a2a.helpers.proto_helpers import get_data_parts, new_data_part, new_task, new_text_part
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.request_handlers import DefaultRequestHandler
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskUpdater
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
    HTTPAuthSecurityScheme,
    Message,
    Role,
    SecurityRequirement,
    SecurityScheme,
    StringList,
    TaskState,
)
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

from eda_retail.carrier_agent import rules
from eda_retail.synthetic.tracking import tracking_ref

RPC_PATH = "/"
DATA = "application/json"
BEARER = "bearer"


def agent_card(url: str, *, name: str = rules.AGENT_NAME, skill_id: str = rules.SKILL_ID, bearer: bool = True,
               interface_url: str | None = None) -> AgentCard:
    """La Agent Card. Los argumentos con nombre solo los cambian los tests de los guardrails del cliente."""
    schemes = {BEARER: SecurityScheme(http_auth_security_scheme=HTTPAuthSecurityScheme(scheme="Bearer"))}
    return AgentCard(
        name=name,
        description="Agente del transportista (simulado): opciones de entrega para un pedido con incidencia.",
        version="1.0.0",
        supported_interfaces=[AgentInterface(url=(interface_url or url).rstrip("/") + RPC_PATH,
                                             protocol_binding="JSONRPC", protocol_version="1.0")],
        capabilities=AgentCapabilities(streaming=False),
        security_schemes=schemes if bearer else {},
        security_requirements=[SecurityRequirement(schemes={BEARER: StringList()})] if bearer else [],
        default_input_modes=[DATA],
        default_output_modes=[DATA],
        skills=[AgentSkill(
            id=skill_id, name="Opciones de entrega",
            description="Para un order_id y una causa: new_delivery_date, can_reship, accepts_claim y notes.",
            tags=["delivery", "carrier"], input_modes=[DATA], output_modes=[DATA],
        )],
    )


def _data(messages: list[Message]) -> dict[str, Any]:
    data: dict[str, Any] = {}
    for message in messages:
        if message.role == Role.ROLE_USER:
            for part in get_data_parts(message.parts):
                if isinstance(part, dict):
                    data.update(part)
    return data


class CarrierExecutor(AgentExecutor):
    def __init__(self, seed: int, today: Callable[[], date] = date.today) -> None:
        self._seed = seed
        self._today = today

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        history = list(context.current_task.history) if context.current_task is not None else []
        if context.current_task is None:
            await event_queue.enqueue_event(new_task(context.task_id, context.context_id,
                                                     TaskState.TASK_STATE_SUBMITTED, history=[context.message]))
        updater = TaskUpdater(event_queue, context.task_id, context.context_id)
        request = _data([*history, context.message])
        order_id, cause = request.get("order_id"), request.get("cause")
        if not isinstance(order_id, str) or not order_id or cause not in rules.RULES:
            refusal = new_text_part("se necesitan order_id y una causa conocida")
            await updater.reject(updater.new_agent_message([refusal]))
            return
        if rules.requires_tracking(cause) and request.get("tracking_ref") != tracking_ref(order_id):
            await updater.requires_input(updater.new_agent_message([new_data_part({"needs": ["tracking_ref"]})]))
            return
        options = rules.delivery_options(order_id, cause, seed=self._seed, today=self._today())
        await updater.add_artifact([new_data_part(options)], name=rules.SKILL_ID)
        await updater.complete()

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        await TaskUpdater(event_queue, context.task_id, context.context_id).cancel()


class BearerAuth(BaseHTTPMiddleware):
    """Exige el bearer token en las peticiones JSON-RPC; la Agent Card (GET) es pública."""

    def __init__(self, app, token: str) -> None:
        super().__init__(app)
        self._expected = f"Bearer {token}".encode()

    async def dispatch(self, request: Request, call_next) -> Response:
        if request.method != "GET":
            given = request.headers.get("authorization", "").encode()
            if not hmac.compare_digest(given, self._expected):
                return JSONResponse({"error": "unauthorized"}, status_code=401,
                                    headers={"WWW-Authenticate": "Bearer"})
        return await call_next(request)


def create_app(*, url: str, token: str, seed: int, today: Callable[[], date] = date.today,
               executor: AgentExecutor | None = None, card: AgentCard | None = None) -> Starlette:
    """La app ASGI del agente. `executor` y `card` solo los cambian los tests del cliente A2A."""
    if not token:
        raise ValueError("el agente del transportista necesita A2A_CARRIER_TOKEN")
    card = card or agent_card(url)
    handler = DefaultRequestHandler(agent_executor=executor or CarrierExecutor(seed, today),
                                    task_store=InMemoryTaskStore(), agent_card=card)
    return Starlette(routes=[*create_agent_card_routes(card), *create_jsonrpc_routes(handler, RPC_PATH)],
                     middleware=[Middleware(BearerAuth, token=token)])
