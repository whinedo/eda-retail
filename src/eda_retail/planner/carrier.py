"""Cliente A2A del planificador y sus guardrails (spec a2a-carrier, «Consulta del planificador» y
«Guardrails de A2A»).

Lo llama `PlanPreparer` en el `prepare` de `CreatePlan` (worker del planificador, fuera de la
transacción, antes del LLM) solo con `POC_VERSION=2` (`from_settings`). Es código, no el LLM:

- **Autenticación y Agent Card.** Cada petición lleva `Authorization: Bearer <A2A_CARRIER_TOKEN>`. Antes
  de la primera tarea se lee la Agent Card y se comprueba su nombre (`carrier-agent`), su URL JSON-RPC
  (`CARRIER_AGENT_URL`), la skill `delivery_options` y el esquema bearer; la card válida se guarda en el
  proceso. Si no coincide, no se envía la tarea.
- **Solo referencias.** La tarea lleva `order_id` y la causa (`request_data` rechaza cualquier otro
  campo); en una ronda `input-required` solo se responde `tracking_ref` (el sintético del pedido). Si
  el agente pide otra cosa, se cancela la tarea.
- **Respuesta validada** con `DeliveryOptions` (`extra="forbid"`, `notes` ≤ 500): lo que no cumple el
  esquema se descarta.
- **Plazo y rondas.** Un plazo total de `A2A_TIMEOUT_S` para toda la consulta (card incluida) y como
  mucho `A2A_MAX_ROUNDS` rondas `input-required`; al superarlos se cancela la tarea si ya existe.
- **Prompt injection.** `render_options` da el JSON que va al prompt dentro del bloque de contenido no
  confiable, sin `<` ni `>` para que `notes` no pueda cerrar el delimitador.

El resultado (`CarrierConsultation`) dice `answered`, `timeout`, `failed` (red, HTTP, estado de la
tarea) o `rejected_by_guardrail`, las rondas atendidas y la latencia; cada fallo se registra en el log
con su motivo y la traza. Con una incidencia de varios pedidos se consulta el primero.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from dataclasses import dataclass
from datetime import date
from typing import Any, Literal, Protocol

import httpx
from a2a.client import ClientConfig, ClientFactory
from a2a.client.card_resolver import A2ACardResolver
from a2a.client.errors import A2AClientTimeoutError
from a2a.helpers.proto_helpers import get_data_parts, new_data_part, new_message
from a2a.types import AgentCard, CancelTaskRequest, Message, Role, SendMessageRequest, Task, TaskState
from opentelemetry import trace
from pydantic import BaseModel, ConfigDict, Field, StrictBool, ValidationError

from eda_retail.config import Settings
from eda_retail.synthetic.tracking import tracking_ref as synthetic_tracking_ref

log = logging.getLogger(__name__)

AGENT_NAME = "carrier-agent"
SKILL_ID = "delivery_options"
REQUEST_FIELDS = ("order_id", "cause", "tracking_ref")
ANSWERABLE = {"tracking_ref"}
CANCEL_TIMEOUT_S = 1.0

Outcome = Literal["answered", "timeout", "failed", "rejected_by_guardrail"]


class DeliveryOptions(BaseModel):
    """La respuesta de `delivery_options`, tal como puede llegar al prompt."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    new_delivery_date: date | None
    can_reship: StrictBool
    accepts_claim: StrictBool
    notes: str = Field(max_length=500)


@dataclass(frozen=True)
class CarrierConsultation:
    outcome: Outcome
    rounds: int
    latency_ms: int
    options: DeliveryOptions | None = None

    def event_payload(self, incident_id: str) -> dict[str, Any]:
        """El payload de `CarrierConsulted`: sin `notes` ni datos personales."""
        return {"incident_id": incident_id, "outcome": self.outcome, "rounds": self.rounds,
                "latency_ms": self.latency_ms}


class CarrierConsultant(Protocol):
    def consult(self, order_id: str, cause: str, *, incident_id: str) -> CarrierConsultation: ...


class GuardrailError(Exception):
    """Un guardrail de A2A no se cumple; el mensaje es el motivo."""


class TaskFailed(Exception):
    """La tarea acabó en un estado que no es `completed` (o la respuesta no es una tarea)."""


def request_data(order_id: str, cause: str, **extra: str) -> dict[str, str]:
    """Los datos de la tarea: solo `order_id`, la causa y, si se pide, `tracking_ref` (regla 10)."""
    unexpected = sorted(set(extra) - {"tracking_ref"})
    if unexpected:
        raise ValueError(f"campos no permitidos en la tarea A2A: {', '.join(unexpected)}")
    data = {"order_id": order_id, "cause": cause, **extra}
    assert set(data) <= set(REQUEST_FIELDS), data
    return data


def render_options(options: DeliveryOptions) -> str:
    """JSON de las opciones para el prompt; `<` y `>` escapados para que nada cierre el bloque."""
    text = json.dumps(options.model_dump(mode="json"), ensure_ascii=False)
    return text.replace("<", "\\u003c").replace(">", "\\u003e")


def _trace_id() -> str:
    context = trace.get_current_span().get_span_context()
    return f"{context.trace_id:032x}" if context.is_valid else "-"


@dataclass
class _Progress:
    rounds: int = 0
    task: Task | None = None


class CarrierClient:
    """Cliente A2A del agente del transportista. `transport` solo lo cambian los tests (en proceso)."""

    def __init__(self, url: str, token: str, *, timeout_s: float, max_rounds: int,
                 transport: httpx.AsyncBaseTransport | None = None, tracking=synthetic_tracking_ref) -> None:
        self._url = url.rstrip("/")
        self._token = token
        self._timeout_s = timeout_s
        self._max_rounds = max_rounds
        self._transport = transport
        self._tracking = tracking
        self._card: AgentCard | None = None  # la card ya comprobada, para todo el proceso

    def consult(self, order_id: str, cause: str, *, incident_id: str) -> CarrierConsultation:
        started = time.monotonic()
        progress = _Progress()
        outcome, reason, options = asyncio.run(self._consult(order_id, cause, progress))
        latency_ms = int((time.monotonic() - started) * 1000)
        if outcome != "answered":
            log.warning("consulta A2A al transportista de %s: %s (%s) rondas=%s trace=%s", incident_id, outcome,
                        reason, progress.rounds, _trace_id())
        return CarrierConsultation(outcome, progress.rounds, latency_ms, options)

    async def _consult(self, order_id: str, cause: str,
                       progress: _Progress) -> tuple[Outcome, str, DeliveryOptions | None]:
        headers = {"Authorization": f"Bearer {self._token}"}
        async with httpx.AsyncClient(transport=self._transport, headers=headers, timeout=self._timeout_s) as http:
            client = None
            try:
                async with asyncio.timeout(self._timeout_s):
                    card = self._card or await self._verified_card(http)
                    self._card = card
                    client = ClientFactory(ClientConfig(streaming=False, httpx_client=http)).create(card)
                    return "answered", "", await self._talk(client, order_id, cause, progress)
            except (TimeoutError, A2AClientTimeoutError, httpx.TimeoutException):
                outcome, reason = "timeout", f"sin respuesta en {self._timeout_s} s"
            except GuardrailError as exc:
                outcome, reason = "rejected_by_guardrail", str(exc)
            except Exception as exc:  # noqa: BLE001 - red, HTTP o protocolo: se planifica sin opciones
                outcome, reason = "failed", f"{type(exc).__name__}: {exc}"
            await self._cancel(client, progress.task)
            return outcome, reason, None

    async def _verified_card(self, http: httpx.AsyncClient) -> AgentCard:
        card = await A2ACardResolver(http, self._url).get_agent_card()
        urls = {i.url.rstrip("/") for i in card.supported_interfaces if i.protocol_binding == "JSONRPC"}
        bearer = any(s.http_auth_security_scheme.scheme.lower() == "bearer" for s in card.security_schemes.values())
        problems = [name for name, ok in (
            ("nombre", card.name == AGENT_NAME),
            ("url", self._url in urls),
            ("skill", SKILL_ID in {s.id for s in card.skills}),
            ("bearer", bearer),
        ) if not ok]
        if problems:
            raise GuardrailError(f"agent_card_mismatch: {', '.join(problems)}")
        return card

    async def _talk(self, client, order_id: str, cause: str, progress: _Progress) -> DeliveryOptions:
        task = await self._send(client, new_message([new_data_part(request_data(order_id, cause))],
                                                    role=Role.ROLE_USER), progress)
        while task.status.state == TaskState.TASK_STATE_INPUT_REQUIRED:
            needs = _needs(task.status.message)
            if not needs or not needs <= ANSWERABLE:
                raise GuardrailError(f"input_request_not_allowed: {sorted(needs) or 'sin campos'}")
            if progress.rounds >= self._max_rounds:
                raise GuardrailError(f"max_rounds: más de {self._max_rounds} rondas input-required")
            progress.rounds += 1
            answer = {"tracking_ref": self._tracking(order_id)}
            task = await self._send(client, new_message([new_data_part(answer)], role=Role.ROLE_USER,
                                                        task_id=task.id, context_id=task.context_id), progress)
        if task.status.state != TaskState.TASK_STATE_COMPLETED:
            raise TaskFailed(f"tarea en {TaskState.Name(task.status.state)}")
        data = [d for artifact in task.artifacts for d in get_data_parts(artifact.parts)]
        if len(data) != 1:
            raise GuardrailError(f"invalid_response: {len(data)} partes de datos")
        try:
            return DeliveryOptions.model_validate(data[0])
        except ValidationError as exc:
            fields = ", ".join(".".join(str(p) for p in e["loc"]) for e in exc.errors())
            raise GuardrailError(f"invalid_response: {fields}") from exc

    @staticmethod
    async def _send(client, message: Message, progress: _Progress) -> Task:
        [response] = [r async for r in client.send_message(SendMessageRequest(message=message))]
        if not response.HasField("task"):
            raise TaskFailed("el agente respondió con un mensaje, no con una tarea")
        progress.task = response.task
        return response.task

    @staticmethod
    async def _cancel(client, task: Task | None) -> None:
        """Cancela la tarea si existe y no ha terminado; un fallo al cancelar solo se registra."""
        if client is None or task is None or task.status.state not in (
                TaskState.TASK_STATE_SUBMITTED, TaskState.TASK_STATE_WORKING, TaskState.TASK_STATE_INPUT_REQUIRED):
            return
        try:
            await asyncio.wait_for(client.cancel_task(CancelTaskRequest(id=task.id)), CANCEL_TIMEOUT_S)
        except Exception as exc:  # noqa: BLE001
            log.warning("no se pudo cancelar la tarea A2A %s: %s", task.id, exc)


def _needs(message: Message) -> set[str]:
    needs: set[str] = set()
    for part in get_data_parts(message.parts):
        if isinstance(part, dict) and isinstance(part.get("needs"), list):
            needs |= {str(n) for n in part["needs"]}
    return needs


def from_settings(settings: Settings) -> CarrierClient | None:
    """El cliente A2A con `POC_VERSION=2`; con la versión 1 el planificador no consulta (None)."""
    if settings.poc_version != 2:
        return None
    return CarrierClient(settings.carrier_agent_url, settings.a2a_carrier_token, timeout_s=settings.a2a_timeout_s,
                         max_rounds=settings.a2a_max_rounds)
