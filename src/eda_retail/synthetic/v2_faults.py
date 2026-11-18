"""Fallos de las dependencias de V2 (spec synthetic-data, «Fallos de las dependencias de V2»; E20).

- `FaultyMail`: construye el servidor MCP de correo en proceso sobre un buzón (`Harness(mail_server=...)`). Su
  `send_email` tiene la misma entrada que el real, así que pasa la lista blanca del cliente. En una llamada elegida,
  `down` lanza un error de la herramienta (el cliente lo trata como `mail_unavailable`, transitorio) e `invalid`
  devuelve una salida sin `message_id` o con la dirección del destinatario (`mcp_output_invalid`); en ninguno de los
  dos se guarda el correo. La elección depende de la semilla, el modo, la `idempotency_key` y el número de llamada
  con esa clave, así que un reintento puede salir bien.
- `FaultyCarrier`: un `CarrierConsultant` para el planificador que consulta con el `CarrierClient` real a la app A2A
  del agente del transportista, montada para cada consulta sobre `httpx.ASGITransport` (cada consulta corre en su
  propio bucle de asyncio y una app compartida ataría su almacén de tareas a otro). En una incidencia elegida,
  `slow` retrasa el agente más que `A2A_TIMEOUT_S`, `unauthorized` consulta con un token que el agente no acepta
  (401) y `bad_card` publica una Agent Card con otra skill. Cuenta las llamadas a `execute` del agente (una por
  tarea y otra por cada ronda `input-required`).
"""

from __future__ import annotations

import asyncio
import hashlib
import threading
from collections import Counter
from datetime import date
from typing import Any

import httpx
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from mcp.server.mcpserver import MCPServer
from mcp.server.mcpserver.exceptions import ToolError

from eda_retail.carrier_agent import rules as carrier_rules
from eda_retail.carrier_agent import server as carrier_server
from eda_retail.config import Settings
from eda_retail.mail_server.server import NAME as MAIL_SERVER_NAME
from eda_retail.mail_server.server import UNKNOWN_CUSTOMER
from eda_retail.mail_server.store import MailStore
from eda_retail.planner.carrier import CarrierClient, CarrierConsultation

MAIL_MODES = ("down", "invalid")
CARRIER_MODES = ("slow", "unauthorized", "bad_card")
MAIL_UNAVAILABLE = "mail_unavailable"
CARRIER_URL = "http://carrier-agent.e20"
CARRIER_TOKEN = "e20-carrier-token"  # token sintético del agente en proceso, no una credencial
WRONG_TOKEN = "e20-wrong-token"
OTHER_SKILL = "track_parcel"
SLOW_MARGIN_S = 1.0  # `slow` tarda `A2A_TIMEOUT_S` más este margen


def chosen(seed: int, mode: str, key: str, probability: float) -> bool:
    """Decide de forma determinista si la llamada o la consulta con esa clave falla."""
    if probability <= 0.0:
        return False
    digest = hashlib.sha256(f"{seed}:v2:{mode}:{key}".encode()).digest()
    return int.from_bytes(digest[:8], "big") / 2**64 < probability


class FaultyMail:
    """Servidor de correo MCP con fallos: `FaultyMail(...).build` es el `mail_server` del harness."""

    def __init__(self, mode: str, *, probability: float, seed: int) -> None:
        if mode not in MAIL_MODES:
            raise ValueError(f"modo de correo desconocido: {mode}")
        self.mode = mode
        self._probability = probability
        self._seed = seed
        self._calls: Counter[str] = Counter()  # llamadas por idempotency_key
        self._lock = threading.Lock()
        self.calls = 0
        self.injected = 0

    @property
    def keys(self) -> int:
        """Claves distintas con las que se llamó a `send_email`."""
        with self._lock:
            return len(self._calls)

    def _inject(self, idempotency_key: str) -> bool:
        with self._lock:
            self.calls += 1
            self._calls[idempotency_key] += 1
            n = self._calls[idempotency_key]
            inject = chosen(self._seed, self.mode, f"{idempotency_key}:{n}", self._probability)
            self.injected += inject
            return inject

    def build(self, store: MailStore) -> MCPServer:
        server = MCPServer(MAIL_SERVER_NAME, instructions="Correo simulado de la PoC con fallos inyectados (E20).")

        @server.tool()
        def send_email(customer_ref: str, subject: str, body: str, idempotency_key: str) -> dict[str, str]:
            """Envía un correo al cliente `customer_ref`, salvo en las llamadas elegidas para fallar."""
            if self._inject(idempotency_key):
                if self.mode == "down":
                    raise ToolError(MAIL_UNAVAILABLE)
                return _invalid_output(idempotency_key, customer_ref)
            mail = store.send(customer_ref=customer_ref, subject=subject, body=body, idempotency_key=idempotency_key)
            if mail is None:
                raise ToolError(UNKNOWN_CUSTOMER)
            return {"message_id": mail.message_id, "accepted_at": mail.accepted_at.isoformat()}

        return server


def _invalid_output(idempotency_key: str, customer_ref: str) -> dict[str, str]:
    """Sin `message_id` o con una dirección: la mitad de cada forma, según la clave."""
    accepted_at = "2026-01-01T00:00:00+00:00"
    if hashlib.sha256(idempotency_key.encode()).digest()[0] % 2:
        return {"accepted_at": accepted_at}
    return {"message_id": f"M-{idempotency_key[:8]}", "accepted_at": accepted_at,
            "to": f"{customer_ref.lower()}@example.test"}


class _Counting(AgentExecutor):
    """El agente del transportista real, con un retraso opcional y la cuenta de tareas recibidas."""

    def __init__(self, inner: AgentExecutor, *, delay_s: float = 0.0) -> None:
        self._inner = inner
        self._delay_s = delay_s
        self.received = 0

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        self.received += 1
        if self._delay_s:
            await asyncio.sleep(self._delay_s)
        await self._inner.execute(context, event_queue)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        await self._inner.cancel(context, event_queue)


class FaultyCarrier:
    """`CarrierConsultant` del planificador contra el agente del transportista en proceso, con fallos.

    `mode` None: sin fallos (el agente de las variantes de correo y de `baseline`). `injected` guarda las incidencias
    elegidas; `tasks_received` y `tasks_on_injected`, las tareas que recibió el agente en total y en esas incidencias.
    """

    def __init__(self, settings: Settings, mode: str | None = None, *, probability: float = 0.0, seed: int,
                 today: date | None = None) -> None:
        if mode is not None and mode not in CARRIER_MODES:
            raise ValueError(f"modo del transportista desconocido: {mode}")
        self.mode = mode
        self._timeout_s = settings.a2a_timeout_s
        self._max_rounds = settings.a2a_max_rounds
        self._probability = probability
        self._seed = seed
        self._today = today
        self._lock = threading.Lock()
        self.injected: set[str] = set()
        self.tasks_received = 0
        self.tasks_on_injected = 0

    def consult(self, order_id: str, cause: str, *, incident_id: str) -> CarrierConsultation:
        inject = self.mode is not None and chosen(self._seed, self.mode, incident_id, self._probability)
        today = (lambda: self._today) if self._today else date.today
        delay = self._timeout_s + SLOW_MARGIN_S if inject and self.mode == "slow" else 0.0
        agent = _Counting(carrier_server.CarrierExecutor(self._seed, today), delay_s=delay)
        skill = OTHER_SKILL if inject and self.mode == "bad_card" else carrier_rules.SKILL_ID
        app = carrier_server.create_app(url=CARRIER_URL, token=CARRIER_TOKEN, seed=self._seed, executor=agent,
                                        card=carrier_server.agent_card(CARRIER_URL, skill_id=skill))
        token = WRONG_TOKEN if inject and self.mode == "unauthorized" else CARRIER_TOKEN
        client = CarrierClient(CARRIER_URL, token, timeout_s=self._timeout_s, max_rounds=self._max_rounds,
                               transport=httpx.ASGITransport(app=app))
        result = client.consult(order_id, cause, incident_id=incident_id)
        with self._lock:
            self.tasks_received += agent.received
            if inject:
                self.injected.add(incident_id)
                self.tasks_on_injected += agent.received
        return result

    def expected_outcome(self, incident_id: str) -> str:
        """El `outcome` de `CarrierConsulted` que corresponde al fallo inyectado en la incidencia."""
        if incident_id not in self.injected:
            return "answered"
        return {"slow": "timeout", "unauthorized": "failed", "bad_card": "rejected_by_guardrail"}[self.mode]

    def report(self) -> dict[str, Any]:
        return {"mode": self.mode, "injected": len(self.injected), "tasks_received": self.tasks_received,
                "tasks_on_injected": self.tasks_on_injected}
