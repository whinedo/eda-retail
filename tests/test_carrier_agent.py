"""Agente del transportista simulado (spec a2a-carrier, «Agente del transportista simulado»).

La app A2A se ejercita en proceso (httpx.ASGITransport): ningún test abre un puerto.
"""

from __future__ import annotations

import asyncio
from datetime import date

import httpx
import pytest
from a2a.client import A2ACardResolver, ClientConfig, ClientFactory
from a2a.helpers.proto_helpers import get_data_parts, new_data_part, new_message
from a2a.types import Role, SendMessageRequest, Task, TaskState

from eda_retail.carrier_agent import rules
from eda_retail.carrier_agent.server import create_app
from eda_retail.config import Settings
from eda_retail.synthetic.tracking import tracking_ref

URL = "http://carrier.test"
TOKEN = "test-token"
TODAY = date(2026, 11, 15)


def _app():
    return create_app(url=URL, token=TOKEN, seed=42, today=lambda: TODAY)


def _http(app, token: str | None = TOKEN) -> httpx.AsyncClient:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url=URL, headers=headers)


async def _send(http: httpx.AsyncClient, data: dict, task: Task | None = None) -> Task:
    card = await A2ACardResolver(http, URL).get_agent_card()
    client = ClientFactory(ClientConfig(streaming=False, httpx_client=http)).create(card)
    message = new_message([new_data_part(data)], role=Role.ROLE_USER,
                          task_id=task.id if task else None, context_id=task.context_id if task else None)
    [response] = [r async for r in client.send_message(SendMessageRequest(message=message))]
    return response.task


def _result(task: Task) -> dict:
    [artifact] = task.artifacts
    [data] = get_data_parts(artifact.parts)
    return data


def test_delivery_options():
    """Opciones de entrega"""
    async def run():
        async with _http(_app()) as http:
            return await _send(http, {"order_id": "P-1001", "cause": "strike"})

    task = asyncio.run(run())

    assert task.status.state == TaskState.TASK_STATE_COMPLETED
    result = _result(task)
    assert set(result) == {"new_delivery_date", "can_reship", "accepts_claim", "notes"}
    assert result["can_reship"] is False and result["accepts_claim"] is False
    assert date.fromisoformat(result["new_delivery_date"]) > TODAY
    assert len(result["notes"]) <= 500


def test_the_agent_asks_for_more_data():
    """El agente pide más datos"""
    async def run():
        async with _http(_app()) as http:
            first = await _send(http, {"order_id": "P-1001", "cause": "loss"})
            second = await _send(http, {"tracking_ref": tracking_ref("P-1001")}, first)
            return first, second

    first, second = asyncio.run(run())

    assert first.status.state == TaskState.TASK_STATE_INPUT_REQUIRED
    assert get_data_parts(first.status.message.parts) == [{"needs": ["tracking_ref"]}]
    assert second.status.state == TaskState.TASK_STATE_COMPLETED
    assert _result(second)["accepts_claim"] is True


def test_a_wrong_tracking_ref_is_asked_again():
    async def run():
        async with _http(_app()) as http:
            first = await _send(http, {"order_id": "P-1001", "cause": "damage"})
            return await _send(http, {"tracking_ref": "TRK-WRONG"}, first)

    assert asyncio.run(run()).status.state == TaskState.TASK_STATE_INPUT_REQUIRED


def test_agent_card_is_published_with_its_skill_and_bearer_scheme():
    async def run():
        async with _http(_app(), token=None) as http:
            return await A2ACardResolver(http, URL).get_agent_card()

    card = asyncio.run(run())

    assert card.name == rules.AGENT_NAME
    assert [s.id for s in card.skills] == [rules.SKILL_ID]
    assert [(i.url.rstrip("/"), i.protocol_binding) for i in card.supported_interfaces] == [(URL, "JSONRPC")]
    [scheme] = card.security_schemes.values()
    assert scheme.http_auth_security_scheme.scheme.lower() == "bearer"


@pytest.mark.parametrize("token", [None, "wrong"])
def test_a_task_without_the_bearer_token_is_refused(token):
    async def run():
        async with _http(_app(), token=token) as http:
            return await http.post("/", json={"jsonrpc": "2.0", "id": 1, "method": "SendMessage", "params": {}})

    assert asyncio.run(run()).status_code == 401


def test_the_agent_needs_a_token():
    with pytest.raises(ValueError, match="A2A_CARRIER_TOKEN"):
        create_app(url=URL, token="", seed=42)


def test_a_task_without_order_or_known_cause_is_rejected():
    async def run():
        async with _http(_app()) as http:
            return await _send(http, {"order_id": "P-1001", "cause": "meteorite"})

    assert asyncio.run(run()).status.state == TaskState.TASK_STATE_REJECTED


@pytest.mark.parametrize("cause, reship, claim, dated", [
    ("delay", False, False, True), ("strike", False, False, True), ("loss", True, True, False),
    ("damage", True, True, False), ("wrong_address", True, False, True), ("unknown", False, False, False),
])
def test_rule_table(cause, reship, claim, dated):
    """La tabla de reglas del docstring de carrier_agent.rules"""
    options = rules.delivery_options("P-1001", cause, seed=42, today=TODAY)
    assert (options["can_reship"], options["accepts_claim"]) == (reship, claim)
    assert (options["new_delivery_date"] is not None) == dated
    assert rules.requires_tracking(cause) == (cause in {"loss", "damage"})


def test_rules_are_deterministic_for_a_seed():
    """Misma semilla, mismo pedido y misma causa: la misma respuesta"""
    a = rules.delivery_options("P-1001", "strike", seed=42, today=TODAY)
    assert a == rules.delivery_options("P-1001", "strike", seed=42, today=TODAY)
    dates = {rules.delivery_options("P-1001", "strike", seed=s, today=TODAY)["new_delivery_date"] for s in range(20)}
    assert len(dates) > 1


def test_tracking_ref_derives_from_the_order():
    """Supuesto A4: un tracking_ref sintético por pedido, siempre el mismo"""
    assert tracking_ref("P-1001") == tracking_ref("P-1001") != tracking_ref("P-1002")
    assert tracking_ref("P-1001").startswith("TRK-")


def test_carrier_agent_settings_have_defaults(monkeypatch):
    for name in ("CARRIER_AGENT_URL", "CARRIER_AGENT_PORT", "A2A_CARRIER_TOKEN", "A2A_TIMEOUT_S", "A2A_MAX_ROUNDS",
                 "CARRIER_AGENT_SEED"):
        monkeypatch.delenv(name, raising=False)
    s = Settings()
    assert (s.carrier_agent_url, s.carrier_agent_port, s.a2a_carrier_token) == ("http://127.0.0.1:8130", 8130, "")
    assert (s.a2a_timeout_s, s.a2a_max_rounds, s.carrier_agent_seed) == (5.0, 2, 42)
