"""Servidor de correo MCP (spec mcp-mail, «Servidor de correo simulado»): sin PostgreSQL, con el buzón en
memoria y el cliente MCP en proceso del SDK. La versión con PostgreSQL está en
tests/integration/test_mail_server.py."""

from __future__ import annotations

import logging

import anyio
import pytest
from mcp import Client

from eda_retail.mail_server import directory
from eda_retail.mail_server.server import UNKNOWN_CUSTOMER, build_server
from eda_retail.mail_server.store import MemoryMailStore
from eda_retail.synthetic.generator import generate
from eda_retail.synthetic.sample_data import ORDERS

ARGS = {"customer_ref": "C-0042", "subject": "Su pedido P-1", "body": "Llegará tarde.", "idempotency_key": "K1"}


def _call(store: MemoryMailStore, args: dict) -> object:
    async def run():
        async with Client(build_server(store)) as client:
            return await client.call_tool("send_email", args)

    return anyio.run(run)


def _store() -> MemoryMailStore:
    return MemoryMailStore({"C-0042": directory.address_for("C-0042")})


def test_mail_sent(caplog):
    """Correo enviado"""
    store = _store()
    with caplog.at_level(logging.INFO):
        result = _call(store, ARGS)
    assert not result.is_error
    assert set(result.structured_content) == {"message_id", "accepted_at"}
    [mail] = store.mailbox
    assert mail.to_address == directory.address_for("C-0042")
    assert mail.message_id == result.structured_content["message_id"]
    address = directory.address_for("C-0042")
    assert address not in str(result)
    assert address not in caplog.text


def test_unknown_customer():
    """Cliente desconocido"""
    store = _store()
    result = _call(store, {**ARGS, "customer_ref": "C-9999"})
    assert result.is_error
    assert UNKNOWN_CUSTOMER in result.content[0].text
    assert store.mailbox == []


def test_repeated_send_with_the_same_key():
    """Envío repetido con la misma clave"""
    store = _store()
    first = _call(store, ARGS)
    again = _call(store, {**ARGS, "body": "Otro cuerpo"})
    assert again.structured_content["message_id"] == first.structured_content["message_id"]
    assert [m.idempotency_key for m in store.mailbox] == ["K1"]


def test_the_server_exposes_only_send_email():
    """El servidor expone una sola herramienta, send_email, con su entrada y su salida"""

    async def run():
        async with Client(build_server(_store())) as client:
            return (await client.list_tools()).tools

    [tool] = anyio.run(run)
    assert tool.name == "send_email"
    assert set(tool.input_schema["required"]) == {"customer_ref", "subject", "body", "idempotency_key"}
    assert set(tool.output_schema["required"]) == {"message_id", "accepted_at"}


def test_the_directory_covers_every_synthetic_and_sample_customer():
    """El directorio se siembra con direcciones sintéticas de todos los clientes del generador y del ejemplo"""
    refs = set(directory.customer_refs())
    assert {o.customer_ref for o in generate(7, 50).orders} <= refs
    assert {c for _, c, *_ in ORDERS} <= refs
    assert directory.address_for("CUST-0042") == "cust-0042@example.test"
    assert directory.customer_refs() == directory.customer_refs()  # determinista


def test_a_delivery_error_is_a_retryable_tool_error():
    """Un fallo del buzón no es unknown_customer: el cliente lo trata como transitorio"""

    def down(key: str) -> str:
        raise RuntimeError("timeout")

    store = MemoryMailStore({"C-0042": "c@example.test"}, new_id=down, dedup=False)
    result = _call(store, ARGS)
    assert result.is_error and UNKNOWN_CUSTOMER not in result.content[0].text


@pytest.mark.parametrize("dedup", [False])
def test_without_dedup_the_store_repeats_the_effect(dedup):
    """E1 sin idempotencia: el buzón no reconoce la clave y repite el envío"""
    ids = iter(["M-1", "M-2"])
    store = MemoryMailStore({"C-0042": "c@example.test"}, new_id=lambda key: next(ids), dedup=dedup)
    _call(store, ARGS)
    _call(store, ARGS)
    assert [m.message_id for m in store.mailbox] == ["M-1", "M-2"]
