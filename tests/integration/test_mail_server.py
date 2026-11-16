"""Servidor de correo MCP contra PostgreSQL real (spec mcp-mail, «Servidor de correo simulado»)."""

from __future__ import annotations

import uuid

import anyio
from mcp import Client

from eda_retail.mail_server.directory import address_for
from eda_retail.mail_server.server import UNKNOWN_CUSTOMER, build_server
from eda_retail.mail_server.store import PgMailStore, seed_customers


def _ref() -> str:
    return f"C-{uuid.uuid4().hex[:8]}"


def _call(connect_db, args: dict):
    async def run():
        async with Client(build_server(PgMailStore(connect_db))) as client:
            return await client.call_tool("send_email", args)

    return anyio.run(run)


def _args(ref: str, key: str) -> dict:
    return {"customer_ref": ref, "subject": "Su pedido", "body": "Llegará tarde.", "idempotency_key": key}


def _mails(conn, key: str) -> list[dict]:
    rows = conn.execute("SELECT * FROM mail_server.mailbox WHERE idempotency_key = %s", (key,)).fetchall()
    conn.commit()
    return rows


def test_mail_sent(conn, connect_db):
    """Correo enviado"""
    ref, key = _ref(), uuid.uuid4().hex
    seed_customers(conn, [(ref, address_for(ref))])
    result = _call(connect_db, _args(ref, key))
    assert not result.is_error and set(result.structured_content) == {"message_id", "accepted_at"}
    assert address_for(ref) not in str(result)
    [mail] = _mails(conn, key)
    assert (mail["to_address"], mail["message_id"]) == (address_for(ref), result.structured_content["message_id"])


def test_unknown_customer(conn, connect_db):
    """Cliente desconocido"""
    key = uuid.uuid4().hex
    result = _call(connect_db, _args(_ref(), key))
    assert result.is_error and UNKNOWN_CUSTOMER in result.content[0].text
    assert _mails(conn, key) == []


def test_repeated_send_with_the_same_key(conn, connect_db):
    """Envío repetido con la misma clave"""
    ref, key = _ref(), uuid.uuid4().hex
    seed_customers(conn, [(ref, address_for(ref))])
    first = _call(connect_db, _args(ref, key))
    again = _call(connect_db, _args(ref, key))
    assert again.structured_content["message_id"] == first.structured_content["message_id"]
    assert len(_mails(conn, key)) == 1


def test_seeding_the_directory_is_idempotent(conn):
    ref = _ref()
    assert seed_customers(conn, [(ref, address_for(ref))]) == 1
    assert seed_customers(conn, [(ref, address_for(ref))]) == 0
