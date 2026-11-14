"""Clientes REST de un componente contra PostgreSQL real, sin relay (las pruebas leen su outbox)."""

from __future__ import annotations

import uuid

from fastapi.testclient import TestClient

from eda_retail.infra.commands import CommandSpec
from eda_retail.infra.component import build_app
from eda_retail.infra.db import connect


class _NoRelay:
    def stop(self, timeout=None) -> None:
        pass


def client(database_url: str, schema: str, specs: dict[str, CommandSpec]) -> TestClient:
    return TestClient(build_app(schema, specs, lambda: connect(database_url), start_relay=_NoRelay))


def body(correlation_id: str, payload: dict, command_id: uuid.UUID | None = None) -> dict:
    return {
        "command_id": str(command_id or uuid.uuid4()),
        "correlation_id": correlation_id,
        "causation_id": str(uuid.uuid4()),
        "payload": payload,
    }


def outbox(conn, schema: str, correlation_id: str) -> list[dict]:
    with conn.transaction():
        return conn.execute(
            f"SELECT event_type, causation_id, payload FROM {schema}.outbox WHERE correlation_id = %s ORDER BY id",
            (correlation_id,),
        ).fetchall()
