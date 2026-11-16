"""Clientes REST de un componente contra PostgreSQL real, sin relay (las pruebas leen su outbox).

Un comando con trabajo largo (`deferred`) responde 202 y deja su trabajo en `command_jobs`; por defecto el
cliente hace después de cada petición los trabajos pendientes, como haría el worker del componente, para que
las pruebas lean el resultado en el outbox. Con `run_jobs=False` la petición solo acepta.
"""

from __future__ import annotations

import uuid

from fastapi.testclient import TestClient

from eda_retail.infra.backoff import Backoff
from eda_retail.infra.commands import CommandSpec
from eda_retail.infra.component import build_app
from eda_retail.infra.db import connect
from eda_retail.infra.jobs import run_job_once

MAX_JOBS = 50  # un trabajo que falla vuelve a pending: tope para no quedarse en bucle


class _NoRelay:
    def stop(self, timeout=None) -> None:
        pass


def run_jobs(database_url: str, schema: str, specs: dict[str, CommandSpec]) -> int:
    """Hace los trabajos pendientes del componente, como su worker. Devuelve cuántos hizo."""
    conn = connect(database_url)
    try:
        done = 0
        while done < MAX_JOBS and run_job_once(conn, schema, specs, lease_seconds=60, backoff=Backoff(0.5, 0.5)):
            done += 1
        return done
    finally:
        conn.close()


class _JobsClient(TestClient):
    def __init__(self, app, drain) -> None:
        super().__init__(app)
        self._drain = drain

    def post(self, *args, **kwargs):  # type: ignore[override]
        response = super().post(*args, **kwargs)
        self._drain()
        return response


def client(database_url: str, schema: str, specs: dict[str, CommandSpec], *, run_jobs_after: bool = True) -> TestClient:
    app = build_app(schema, specs, lambda: connect(database_url), start_relay=_NoRelay)
    if not run_jobs_after or not any(spec.deferred for spec in specs.values()):
        return TestClient(app)
    return _JobsClient(app, lambda: run_jobs(database_url, schema, specs))


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


def jobs(conn, schema: str, correlation_id: str) -> list[dict]:
    with conn.transaction():
        return conn.execute(
            f"SELECT * FROM {schema}.command_jobs WHERE correlation_id = %s ORDER BY job_id", (correlation_id,),
        ).fetchall()
