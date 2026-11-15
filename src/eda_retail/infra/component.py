"""Proceso de un componente que atiende comandos (design.md §4.4).

Expone su command handler como `POST /commands/<Comando>` y, mientras vive, mantiene el relay de
su outbox en un hilo del mismo proceso.
"""

from __future__ import annotations

from collections.abc import AsyncIterator, Callable, Mapping
from contextlib import asynccontextmanager
from typing import Protocol
from urllib.parse import urlsplit

import psycopg
from fastapi import FastAPI

from eda_retail.infra.commands import CommandSpec, command_router


class Relay(Protocol):
    def stop(self, timeout: float | None = None) -> None: ...


def listen_address(url: str) -> tuple[str, int]:
    """Host y puerto en los que escucha el sistema, sacados de su `<SISTEMA>_URL`."""
    parts = urlsplit(url)
    if not parts.hostname or parts.port is None:
        raise ValueError(f"la URL del sistema debe llevar host y puerto: {url!r}")
    return parts.hostname, parts.port


def build_app(
    schema: str,
    specs: Mapping[str, CommandSpec],
    connect: Callable[[], psycopg.Connection],
    *,
    start_relay: Callable[[], Relay],
) -> FastAPI:
    """API de comandos del componente `schema`; su relay arranca y se detiene con ella."""

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        relay = start_relay()
        try:
            yield
        finally:
            relay.stop(10)

    app = FastAPI(title=f"eda-{schema}", lifespan=lifespan)
    app.include_router(command_router(schema, specs, connect))
    return app


def serve(name: str, schema: str, specs: Mapping[str, CommandSpec], url: str, *,
          limit_concurrency: int | None = None) -> None:
    """Arranca el proceso del componente: trazas, relay de su outbox y su API de comandos.

    `limit_concurrency`: peticiones simultáneas como máximo (los ejecutores, `EXECUTOR_MAX_CONCURRENCY`;
    spec resilience, «Pico de incidencias»). Por encima, uvicorn responde 503 y la saga reintenta el
    comando con el mismo `command_id` (design.md §4.4)."""
    import uvicorn

    from eda_retail.config import load_settings
    from eda_retail.infra.db import connect
    from eda_retail.infra.logs import setup_logging
    from eda_retail.infra.outbox import start_relay
    from eda_retail.infra.tracing import setup_tracing

    settings = load_settings()
    setup_logging(name)
    provider = setup_tracing(name, settings)
    host, port = listen_address(url)
    app = build_app(schema, specs, lambda: connect(settings.database_url),
                    start_relay=lambda: start_relay(settings, schema))
    try:
        uvicorn.run(app, host=host, port=port, log_level="info", limit_concurrency=limit_concurrency)
    finally:
        if provider is not None:
            provider.shutdown()
