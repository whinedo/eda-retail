"""Aplicación FastAPI del front: solo atiende peticiones de localhost.

La guarda de origen va por fuera del middleware CORS para que una petición (o un preflight)
desde un origen ajeno reciba 403 antes de llegar a ningún endpoint.
"""

from __future__ import annotations

import os
import re
from collections.abc import AsyncIterator, Iterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated

import psycopg
from fastapi import Depends, FastAPI, Header, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from eda_retail.config import load_settings
from eda_retail.llm.ollama_client import DEFAULT_HOST as DEFAULT_OLLAMA_HOST
from eda_retail.web.commands import (
    NOTICE_SCRIPT,
    RUN_MAX_SECONDS,
    SCRIPT_LABEL,
    CommandRequest,
    CommandRunner,
    InputLine,
    RunInputFull,
    RunLaunchError,
    RunnerBusy,
    RunNotAcceptingInput,
    RunNotFound,
    output_stream,
)
from eda_retail.web.db import read_only_connect
from eda_retail.web.events import DEFAULT_LIMIT, MAX_LIMIT, events_after, last_seq, list_events
from eda_retail.web.health import HealthChecker, check_kafka, check_ollama, check_postgres
from eda_retail.web.nodes import node_detail
from eda_retail.web.stream import event_stream, start_seq
from eda_retail.web.topology import EDGES, NODES, node
from eda_retail.web.traces import jaeger_ui_url

# Orígenes admitidos: http(s) en localhost, 127.0.0.1 o ::1, con cualquier puerto
# (el servidor de Vite en desarrollo y el propio backend).
LOCAL_ORIGIN = re.compile(r"^https?://(localhost|127\.0\.0\.1|\[::1\])(:\d+)?$")
# Cabecera Host admitida: frena el DNS rebinding (un dominio ajeno que resuelve a 127.0.0.1).
LOCAL_HOST = re.compile(r"^(localhost|127\.0\.0\.1|\[::1\])(:\d+)?$")


class LocalOriginGuard:
    """Rechaza con 403 cualquier petición HTTP cuyo Host u Origin no sean de localhost."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            host = _header(scope, b"host")
            origin = _header(scope, b"origin")
            if host is None or not LOCAL_HOST.match(host) or (origin is not None and not LOCAL_ORIGIN.match(origin)):
                await _forbidden(send)
                return
        await self.app(scope, receive, send)


def _header(scope: Scope, name: bytes) -> str | None:
    for key, value in scope.get("headers", []):
        if key.lower() == name:
            return value.decode("latin-1")
    return None


async def _forbidden(send: Send) -> None:
    body = b'{"detail":"origen no permitido"}'
    await send(
        {
            "type": "http.response.start",
            "status": 403,
            "headers": [(b"content-type", b"application/json"), (b"content-length", str(len(body)).encode())],
        }
    )
    await send({"type": "http.response.body", "body": body})


def _read_only_conn(request: Request) -> Iterator[psycopg.Connection]:
    with read_only_connect(request.app.state.database_url) as conn:
        yield conn


Conn = Annotated[psycopg.Connection, Depends(_read_only_conn)]


def free_text_enabled() -> bool:
    """`send --text` solo con `WEB_FREE_TEXT=true` (o `1`)."""
    return os.environ.get("WEB_FREE_TEXT", "").strip().lower() in ("true", "1")


def create_app(
    database_url: str | None = None,
    *,
    stream_max_polls: int | None = None,
    notice_script: Path = NOTICE_SCRIPT,
    run_max_seconds: float = RUN_MAX_SECONDS,
    health: HealthChecker | None = None,
    free_text: bool | None = None,
) -> FastAPI:
    """`stream_max_polls`, `notice_script`, `run_max_seconds` y `health` solo los cambian los tests.

    `free_text`: si se admite `send --text`; por defecto lo decide `WEB_FREE_TEXT` (deshabilitado salvo
    `true` o `1`: la PoC es un ejemplo académico).
    """
    if free_text is None:
        free_text = free_text_enabled()
    runner = CommandRunner(notice_script, run_max_seconds)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        yield
        runner.shutdown()  # parada del backend: no deja el comando en curso huérfano

    app = FastAPI(title="eda-retail web", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)
    app.state.runner = runner
    settings = load_settings()
    app.state.database_url = database_url or settings.database_url
    db_url = app.state.database_url
    ollama_host = os.environ.get("OLLAMA_HOST", DEFAULT_OLLAMA_HOST)
    health = health or HealthChecker(
        {
            "postgres": lambda: check_postgres(db_url),
            "kafka": lambda: check_kafka(settings.kafka_bootstrap_servers),
            "ollama": lambda: check_ollama(ollama_host),
        }
    )
    app.state.stream_max_polls = stream_max_polls
    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=LOCAL_ORIGIN.pattern,
        allow_methods=["GET", "POST"],
        allow_headers=["Content-Type", "Last-Event-ID"],
    )
    app.add_middleware(LocalOriginGuard)  # añadido el último: es el más externo

    @app.get("/api/ping")
    def ping() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/api/events")
    def events(
        conn: Conn,
        limit: Annotated[int, Query(ge=1, le=MAX_LIMIT)] = DEFAULT_LIMIT,
        before_seq: Annotated[int | None, Query(ge=1)] = None,
        incident_id: str | None = None,
        node: str | None = None,
    ) -> dict:
        return list_events(conn, limit=limit, before_seq=before_seq, incident_id=incident_id, node=node)

    @app.get("/api/events/stream")
    def events_stream(
        request: Request,
        after_seq: Annotated[int | None, Query(ge=0)] = None,
        last_event_id: Annotated[str | None, Header()] = None,
    ) -> StreamingResponse:
        database_url = request.app.state.database_url
        max_polls = request.app.state.stream_max_polls

        def frames() -> Iterator[str]:
            # La conexión vive lo que dure el stream: se abre aquí, no en una dependencia.
            with read_only_connect(database_url) as conn:
                start = start_seq(last_event_id=last_event_id, after_seq=after_seq, current_last=lambda: last_seq(conn))
                yield from event_stream(lambda after: events_after(conn, after), start, max_polls=max_polls)

        return _sse(frames())

    @app.get("/api/topology")
    def topology() -> dict:
        return {
            "nodes": [n.as_dict() for n in NODES],
            "edges": [{"source": source, "target": target} for source, target in EDGES],
        }

    @app.get("/api/config")
    def config() -> dict:
        return {"jaeger_ui_url": jaeger_ui_url(), "free_text_enabled": free_text}

    @app.get("/api/health")
    def health_report() -> dict:
        return health.report()

    @app.get("/api/nodes/{node_id}")
    def node_info(node_id: str) -> dict:
        try:
            found = node(node_id)
        except KeyError:
            raise HTTPException(404, "nodo desconocido") from None
        with read_only_connect(db_url) as conn:
            return node_detail(conn, found, health)

    @app.post("/api/commands", status_code=202)
    def run_command(body: CommandRequest) -> dict:
        if body.text is not None and not free_text:
            raise HTTPException(422, "el texto libre está deshabilitado (WEB_FREE_TEXT)")
        try:
            run = runner.start(body)
        except RunnerBusy:
            raise HTTPException(409, "ya hay un comando en ejecución") from None
        except RunLaunchError as error:
            raise HTTPException(500, f"no se pudo lanzar {SCRIPT_LABEL}: {error}") from None
        return {"run_id": run.run_id, "argv": [SCRIPT_LABEL, *body.args()]}

    @app.get("/api/commands/{run_id}/stream")
    def command_stream(run_id: str, last_event_id: Annotated[str | None, Header()] = None) -> StreamingResponse:
        try:
            run = runner.get(run_id)
        except RunNotFound:
            raise HTTPException(404, "ejecución desconocida") from None
        return _sse(output_stream(run, last_event_id))

    @app.post("/api/commands/{run_id}/input", status_code=202)
    def command_input(run_id: str, body: InputLine) -> dict:
        try:
            runner.send_line(run_id, body.line)
        except RunNotFound:
            raise HTTPException(404, "ejecución desconocida") from None
        except RunNotAcceptingInput:
            raise HTTPException(409, "la ejecución no es interactiva o ya terminó") from None
        except RunInputFull:
            raise HTTPException(409, "la entrada del proceso está llena: no la está leyendo") from None
        return {"ok": True}

    return app


def _sse(frames: Iterator[str]) -> StreamingResponse:
    return StreamingResponse(
        frames, media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
    )
