"""Aplicación FastAPI del front: solo atiende peticiones de localhost.

La guarda de origen va por fuera del middleware CORS para que una petición (o un preflight)
desde un origen ajeno reciba 403 antes de llegar a ningún endpoint.
"""

from __future__ import annotations

import re
from collections.abc import Iterator
from typing import Annotated

import psycopg
from fastapi import Depends, FastAPI, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from starlette.types import ASGIApp, Receive, Scope, Send

from eda_retail.config import load_settings
from eda_retail.web.db import read_only_connect
from eda_retail.web.events import DEFAULT_LIMIT, MAX_LIMIT, events_after, last_seq, list_events
from eda_retail.web.stream import event_stream, start_seq

# Orígenes admitidos: http(s) en localhost, 127.0.0.1 o ::1, con cualquier puerto
# (el servidor de Vite en desarrollo y el propio backend).
LOCAL_ORIGIN = re.compile(r"^https?://(localhost|127\.0\.0\.1|\[::1\])(:\d+)?$")


class LocalOriginGuard:
    """Rechaza con 403 cualquier petición HTTP cuya cabecera Origin no sea de localhost."""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http":
            origin = _header(scope, b"origin")
            if origin is not None and not LOCAL_ORIGIN.match(origin):
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


def create_app(database_url: str | None = None, *, stream_max_polls: int | None = None) -> FastAPI:
    """`stream_max_polls` corta el stream tras N lecturas; solo lo usan los tests."""
    app = FastAPI(title="eda-retail web", docs_url=None, redoc_url=None, openapi_url=None)
    app.state.database_url = database_url or load_settings().database_url
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

        return StreamingResponse(
            frames(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"}
        )

    return app
