"""Aplicación FastAPI del front: solo atiende peticiones de localhost.

La guarda de origen va por fuera del middleware CORS para que una petición (o un preflight)
desde un origen ajeno reciba 403 antes de llegar a ningún endpoint.
"""

from __future__ import annotations

import re

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

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


def create_app() -> FastAPI:
    app = FastAPI(title="eda-retail web", docs_url=None, redoc_url=None, openapi_url=None)
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

    return app
