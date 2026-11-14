"""Invocación síncrona de comandos por REST (design.md §4.4) con la biblioteca estándar.

`POST <url>/commands/<Comando>` con el sobre del comando. Las respuestas 2xx y 4xx se devuelven:
el sistema ha decidido (aceptado, rechazado o comando desconocido). Si no responde, o responde
con un error 5xx, se lanza `CommandUnavailable` y quien invoca puede reintentar con el mismo
`command_id`.
"""

from __future__ import annotations

import json
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from typing import Any

from eda_retail.infra import tracing
from eda_retail.infra.commands import ACCEPTED, REPLAYED_HEADER


class CommandUnavailable(RuntimeError):
    """El sistema no ha respondido al comando: se puede reintentar con el mismo `command_id`."""


@dataclass(frozen=True)
class CommandReply:
    status_code: int
    body: dict[str, Any]
    replayed: bool = False

    @property
    def accepted(self) -> bool:
        return self.status_code == ACCEPTED

    @property
    def reason(self) -> str | None:
        return self.body.get("reason")


def invoke(
    url: str,
    command_type: str,
    *,
    command_id: uuid.UUID,
    correlation_id: str,
    causation_id: uuid.UUID | None,
    payload: dict[str, Any],
    timeout: float,
) -> CommandReply:
    target = f"{url.rstrip('/')}/commands/{command_type}"
    body = {
        "command_id": str(command_id),
        "correlation_id": correlation_id,
        "causation_id": str(causation_id) if causation_id is not None else None,
        "payload": payload,
    }
    headers = {"Content-Type": "application/json", **(tracing.inject_current() or {})}
    request = urllib.request.Request(target, data=json.dumps(body, default=str).encode(), headers=headers,
                                     method="POST")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return _reply(response.status, response.read(), response.headers)
    except urllib.error.HTTPError as error:
        if error.code >= 500:
            raise CommandUnavailable(f"{command_type} en {url}: HTTP {error.code}") from error
        return _reply(error.code, error.read(), error.headers)
    except (urllib.error.URLError, TimeoutError, ConnectionError) as error:
        raise CommandUnavailable(f"{command_type}: {url} no responde ({error})") from error


def _reply(status: int, raw: bytes, headers) -> CommandReply:
    try:
        body = json.loads(raw) if raw else {}
    except json.JSONDecodeError:
        body = {"status": "invalid_response"}
    return CommandReply(status, body if isinstance(body, dict) else {}, headers.get(REPLAYED_HEADER) == "true")
