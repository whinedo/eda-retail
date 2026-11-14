"""Enlaces a Jaeger (spec web-front, "Acceso a las trazas en Jaeger").

El `trace_id` sale del `traceparent` W3C que `event_log` guarda en `event_log.events.trace_context`; el front
solo enlaza a la interfaz de Jaeger, nunca consulta su API.
"""

from __future__ import annotations

import os
import re
from typing import Any
from urllib.parse import urlsplit

DEFAULT_JAEGER_UI_URL = "http://localhost:16686"
# version-trace_id-parent_id-flags, en minúsculas (W3C Trace Context).
_TRACEPARENT = re.compile(r"^[0-9a-f]{2}-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$")
_NULL_TRACE_ID = "0" * 32


def trace_id_from_context(context: Any) -> str | None:
    """trace_id del traceparent, o None si no hay contexto o está mal formado."""
    if not isinstance(context, dict):
        return None
    traceparent = context.get("traceparent")
    if not isinstance(traceparent, str):
        return None
    match = _TRACEPARENT.match(traceparent)
    if not match or match.group(1) == _NULL_TRACE_ID:
        return None
    return match.group(1)


def jaeger_ui_url() -> str:
    """JAEGER_UI_URL sin la barra final; cualquier valor que no sea http(s) se ignora."""
    raw = (os.environ.get("JAEGER_UI_URL") or "").strip()
    parts = urlsplit(raw)
    if parts.scheme not in {"http", "https"} or not parts.netloc:
        return DEFAULT_JAEGER_UI_URL
    return raw.rstrip("/")
