"""Stream SSE de eventos nuevos (spec web-front, "Actualización en vivo").

Lee `events` por `seq`, como los proyectores: cada lectura sigue tras el último `seq` enviado,
así que una reconexión con `Last-Event-ID` no pierde ni duplica filas.
"""

from __future__ import annotations

import json
import time
from collections.abc import Callable, Iterator
from typing import Any

IDLE_SECONDS = 0.5  # el panel debe mostrar un evento nuevo en menos de 2 s


def start_seq(*, last_event_id: str | None, after_seq: int | None, current_last: Callable[[], int]) -> int:
    """Último `seq` que el cliente ya tiene: Last-Event-ID, luego after_seq, luego el final del log."""
    if last_event_id and last_event_id.strip().isdigit():
        return int(last_event_id)
    if after_seq is not None:
        return after_seq
    return current_last()


def event_stream(
    fetch_after: Callable[[int], list[dict[str, Any]]],
    last: int,
    *,
    idle_seconds: float = IDLE_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
    max_polls: int | None = None,
) -> Iterator[str]:
    """Genera tramas SSE (`id` = `seq`) para siempre, o durante `max_polls` lecturas (tests)."""
    polls = 0
    while max_polls is None or polls < max_polls:
        polls += 1
        batch = fetch_after(last)
        for event in batch:
            last = event["seq"]
            yield f"id: {last}\nevent: event\ndata: {json.dumps(event, default=str)}\n\n"
        if not batch:
            sleep(idle_seconds)
