"""Escritura en el log de eventos y en el outbox.

`append_events` no abre transacción: se llama dentro de la transacción del componente,
para que el estado de escritura, `events` y `outbox` se confirmen juntos (patrón Outbox).
Nunca publica en Kafka; eso lo hace el relay.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

import psycopg
from psycopg.types.json import Jsonb
from pydantic import ValidationError

from eda_retail.events.envelope import EventEnvelope


class InvalidEventError(ValueError):
    """Evento rechazado porque no cumple la estructura común."""


def _validate(event: EventEnvelope | Mapping[str, Any]) -> EventEnvelope:
    if isinstance(event, EventEnvelope):
        return event
    try:
        return EventEnvelope.model_validate(event)
    except ValidationError as exc:
        fields = ", ".join(".".join(str(p) for p in err["loc"]) for err in exc.errors())
        raise InvalidEventError(f"evento rechazado, campos ausentes o inválidos: {fields}") from exc


def append_events(
    conn: psycopg.Connection, events: Iterable[EventEnvelope | Mapping[str, Any]]
) -> list[int]:
    """Valida y añade los eventos a `events` y `outbox`. Devuelve sus `seq` en orden."""
    validated = [_validate(e) for e in events]
    seqs: list[int] = []
    for e in validated:
        row = conn.execute(
            """
            INSERT INTO events (event_id, event_type, event_version, occurred_at,
                                correlation_id, causation_id, producer, payload)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING seq
            """,
            (
                e.event_id,
                e.event_type,
                e.event_version,
                e.occurred_at,
                e.correlation_id,
                e.causation_id,
                e.producer,
                Jsonb(e.payload),
            ),
        ).fetchone()
        seq = row["seq"] if isinstance(row, Mapping) else row[0]
        conn.execute("INSERT INTO outbox (seq) VALUES (%s)", (seq,))
        seqs.append(seq)
    return seqs
