"""Dead letter queue (spec resilience, «Dead letter queue»; supuesto A6).

Un evento irrecuperable (esquema inválido) o que agota sus reintentos se guarda en
`dead_letters`, con el motivo y el `correlation_id`, en la misma transacción que el
procesamiento del consumidor. El relay lo publica después en `eda.dlq`
(`published_at`): el código de negocio nunca publica en Kafka.
"""

from __future__ import annotations

import uuid
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from eda_retail.infra.schemas import qualified

INVALID_SCHEMA = "invalid_schema"
RETRIES_EXHAUSTED = "retries_exhausted"


def record(
    conn: psycopg.Connection,
    *,
    consumer: str,
    reason: str,
    event_id: uuid.UUID | None,
    correlation_id: str | None,
    raw: Any,
    schema: str | None = None,
) -> int:
    """Guarda la dead letter en `<schema>.dead_letters` (o en la tabla compartida, sin esquema)."""
    table = qualified(schema, "dead_letters") if schema is not None else "dead_letters"
    row = conn.execute(
        f"""
        INSERT INTO {table} (consumer, event_id, correlation_id, reason, raw_message)
        VALUES (%s, %s, %s, %s, %s) RETURNING id
        """,
        (consumer, event_id, correlation_id, reason, Jsonb(raw)),
    ).fetchone()
    return row["id"]
