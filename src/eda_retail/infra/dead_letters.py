"""Dead letter queue (spec resilience, «Dead letter queue»; supuesto A6).

Un evento irrecuperable (esquema inválido) o que agota sus reintentos se guarda en
`dead_letters`, con el motivo y el `correlation_id`, en la misma transacción que el
procesamiento del consumidor. El relay lo publica después en `eda.dlq`
(`published_at`): el código de negocio nunca publica en Kafka.

El lado de lectura (`event_log` y los proyectores) guarda las suyas con `record_at`, identificadas
por su posición en Kafka, y las publica un relay que solo lleva dead letters (migración 0024).
"""

from __future__ import annotations

import uuid
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

from eda_retail.infra.positions import Position
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
    schema: str,
) -> int:
    """Guarda la dead letter en `<schema>.dead_letters`."""
    table = qualified(schema, "dead_letters")
    row = conn.execute(
        f"""
        INSERT INTO {table} (consumer, event_id, correlation_id, reason, raw_message)
        VALUES (%s, %s, %s, %s, %s) RETURNING id
        """,
        (consumer, event_id, correlation_id, reason, Jsonb(raw)),
    ).fetchone()
    return row["id"]


def record_at(
    conn: psycopg.Connection,
    *,
    consumer: str,
    position: Position,
    reason: str,
    event_id: uuid.UUID | None,
    correlation_id: str | None,
    raw: Any,
    schema: str,
) -> bool:
    """Dead letter de un consumidor posicionado (lado de lectura), identificada por su posición.

    Sin inbox y con la posición rebobinable, el mismo mensaje puede aplicarse otra vez: la fila
    es única por (consumidor, posición) y una segunda vez no se guarda. True si la guardó.
    """
    table = qualified(schema, "dead_letters")
    return conn.execute(
        f"""
        INSERT INTO {table} (consumer, event_id, correlation_id, reason, raw_message,
                             kafka_topic, kafka_partition, kafka_offset)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (consumer, kafka_topic, kafka_partition, kafka_offset) DO NOTHING
        """,
        (consumer, event_id, correlation_id, reason, Jsonb(raw), position.topic, position.partition,
         position.offset),
    ).rowcount == 1
