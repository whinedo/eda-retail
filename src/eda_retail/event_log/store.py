"""Vista del log de eventos (spec cqrs-views, "Vista del log de eventos"; design.md §8).

Cada mensaje de `eda.events` se guarda en `event_log.events` con un `seq` creciente en el orden de
consumo, su partición, su offset y su contexto de traza. El `seq` se asigna con un advisory lock de
la transacción tomado antes del INSERT: dos transacciones no pueden intercalarse, así que el orden
de `seq` es el de los COMMIT y quien lee "lo que va después del último `seq`" no se salta nada.
Un `event_id` repetido no se guarda dos veces. La tabla es solo de añadir.

Un mensaje que no es un evento válido (no es JSON, no cumple el esquema o lleva datos personales)
no se guarda en el log: va a `event_log.dead_letters` en la misma transacción que la posición, y el
relay de dead letters del lado de lectura lo publica en `eda.dlq`.
"""

from __future__ import annotations

import json
import logging

import psycopg
from confluent_kafka import Message
from psycopg.types.json import Jsonb

from eda_retail.events.envelope import EventEnvelope
from eda_retail.infra import dead_letters
from eda_retail.infra.consumer import InvalidMessage, decode_checked
from eda_retail.infra.positions import Position, position_of, trace_context_of

log = logging.getLogger(__name__)

SCHEMA = "event_log"
CONSUMER = "event_log"
_SEQ_LOCK = 0x6576656E745F6C6F  # "event_lo": serializa la asignación de seq


def published_envelope(msg: Message) -> EventEnvelope:
    """El evento tal como se publicó (sin convertir de versión); `InvalidMessage` si no es válido.

    `event_log` atiende todos los tipos: un `event_type` fuera del catálogo con la estructura común
    válida se guarda como cualquier otro evento (tolerant reader).
    """
    decode_checked(msg)
    body = json.loads(msg.value())
    body.pop("seq", None)
    return EventEnvelope.model_validate(body)


def envelope_of(msg: Message) -> EventEnvelope | None:
    """El evento tal como se publicó, o None si no es válido."""
    try:
        return published_envelope(msg)
    except InvalidMessage:
        return None


def record(
    conn: psycopg.Connection, event: EventEnvelope, position: Position, trace_context: dict[str, str] | None
) -> int | None:
    """Guarda el evento dentro de la transacción abierta; devuelve su `seq`, o None si ya estaba."""
    conn.execute("SELECT pg_advisory_xact_lock(%s)", (_SEQ_LOCK,))
    row = conn.execute(
        """
        INSERT INTO event_log.events (event_id, event_type, event_version, occurred_at, correlation_id,
                                      causation_id, producer, payload, kafka_topic, kafka_partition,
                                      kafka_offset, trace_context)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (event_id) DO NOTHING
        RETURNING seq
        """,
        (event.event_id, event.event_type, event.event_version, event.occurred_at, event.correlation_id,
         event.causation_id, event.producer, Jsonb(event.payload), position.topic, position.partition,
         position.offset, Jsonb(trace_context) if trace_context else None),
    ).fetchone()
    return row["seq"] if row else None


def apply_message(conn: psycopg.Connection, msg: Message) -> None:
    """`ApplyMessage` del consumidor posicionado: guarda el evento, o su dead letter si no es válido.

    La dead letter va en la misma transacción que la posición, así que el mensaje no bloquea el log
    ni se descarta en silencio (spec resilience, «Evento inválido en el lado de lectura»).
    """
    try:
        event = published_envelope(msg)
    except InvalidMessage as bad:
        log.warning("event_log: mensaje inválido a dead letter (%s): %s", bad.event_type or "?", bad)
        dead_letters.record_at(
            conn, consumer=CONSUMER, position=position_of(msg), reason=dead_letters.INVALID_SCHEMA,
            event_id=bad.event_id, correlation_id=bad.correlation_id, raw=bad.raw, schema=SCHEMA,
        )
        return
    record(conn, event, position_of(msg), trace_context_of(msg))
