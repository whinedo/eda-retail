"""Inbox: deduplicación de eventos por consumidor (spec idempotency).

`register` se llama dentro de la transacción del procesamiento, para que el registro
y los cambios que produce el evento se confirmen juntos.
"""

from __future__ import annotations

import uuid

import psycopg

from eda_retail.infra.schemas import qualified


def register(
    conn: psycopg.Connection,
    consumer: str,
    event_id: uuid.UUID,
    *,
    enabled: bool = True,
    schema: str | None = None,
    kind: str = "event",
) -> bool:
    """Registra el evento. Devuelve False si ya estaba procesado (duplicado).

    Con `schema`, usa el inbox del componente (`<schema>.inbox`, clave `message_id`); sin él,
    la tabla compartida del modelo anterior (hasta T10). Con `enabled=False`
    (INBOX_ENABLED=false, experimento E1) no escribe nada y trata todo evento como nuevo.
    """
    if not enabled:
        return True
    if schema is not None:
        cur = conn.execute(
            f"INSERT INTO {qualified(schema, 'inbox')} (message_id, kind) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (event_id, kind),
        )
        return cur.rowcount == 1
    cur = conn.execute(
        "INSERT INTO inbox (consumer, event_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (consumer, event_id),
    )
    return cur.rowcount == 1


def seen(
    conn: psycopg.Connection, consumer: str, event_id: uuid.UUID, *, enabled: bool = True, schema: str | None = None
) -> bool:
    """True si el evento ya se procesó. Lectura previa: evita pagar una llamada al LLM por un
    duplicado. No sustituye a `register`, que es quien decide dentro de la transacción."""
    if not enabled:
        return False
    if schema is not None:
        row = conn.execute(
            f"SELECT 1 FROM {qualified(schema, 'inbox')} WHERE message_id = %s", (event_id,)
        ).fetchone()
        return row is not None
    row = conn.execute(
        "SELECT 1 FROM inbox WHERE consumer = %s AND event_id = %s", (consumer, event_id)
    ).fetchone()
    return row is not None
