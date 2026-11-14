"""Inbox: deduplicación de eventos por consumidor (spec idempotency).

`register` se llama dentro de la transacción del procesamiento, para que el registro
y los cambios que produce el evento se confirmen juntos.
"""

from __future__ import annotations

import uuid

import psycopg


def register(
    conn: psycopg.Connection, consumer: str, event_id: uuid.UUID, *, enabled: bool = True
) -> bool:
    """Registra el evento. Devuelve False si ya estaba procesado (duplicado).

    Con `enabled=False` (INBOX_ENABLED=false, experimento E1) no escribe nada y
    trata todo evento como nuevo.
    """
    if not enabled:
        return True
    cur = conn.execute(
        "INSERT INTO inbox (consumer, event_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
        (consumer, event_id),
    )
    return cur.rowcount == 1


def seen(conn: psycopg.Connection, consumer: str, event_id: uuid.UUID, *, enabled: bool = True) -> bool:
    """True si el evento ya se procesó. Lectura previa: evita pagar una llamada al LLM por un
    duplicado. No sustituye a `register`, que es quien decide dentro de la transacción."""
    if not enabled:
        return False
    row = conn.execute(
        "SELECT 1 FROM inbox WHERE consumer = %s AND event_id = %s", (consumer, event_id)
    ).fetchone()
    return row is not None
