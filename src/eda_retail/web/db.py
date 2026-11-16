"""Conexión del front a PostgreSQL: solo lectura (spec web-front, "Acceso de solo lectura y local").

PostgreSQL rechaza cualquier escritura en una transacción de solo lectura, así que el front no
puede escribir en `event_log.events` ni en las vistas aunque un endpoint lo intentara por error.
"""

from __future__ import annotations

import psycopg
from psycopg.rows import dict_row


def read_only_connect(database_url: str) -> psycopg.Connection:
    return psycopg.connect(
        database_url,
        row_factory=dict_row,
        autocommit=True,
        options="-c default_transaction_read_only=on",
    )
