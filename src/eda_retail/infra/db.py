"""Conexión a PostgreSQL con psycopg 3."""

from __future__ import annotations

import psycopg
from psycopg.rows import dict_row


def connect(database_url: str) -> psycopg.Connection:
    """Abre una conexión sin autocommit: cada `with conn.transaction()` es una transacción."""
    return psycopg.connect(database_url, row_factory=dict_row, autocommit=False)
