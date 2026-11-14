"""Posición de un proyector: el último `seq` aplicado a su vista (consistencia eventual)."""

from __future__ import annotations

import psycopg


def last_applied_seq(conn: psycopg.Connection, projector: str) -> int:
    row = conn.execute("SELECT last_seq FROM projector_offsets WHERE projector = %s", (projector,)).fetchone()
    return row["last_seq"] if row else 0
