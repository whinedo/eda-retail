"""Posición aplicada de una vista: el último offset que su proyector aplicó en cada partición
(spec cqrs-views, «Consistencia eventual declarada»).

El cliente que espera ver el efecto de un evento conoce su partición y su offset (los da la vista
del log, `event_log.events`) y sabe si la vista ya lo incluye: `applied[partición] >= offset`.
"""

from __future__ import annotations

import os

import psycopg


def events_topic(topic: str | None = None) -> str:
    return topic or os.environ.get("EVENTS_TOPIC", "eda.events")


def applied(conn: psycopg.Connection, projector: str, topic: str | None = None) -> dict[int, int]:
    """Offset aplicado por partición del topic de eventos; vacío si el proyector aún no aplicó nada."""
    rows = conn.execute(
        "SELECT kafka_partition, kafka_offset FROM views.offsets "
        "WHERE consumer = %s AND kafka_topic = %s AND kafka_offset >= 0",
        (projector, events_topic(topic)),
    ).fetchall()
    return {r["kafka_partition"]: r["kafka_offset"] for r in rows}
