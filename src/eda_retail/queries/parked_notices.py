"""Consulta de `parked_notices` (spec cqrs-views, «Avisos aparcados»; spec web-front, «Aviso de avisos
aparcados»): solo lee `views.view_parked_notices` y `views.offsets`, nunca escribe y responde con `applied`
(offset aplicado por partición). Los avisos van por plazo, el que vence antes primero.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

import psycopg

from eda_retail.projectors.parked_notices import PROJECTOR_NAME
from eda_retail.queries.positions import applied

MAX_NOTICES = 100


@dataclass(frozen=True)
class ParkedNotice:
    incident_id: str
    order_ids: list[str]
    deadline_at: datetime
    parked_at: datetime
    last_event_id: str
    kafka_partition: int
    kafka_offset: int


@dataclass(frozen=True)
class ParkedNoticesView:
    notices: list[ParkedNotice]
    applied: dict[int, int]


def get_parked_notices(conn: psycopg.Connection, *, limit: int = MAX_NOTICES,
                       topic: str | None = None) -> ParkedNoticesView:
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        rows = conn.execute(
            "SELECT incident_id, order_ids, deadline_at, parked_at, last_event_id::text AS last_event_id, "
            "kafka_partition, kafka_offset FROM views.view_parked_notices "
            "ORDER BY deadline_at, incident_id LIMIT %s",
            (limit,),
        ).fetchall()
    return ParkedNoticesView([ParkedNotice(**r) for r in rows], position)
