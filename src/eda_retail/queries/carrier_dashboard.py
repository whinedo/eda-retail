"""Consulta de `carrier_dashboard` (spec cqrs-views): solo lee `view_*` y
`views.offsets`, nunca escribe y responde con `applied` (offset aplicado por partición)."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg

from eda_retail.projectors.carrier_dashboard import PROJECTOR_NAME
from eda_retail.queries.positions import applied


@dataclass(frozen=True)
class CarrierRow:
    carrier: str
    incidents: int
    delays: int
    claims_filed: int
    refunds_issued: int


@dataclass(frozen=True)
class CarrierDashboard:
    carriers: list[CarrierRow]
    applied: dict[int, int]


def get_carrier_dashboard(conn: psycopg.Connection, *, topic: str | None = None) -> CarrierDashboard:
    with conn.transaction():
        position = applied(conn, PROJECTOR_NAME, topic)
        rows = conn.execute(
            "SELECT carrier, incidents, delays, claims_filed, refunds_issued "
            "FROM views.view_carrier_dashboard ORDER BY carrier"
        ).fetchall()
    return CarrierDashboard([CarrierRow(**row) for row in rows], position)
