"""Consulta de `carrier_dashboard` (spec cqrs-views): solo lee `view_*` y
`projector_offsets`, nunca escribe y responde con `last_applied_seq`."""

from __future__ import annotations

from dataclasses import dataclass

import psycopg

from eda_retail.projectors.carrier_dashboard import PROJECTOR_NAME
from eda_retail.queries.positions import last_applied_seq


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
    last_applied_seq: int


def get_carrier_dashboard(conn: psycopg.Connection) -> CarrierDashboard:
    with conn.transaction():
        position = last_applied_seq(conn, PROJECTOR_NAME)
        rows = conn.execute(
            "SELECT carrier, incidents, delays, claims_filed, refunds_issued "
            "FROM view_carrier_dashboard ORDER BY carrier"
        ).fetchall()
    return CarrierDashboard([CarrierRow(**row) for row in rows], position)
