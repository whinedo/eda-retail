"""Incidencia cerrada según la vista `incident_status` (spec incident-planning, «Trabajo de una incidencia ya
cerrada»). Sin PostgreSQL: una conexión falsa que responde a la única lectura."""

from __future__ import annotations

import contextlib

import pytest

from eda_retail.queries.incident_status import incident_closed


class _Conn:
    def __init__(self, row):
        self.row = row
        self.sql: list[str] = []

    @contextlib.contextmanager
    def transaction(self):
        yield

    def execute(self, sql, params):
        self.sql.append(sql)
        return self

    def fetchone(self):
        return self.row


@pytest.mark.parametrize(("row", "closed"), [
    ({"status": "resolved"}, True),
    ({"status": "escalated"}, True),
    ({"status": "open"}, False),
    ({"status": "in_progress"}, False),
    (None, False),  # la proyección aún no tiene la incidencia (consistencia eventual): se planifica
])
def test_only_resolved_or_escalated_incidents_are_closed(row, closed):
    """Trabajo de una incidencia ya cerrada: solo resolved o escalated en la vista cierran la incidencia"""
    conn = _Conn(row)
    assert incident_closed(conn, "I-1") is closed
    assert all("views.view_incident_status" in sql for sql in conn.sql)
