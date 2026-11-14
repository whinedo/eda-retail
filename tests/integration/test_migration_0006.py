"""Migración 0006: vistas de coste y del panel por transportista (fase 5)."""

from __future__ import annotations

import uuid

import psycopg
import pytest

from eda_retail.infra.migrations import downgrade, upgrade

_COLUMNS = (
    "SELECT table_name, column_name FROM information_schema.columns "
    "WHERE table_name IN ('view_cost_per_incident', 'view_carrier_dashboard')"
)
_NEW = {
    ("view_cost_per_incident", "claims"),
    ("view_cost_per_incident", "reissues"),
    ("view_cost_per_incident", "refunds_amount"),
    ("view_cost_per_incident", "last_seq"),
    ("view_carrier_dashboard", "incidents"),
    ("view_carrier_dashboard", "last_seq"),
}
_DROPPED = {("view_cost_per_incident", "llm_cost"), ("view_cost_per_incident", "effects_cost")}


@pytest.fixture
def scratch_db(database_url: str):
    name = f"eda_mig_{uuid.uuid4().hex[:8]}"
    base, _, _ = database_url.rpartition("/")
    with psycopg.connect(f"{base}/postgres", autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name}")
    try:
        yield f"{base}/{name}"
    finally:
        with psycopg.connect(f"{base}/postgres", autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def _state(url: str) -> tuple[set, str | None]:
    with psycopg.connect(url) as c:
        cols = set(c.execute(_COLUMNS).fetchall())
        aux = c.execute("SELECT to_regclass('view_carrier_incidents')").fetchone()[0]
    return cols, aux


def test_migration_0006_round_trip(scratch_db):
    """La migración 0006 se aplica y se revierte"""
    upgrade(scratch_db)
    cols, aux = _state(scratch_db)
    assert _NEW <= cols and not cols & _DROPPED
    assert aux == "view_carrier_incidents"
    downgrade(scratch_db, "0005")
    cols, aux = _state(scratch_db)
    assert _DROPPED <= cols and not cols & _NEW
    assert aux is None
    upgrade(scratch_db)
    assert _state(scratch_db) == (cols - _DROPPED | _NEW, "view_carrier_incidents")


def test_view_counters_start_at_zero(conn):
    """Una fila nueva de las vistas de coste y del panel empieza con los contadores a cero"""
    incident, carrier = f"I-{uuid.uuid4().hex[:8]}", f"c-{uuid.uuid4().hex[:8]}"
    with conn.transaction():
        conn.execute("INSERT INTO view_cost_per_incident (incident_id) VALUES (%s)", (incident,))
        conn.execute("INSERT INTO view_carrier_dashboard (carrier) VALUES (%s)", (carrier,))
    cost = conn.execute(
        "SELECT claims, reissues, refunds_amount, last_seq FROM view_cost_per_incident WHERE incident_id = %s",
        (incident,),
    ).fetchone()
    board = conn.execute(
        "SELECT incidents, last_seq FROM view_carrier_dashboard WHERE carrier = %s", (carrier,)
    ).fetchone()
    assert (cost["claims"], cost["reissues"], cost["refunds_amount"], cost["last_seq"]) == (0, 0, 0, 0)
    assert (board["incidents"], board["last_seq"]) == (0, 0)
