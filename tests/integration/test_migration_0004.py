"""Migración 0004: intentos por paso, step_id por incidencia en efectos, envíos y dead letters."""

from __future__ import annotations

import uuid

import psycopg
import pytest

from eda_retail.infra.migrations import downgrade, upgrade


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


def _ids() -> tuple[str, str]:
    return f"I-{uuid.uuid4().hex[:8]}", f"I-{uuid.uuid4().hex[:8]}"


def test_plan_steps_attempts_default_zero(conn):
    """Un paso nuevo empieza con cero intentos"""
    incident, _ = _ids()
    with conn.transaction():
        conn.execute(
            "INSERT INTO incidents (incident_id, status, severity, cause) VALUES (%s, 'open', 'high', 'loss')",
            (incident,),
        )
        conn.execute(
            "INSERT INTO plan_steps (incident_id, step_id, origin_plan_version, action) VALUES (%s, 'S1', 1, 'file_claim')",
            (incident,),
        )
    row = conn.execute("SELECT attempts FROM plan_steps WHERE incident_id = %s", (incident,)).fetchone()
    assert row["attempts"] == 0


@pytest.mark.parametrize(
    ("table", "columns", "values"),
    [
        ("refunds", "(refund_id, incident_id, order_id, step_id, amount)", "(%s, %s, 'O1', 'S4', 10)"),
        ("customer_messages", "(message_id, incident_id, order_id, step_id)", "(%s, %s, 'O1', 'S1')"),
        ("shipments", "(shipment_id, incident_id, order_id, step_id)", "(%s, %s, 'O1', 'S3')"),
    ],
)
def test_effect_step_id_unique_per_incident(conn, table, columns, values):
    """El mismo step_id puede tener un efecto en dos incidencias, pero solo uno por incidencia"""
    a, b = _ids()
    with conn.transaction():
        for incident in (a, b):
            conn.execute(f"INSERT INTO {table} {columns} VALUES {values}", (uuid.uuid4().hex, incident))
    with pytest.raises(psycopg.errors.UniqueViolation), conn.transaction():
        conn.execute(f"INSERT INTO {table} {columns} VALUES {values}", (uuid.uuid4().hex, a))


def test_dead_letters_pending_publication(conn):
    """Una dead letter nueva queda pendiente de publicar en eda.dlq"""
    with conn.transaction():
        row = conn.execute(
            "INSERT INTO dead_letters (consumer, reason) VALUES ('test', 'invalid_schema') RETURNING published_at"
        ).fetchone()
    assert row["published_at"] is None


def test_migration_0004_round_trip(scratch_db):
    """La migración 0004 se aplica y se revierte"""
    upgrade(scratch_db)
    with psycopg.connect(scratch_db) as c:
        assert c.execute("SELECT to_regclass('shipments')").fetchone()[0] == "shipments"
    downgrade(scratch_db, "0003")
    with psycopg.connect(scratch_db) as c:
        assert c.execute("SELECT to_regclass('shipments')").fetchone()[0] is None
        cols = set(
            c.execute(
                "SELECT table_name, column_name FROM information_schema.columns "
                "WHERE table_name IN ('refunds', 'customer_messages', 'plan_steps', 'dead_letters')"
            ).fetchall()
        )
        assert not cols & {
            ("plan_steps", "attempts"),
            ("refunds", "incident_id"),
            ("customer_messages", "incident_id"),
            ("dead_letters", "published_at"),
        }
    upgrade(scratch_db)
