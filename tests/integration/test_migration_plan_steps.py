"""Migración 0003: step_id único por incidencia, no global."""

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


def _incident(c: psycopg.Connection, incident: str) -> None:
    c.execute(
        "INSERT INTO incidents (incident_id, status, severity, cause) VALUES (%s, 'open', 'high', 'loss')",
        (incident,),
    )


def test_same_step_id_in_two_incidents(conn):
    """Dos incidencias pueden tener un paso con el mismo step_id"""
    a, b = f"I-{uuid.uuid4().hex[:8]}", f"I-{uuid.uuid4().hex[:8]}"
    with conn.transaction():
        for incident in (a, b):
            _incident(conn, incident)
            conn.execute(
                "INSERT INTO plan_steps (incident_id, step_id, origin_plan_version, action) "
                "VALUES (%s, 'S1', 1, 'notify_customer')",
                (incident,),
            )
    rows = conn.execute(
        "SELECT count(*) AS n FROM plan_steps WHERE step_id = 'S1' AND incident_id = ANY(%s)", ([a, b],)
    ).fetchone()
    assert rows["n"] == 2


def test_step_id_unique_within_incident(conn):
    """Un step_id no se repite dentro de la misma incidencia"""
    incident = f"I-{uuid.uuid4().hex[:8]}"
    with conn.transaction():
        _incident(conn, incident)
        conn.execute(
            "INSERT INTO plan_steps (incident_id, step_id, origin_plan_version, action) VALUES (%s, 'S1', 1, 'notify_customer')",
            (incident,),
        )
    with pytest.raises(psycopg.errors.UniqueViolation), conn.transaction():
        conn.execute(
            "INSERT INTO plan_steps (incident_id, step_id, origin_plan_version, action) VALUES (%s, 'S1', 2, 'file_claim')",
            (incident,),
        )


def test_compensation_references_step_of_same_incident(conn):
    """compensates apunta a un paso de la misma incidencia"""
    a, b = f"I-{uuid.uuid4().hex[:8]}", f"I-{uuid.uuid4().hex[:8]}"
    with conn.transaction():
        _incident(conn, a)
        _incident(conn, b)
        conn.execute(
            "INSERT INTO plan_steps (incident_id, step_id, origin_plan_version, action) VALUES (%s, 'S3', 1, 'reserve_stock')",
            (a,),
        )
        conn.execute(
            "INSERT INTO plan_steps (incident_id, step_id, origin_plan_version, action, compensates) "
            "VALUES (%s, 'S3-comp', 1, 'release_stock', 'S3')",
            (a,),
        )
    with pytest.raises(psycopg.errors.ForeignKeyViolation), conn.transaction():
        conn.execute(
            "INSERT INTO plan_steps (incident_id, step_id, origin_plan_version, action, compensates) "
            "VALUES (%s, 'S3-comp', 1, 'release_stock', 'S3')",
            (b,),  # S3 no existe en b
        )


def test_migration_0003_round_trip(scratch_db):
    """La migración 0003 se aplica y se revierte"""
    upgrade(scratch_db)
    with psycopg.connect(scratch_db) as c:
        pk = c.execute(
            "SELECT array_agg(a.attname ORDER BY a.attname) FROM pg_index i "
            "JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) "
            "WHERE i.indrelid = 'plan_steps'::regclass AND i.indisprimary"
        ).fetchone()[0]
    assert pk == ["incident_id", "step_id"]
    downgrade(scratch_db, "0002")
    upgrade(scratch_db)
