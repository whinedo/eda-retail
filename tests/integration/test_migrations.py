"""Migraciones con Alembic: base sobre volúmenes existentes y columna trace_context."""

from __future__ import annotations

import uuid
from pathlib import Path

import psycopg
import pytest

from eda_retail.infra.migrations import downgrade, upgrade

SCHEMA = Path(__file__).resolve().parents[2] / "db" / "init" / "001_schema.sql"


@pytest.fixture
def scratch_db(database_url: str):
    """Base de datos vacía y desechable para probar migraciones."""
    name = f"eda_mig_{uuid.uuid4().hex[:8]}"
    base, _, _ = database_url.rpartition("/")
    with psycopg.connect(f"{base}/postgres", autocommit=True) as admin:
        admin.execute(f"CREATE DATABASE {name}")
    try:
        yield f"{base}/{name}"
    finally:
        with psycopg.connect(f"{base}/postgres", autocommit=True) as admin:
            admin.execute(f"DROP DATABASE IF EXISTS {name} WITH (FORCE)")


def _columns(url: str, table: str) -> set[str]:
    with psycopg.connect(url) as c:
        rows = c.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = %s",
            (table,),
        ).fetchall()
    return {r[0] for r in rows}


def test_upgrade_on_empty_database_creates_schema_and_trace_column(scratch_db):
    """Una base vacía queda con el esquema completo y outbox.trace_context"""
    upgrade(scratch_db)
    assert "seq" in _columns(scratch_db, "events")
    assert "trace_context" in _columns(scratch_db, "outbox")
    assert "trace_context" not in _columns(scratch_db, "events")  # el log no cambia


def test_upgrade_on_volume_created_by_init_script(scratch_db):
    """Un volumen creado por db/init/001_schema.sql se migra sin recrear tablas"""
    with psycopg.connect(scratch_db, autocommit=True) as c:
        c.execute(SCHEMA.read_text(encoding="utf-8"))
        c.execute(
            "INSERT INTO events (event_id, event_type, correlation_id, producer, payload) "
            "VALUES (gen_random_uuid(), 'X', 'I1', 'test', '{}')"
        )
    upgrade(scratch_db)
    upgrade(scratch_db)  # idempotente
    assert "trace_context" in _columns(scratch_db, "outbox")
    with psycopg.connect(scratch_db) as c:
        assert c.execute("SELECT count(*) FROM events").fetchone()[0] == 1


def test_downgrade_removes_trace_column(scratch_db):
    """La migración de trace_context se puede revertir"""
    upgrade(scratch_db)
    downgrade(scratch_db, "0001")
    assert "trace_context" not in _columns(scratch_db, "outbox")
    assert "seq" in _columns(scratch_db, "outbox")
