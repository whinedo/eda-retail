"""Migración 0008: un esquema por componente (spec event-backbone, "Propiedad de los datos")."""

from __future__ import annotations

import uuid

import psycopg
import pytest

from eda_retail.infra.migrations import downgrade, upgrade
from eda_retail.infra.schemas import COMPONENTS, JOB_COMPONENTS


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


def _tables(url: str) -> set[str]:
    with psycopg.connect(url) as c:
        rows = c.execute(
            "SELECT table_schema || '.' || table_name FROM information_schema.tables"
            " WHERE table_schema = ANY(%s)",
            (list(COMPONENTS),),
        ).fetchall()
    return {r[0] for r in rows}


def test_upgrade_creates_every_component_schema(scratch_db):
    """Cada componente tiene su esquema con su outbox, inbox y dead letters"""
    upgrade(scratch_db)
    tables = _tables(scratch_db)
    for name, component in COMPONENTS.items():
        if component.publishes:
            assert {f"{name}.outbox", f"{name}.inbox", f"{name}.dead_letters"} <= tables
        assert (f"{name}.effect_keys" in tables) == component.effect_keys
        assert (f"{name}.command_jobs" in tables) == (name in JOB_COMPONENTS)


def test_no_foreign_key_crosses_schemas(scratch_db):
    """Sin claves foráneas entre tablas de componentes distintos"""
    upgrade(scratch_db)
    with psycopg.connect(scratch_db) as c:
        crossing = c.execute(
            """
            SELECT tc.table_schema, ccu.table_schema
            FROM information_schema.table_constraints tc
            JOIN information_schema.constraint_column_usage ccu USING (constraint_name, constraint_schema)
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema <> ccu.table_schema
            """
        ).fetchall()
    assert crossing == []


def test_upgrade_is_idempotent_and_downgrade_drops_the_schemas(scratch_db):
    upgrade(scratch_db, "0016")  # 0017 borra el modelo compartido y no tiene vuelta atrás
    downgrade(scratch_db, "0007")
    assert _tables(scratch_db) == set()
    upgrade(scratch_db)
    assert "planner.outbox" in _tables(scratch_db)
