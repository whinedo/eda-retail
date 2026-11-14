"""Migración 0005: `plan_steps.result`, el resultado de un paso completado."""

from __future__ import annotations

import uuid

import psycopg
import pytest

from eda_retail.infra.migrations import downgrade, upgrade

_RESULT_COLUMN = (
    "SELECT data_type FROM information_schema.columns "
    "WHERE table_name = 'plan_steps' AND column_name = 'result'"
)


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


def test_migration_0005_round_trip(scratch_db):
    """La migración 0005 se aplica y se revierte"""
    upgrade(scratch_db)
    with psycopg.connect(scratch_db) as c:
        assert c.execute(_RESULT_COLUMN).fetchone() == ("jsonb",)
    downgrade(scratch_db, "0004")
    with psycopg.connect(scratch_db) as c:
        assert c.execute(_RESULT_COLUMN).fetchone() is None
        assert c.execute("SELECT version_num FROM alembic_version").fetchone() == ("0004",)
    upgrade(scratch_db)
    with psycopg.connect(scratch_db) as c:
        assert c.execute(_RESULT_COLUMN).fetchone() == ("jsonb",)
