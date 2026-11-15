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


def _columns(url: str, table: str, schema: str = "public") -> set[str]:
    with psycopg.connect(url) as c:
        rows = c.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = %s AND table_name = %s",
            (schema, table),
        ).fetchall()
    return {r[0] for r in rows}


def _public_tables(url: str) -> set[str]:
    with psycopg.connect(url) as c:
        rows = c.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"
                         " AND table_name <> 'alembic_version'").fetchall()
    return {r[0] for r in rows}


def test_upgrade_on_empty_database_leaves_only_component_schemas(scratch_db):
    """Una base vacía queda con los esquemas de los componentes y sin el modelo compartido (0017)"""
    upgrade(scratch_db)
    assert _public_tables(scratch_db) == set()
    assert {"seq", "event_id", "kafka_offset"} <= _columns(scratch_db, "events", "event_log")
    assert "trace_context" in _columns(scratch_db, "outbox", "orchestrator")


def test_upgrade_on_volume_created_by_init_script(scratch_db):
    """Un volumen creado por db/init/001_schema.sql se migra: 0017 borra el modelo compartido"""
    with psycopg.connect(scratch_db, autocommit=True) as c:
        c.execute(SCHEMA.read_text(encoding="utf-8"))
        c.execute(
            "INSERT INTO events (event_id, event_type, correlation_id, producer, payload) "
            "VALUES (gen_random_uuid(), 'X', 'I1', 'test', '{}')"
        )
    upgrade(scratch_db)
    upgrade(scratch_db)  # idempotente
    assert _public_tables(scratch_db) == set()
    with psycopg.connect(scratch_db) as c:
        assert c.execute("SELECT to_regproc('public.events_reject_mutation')").fetchone()[0] is None


def test_shared_model_migrations_still_revert_before_0017(scratch_db):
    """Hasta 0016 las migraciones se revierten; 0017 no tiene vuelta atrás"""
    upgrade(scratch_db, "0016")
    downgrade(scratch_db, "0001")
    assert "trace_context" not in _columns(scratch_db, "outbox")
    assert "seq" in _columns(scratch_db, "outbox")
    upgrade(scratch_db)
    with pytest.raises(NotImplementedError):
        downgrade(scratch_db, "0016")


def test_order_status_view_migration_reverts(scratch_db):
    """0018 crea views.view_order_status y su downgrade la borra"""
    upgrade(scratch_db)
    assert {"order_id", "incident_id", "status", "kafka_offset"} <= _columns(scratch_db, "view_order_status", "views")
    downgrade(scratch_db, "0017")
    assert _columns(scratch_db, "view_order_status", "views") == set()
    upgrade(scratch_db)
    assert "status" in _columns(scratch_db, "view_order_status", "views")


def test_parked_notices_view_migration_reverts(scratch_db):
    """0020 crea views.view_parked_notices y su downgrade la borra"""
    upgrade(scratch_db)
    assert {"incident_id", "order_ids", "deadline_at", "parked_at", "last_event_id", "kafka_partition",
            "kafka_offset"} <= _columns(scratch_db, "view_parked_notices", "views")
    downgrade(scratch_db, "0019")
    assert _columns(scratch_db, "view_parked_notices", "views") == set()
    assert "status" in _columns(scratch_db, "view_order_status", "views")
    upgrade(scratch_db)
    assert "deadline_at" in _columns(scratch_db, "view_parked_notices", "views")


def test_mail_server_migration_reverts(scratch_db):
    """0021 crea el esquema mail_server con su directorio y su buzón, y su downgrade lo borra"""
    upgrade(scratch_db)
    assert _columns(scratch_db, "customers", "mail_server") == {"customer_ref", "email"}
    assert {"message_id", "idempotency_key", "customer_ref", "to_address", "subject", "body",
            "accepted_at"} == _columns(scratch_db, "mailbox", "mail_server")
    downgrade(scratch_db, "0020")
    assert _columns(scratch_db, "mailbox", "mail_server") == set()
    assert "deadline_at" in _columns(scratch_db, "view_parked_notices", "views")
    upgrade(scratch_db)
    assert "to_address" in _columns(scratch_db, "mailbox", "mail_server")


def test_command_jobs_migration_reverts(scratch_db):
    """0019 crea command_jobs en los componentes con trabajo largo y su downgrade la borra"""
    from eda_retail.infra.schemas import JOB_COMPONENTS

    upgrade(scratch_db)
    for schema in JOB_COMPONENTS:
        assert {"job_id", "command_id", "status", "lease_until", "trace_context"} <= _columns(
            scratch_db, "command_jobs", schema)
    downgrade(scratch_db, "0018")
    assert all(_columns(scratch_db, "command_jobs", schema) == set() for schema in JOB_COMPONENTS)
    upgrade(scratch_db)
    assert "outcome" in _columns(scratch_db, "command_jobs", "planner")


def test_deadlines_start_at_the_first_send_and_the_migration_reverts(scratch_db):
    """0019 guarda el plazo sin vencimiento hasta el primer envío; su downgrade vuelve a exigir expires_at"""
    upgrade(scratch_db)
    assert "timeout_s" in _columns(scratch_db, "deadlines", "orchestrator")
    command_id = uuid.uuid4()
    with psycopg.connect(scratch_db) as c:
        c.execute("INSERT INTO orchestrator.command_outbox (command_id, incident_id, command_type, target, payload)"
                  " VALUES (%s, 'I-1', 'CreatePlan', 'planner', '{}')", (command_id,))
        c.execute("INSERT INTO orchestrator.deadlines (command_id, incident_id, kind, timeout_s)"
                  " VALUES (%s, 'I-1', 'create_plan', 300)", (command_id,))
    downgrade(scratch_db, "0018")
    assert "timeout_s" not in _columns(scratch_db, "deadlines", "orchestrator")
    with psycopg.connect(scratch_db) as c:
        expires = c.execute("SELECT expires_at FROM orchestrator.deadlines WHERE command_id = %s",
                            (command_id,)).fetchone()[0]
    assert expires is not None
    upgrade(scratch_db)
