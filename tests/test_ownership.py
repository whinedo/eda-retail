"""Escritura fuera de su esquema (spec event-backbone, "Propiedad de los datos")."""

from __future__ import annotations

from pathlib import Path

import pytest

from eda_retail.infra import ownership

SRC = Path(__file__).resolve().parents[1] / "src"


def test_writes_into_its_own_schema_are_allowed():
    source = '''
conn.execute("INSERT INTO planner.plans (id) VALUES (%s)", (1,))
conn.execute("UPDATE planner.plans SET x = 1")
conn.execute("DELETE FROM planner.inbox WHERE true")
'''
    assert ownership.foreign_writes(source, "planner") == []


@pytest.mark.parametrize(
    "statement",
    [
        "INSERT INTO inventory.orders (id) VALUES (1)",
        "UPDATE orchestrator.plan_steps SET status = 'x'",
        "DELETE FROM interpreter.inbox",
        "TRUNCATE views.view_orders",
        "insert into   events (seq) values (1)",
        "INSERT INTO outbox (seq) VALUES (1)",
    ],
)
def test_writes_into_another_schema_or_a_shared_table_are_reported(statement):
    """Un componente no escribe en tablas ajenas ni en las compartidas sin esquema"""
    source = f'conn.execute("""{statement}""")'
    assert ownership.foreign_writes(source, "planner") != []


def test_reads_of_other_schemas_are_not_writes():
    source = 'conn.execute("SELECT * FROM views.view_orders WHERE order_id = %s")'
    assert ownership.foreign_writes(source, "planner") == []


@pytest.mark.parametrize(
    "statement",
    [
        "SELECT id FROM planner.outbox WHERE published_at IS NULL FOR UPDATE SKIP LOCKED",
        "SELECT 1 FROM planner.plans p FOR UPDATE OF p",
        "SELECT 1 FROM planner.plans FOR NO KEY UPDATE",
        "INSERT INTO planner.plans (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2",
    ],
)
def test_row_locks_and_upserts_are_not_foreign_writes(statement):
    """FOR UPDATE y ON CONFLICT DO UPDATE no son escrituras en otra tabla"""
    assert ownership.foreign_writes(f'conn.execute("""{statement}""")', "planner") == []


def test_interpolated_table_targets_are_reported():
    """Un componente escribe en tablas con nombre literal: un destino interpolado no se puede comprobar"""
    source = 'conn.execute(f"INSERT INTO {qualified(\'inventory\', \'orders\')} (id) VALUES (1)")'
    assert ownership.foreign_writes(source, "planner") == ["{qualified('inventory', 'orders')}"]


def test_component_packages_only_write_their_schema():
    """Escritura fuera de su esquema: el código de cada componente solo escribe en su esquema"""
    for package, schema in ownership.COMPONENT_PACKAGES.items():
        for path in sorted((SRC / package).rglob("*.py")):
            found = ownership.foreign_writes(path.read_text(), schema)
            assert found == [], f"{path}: {found}"
