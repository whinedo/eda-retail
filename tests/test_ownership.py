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


def test_component_packages_only_write_their_schema():
    """Escritura fuera de su esquema: el código de cada componente solo escribe en su esquema"""
    for package, schema in ownership.COMPONENT_PACKAGES.items():
        for path in sorted((SRC / package).rglob("*.py")):
            found = ownership.foreign_writes(path.read_text(), schema)
            assert found == [], f"{path}: {found}"
