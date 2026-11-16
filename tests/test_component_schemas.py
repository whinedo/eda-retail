"""Un esquema por componente (spec event-backbone, "Propiedad de los datos"; design.md §2).

Sin PostgreSQL: se comprueba el DDL que genera `infra.schemas`. La migración contra la base
real está en tests/integration/test_component_schemas.py.
"""

from __future__ import annotations

import re

import pytest

from eda_retail.infra import schemas


def _tables(statements: list[str]) -> set[str]:
    return {m.group(1) for s in statements for m in re.finditer(r"CREATE TABLE IF NOT EXISTS (\S+)", s)}


def test_every_component_of_the_design_has_a_schema():
    """Cada componente tiene su propio esquema (design.md §2)"""
    assert set(schemas.COMPONENTS) == {
        "intake",
        "interpreter",
        "planner",
        "replanner",
        "orchestrator",
        "executor_notify_customer",
        "executor_file_claim",
        "executor_reissue_shipment",
        "executor_issue_refund",
        "inventory",
        "event_log",
        "views",
        "mail_server",
    }


def test_writing_component_gets_outbox_inbox_and_dead_letters_in_its_schema():
    """El componente escribe su outbox, su inbox y sus dead letters en su esquema"""
    statements = schemas.ddl("planner")
    assert statements[0] == "CREATE SCHEMA IF NOT EXISTS planner"
    assert _tables(statements) == {"planner.outbox", "planner.inbox", "planner.dead_letters"}


def test_effect_keys_only_for_components_that_execute_effects():
    """Solo los ejecutores e inventory tienen effect_keys (design.md §2)"""
    with_keys = {name for name, c in schemas.COMPONENTS.items() if c.effect_keys}
    assert with_keys == {
        "executor_notify_customer",
        "executor_file_claim",
        "executor_reissue_shipment",
        "executor_issue_refund",
        "inventory",
    }
    assert "inventory.effect_keys" in _tables(schemas.ddl("inventory"))


def test_query_components_only_get_their_schema():
    """event_log y views no publican eventos: sus tablas llegan con el lado de lectura (T3)"""
    assert schemas.ddl("views") == ["CREATE SCHEMA IF NOT EXISTS views"]
    assert schemas.ddl("event_log") == ["CREATE SCHEMA IF NOT EXISTS event_log"]
    assert schemas.ddl("mail_server") == ["CREATE SCHEMA IF NOT EXISTS mail_server"]  # sus tablas, en la 0021


def test_no_table_references_another_schema():
    """Sin claves foráneas ni relaciones entre esquemas"""
    for name in schemas.COMPONENTS:
        for statement in schemas.ddl(name):
            assert "REFERENCES" not in statement
            for qualified in re.findall(r"\b([a-z_]+)\.[a-z_]+\b", statement):
                assert qualified == name, statement


def test_inbox_keeps_the_kind_and_the_command_response():
    """El inbox deduplica eventos y comandos y guarda la respuesta del comando"""
    inbox = " ".join(next(s for s in schemas.ddl("planner") if "planner.inbox" in s).split())
    for column in ("message_id UUID PRIMARY KEY", "kind TEXT NOT NULL", "status_code INTEGER", "response JSONB"):
        assert column in inbox


@pytest.mark.parametrize("bad", ["", "Planner", "planner;drop", "1planner", "executor-file_claim"])
def test_invalid_schema_names_are_rejected(bad):
    with pytest.raises(ValueError):
        schemas.ddl(bad)


def test_unknown_component_is_rejected():
    with pytest.raises(ValueError):
        schemas.ddl("billing")
