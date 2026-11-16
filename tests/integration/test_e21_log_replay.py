"""E21 Reproducción total del log (spec experiments) y reinicio de offsets (spec synthetic-data), de punta a
punta con el LLM oráculo. Una sola ejecución de E21 para todos los escenarios."""

from __future__ import annotations

import pytest

from eda_retail.experiments.e21_replay import run_e21

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e21(base_settings, seed=21, incidents=8)


def test_group_reset_to_the_beginning(report):
    """Grupo reiniciado al principio"""
    replay = report.metrics["replay"]
    assert replay["received"] and all(n == replay["messages"] for n in replay["received"].values())
    assert replay["lag"] == 0


def test_replay_with_inbox(report):
    """Reproducción con inbox activo"""
    assert report.metrics["closed"] == 8 and report.metrics["replans"] > 0
    changes = report.metrics["replay"]["changes"]
    assert "executions" not in changes
    assert not any(t.endswith(("outbox", "command_outbox")) for t in changes.get("rows", {}))


def test_state_unchanged(report):
    """Estado intacto"""
    replay = report.metrics["replay"]
    assert replay["changes"] == {} and not replay["changed"]
    assert replay["inbox_registered"] == replay["inbox_discarded"] and replay["inbox_matches"]


def test_executors_answer_old_commands(report):
    """Ejecutores activos ante comandos antiguos"""
    old = report.metrics["old_commands"]
    assert old["redelivered"] > 0 and old["not_replayed"] == 0


def test_control_without_inbox(report):
    """Control sin inbox"""
    assert report.metrics["controls"]["orchestrator"]["changed"]


def test_h21_met(report):
    """Cumplimiento de H21"""
    assert report.hypothesis == {"H21": True}
