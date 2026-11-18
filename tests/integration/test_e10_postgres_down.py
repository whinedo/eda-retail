"""E10 Caída de PostgreSQL (spec experiments) de punta a punta: con todos los componentes en procesos propios, corta de
verdad la base de `docker compose` (contenedor `POSTGRES_CONTAINER`, por defecto `eda-postgres`) y la devuelve a su
estado al terminar cada corte, también si falla. Una sola ejecución de E10 (las tres variantes) para todos los
escenarios, con 8 incidencias, 4 avisos durante el corte y un corte de 10 s."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e10_postgres_down import run_e10
from eda_retail.experiments.processes import COMPONENTS

OUTAGE_S = 10
EXTRA = 4
RUN_TIMEOUT_S = 120  # si una variante se atasca, el shard pierde 2 min y no los 5 del plazo por defecto

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e10(base_settings, seed=10, incidents=8, extra=EXTRA, outage_s=OUTAGE_S, run_timeout_s=RUN_TIMEOUT_S)


def _detail(report, variant: str) -> str:
    m = report.metrics["variants"][variant]
    keys = ("open", "escalation_reasons", "outage", "planned_at_cut", "notices", "db_probe_failures_during_outage",
            "harness_reconnected", "processes", "process_deaths", "exit_codes", "drain_seconds", "sample_errors",
            "pending_at_end", "outbox_rows", "log_rows", "missing_in_log", "log_caught_up", "executions",
            "invariants")
    return json.dumps({k: m.get(k) for k in keys}, default=str)[-8000:]


def _recovered(report, variant: str) -> None:
    m = report.metrics["variants"][variant]
    assert m["outage"]["cut_seconds"] <= OUTAGE_S + 15, _detail(report, variant)  # docker start incluido
    assert m["process_deaths"] == {}, _detail(report, variant)
    assert sum(m["pending_at_end"].values()) == 0, _detail(report, variant)
    assert m["missing_in_log"] == 0 and m["log_rows"] == m["outbox_rows"], _detail(report, variant)
    assert m["executions"]["keys"] > 0 and m["executions"]["max_per_key"] == 1, _detail(report, variant)


def test_database_stopped(report):
    """Base parada"""
    _recovered(report, "stopped")
    errors = report.metrics["variants"]["stopped"]["connection_errors"]
    assert set(errors) == {c.name for c in COMPONENTS} and all(n > 0 for n in errors.values()), \
        _detail(report, "stopped")


def test_database_paused(report):
    """Base congelada"""
    _recovered(report, "paused")
    m = report.metrics["variants"]["paused"]
    assert m["db_probe_failures_during_outage"] > 0, _detail(report, "paused")
    assert m["invariants"]["I2"]["violations"] == 0, _detail(report, "paused")


def test_notices_during_the_database_outage(report):
    """Avisos durante la caída de la base"""
    for variant in ("stopped", "paused"):
        m = report.metrics["variants"][variant]
        assert m["notices"]["rejected_during_outage"] > 0, _detail(report, variant)
        assert m["notices"]["accepted"] == m["extra_notices"] == EXTRA, _detail(report, variant)
        assert m["open"] == 0, _detail(report, variant)


def test_processes_at_the_end_of_e10(report):
    """Procesos al terminar E10"""
    for variant, m in report.metrics["variants"].items():
        assert m["process_deaths"] == {}, _detail(report, variant)
        assert m["exit_codes"] == {c.name: 0 for c in COMPONENTS}, _detail(report, variant)


def test_e10_report_with_invariants(report):
    for variant, m in report.metrics["variants"].items():
        assert set(m["invariants"]) == {"I1", "I2", "I3", "I4", "I5"}
        broken = {name: inv for name, inv in m["invariants"].items() if inv["violations"] != 0}
        assert broken == {}, _detail(report, variant)


def test_h10(report):
    """Cumplimiento de H10"""
    for variant, m in report.metrics["variants"].items():
        assert m["h10"], _detail(report, variant)
    assert report.hypothesis == {"H10": True}
