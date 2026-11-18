"""E9 Caída de Kafka (spec experiments) de punta a punta: corta de verdad el broker de `docker compose` (contenedor
`KAFKA_CONTAINER`, por defecto `eda-kafka`) y lo devuelve a su estado al terminar cada corte, también si falla. Una sola
ejecución de E9 (las tres variantes) para todos los escenarios, con 12 incidencias, 4 avisos durante el corte y un
corte de 10 s. Los avisos del corte dejan filas pendientes en el outbox de `intake`, así que cada variante con corte
tiene al menos un lote sin confirmar."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e9_kafka_down import RELAY_FLUSH_TIMEOUT_S, run_e9

OUTAGE_S = 10
RUN_TIMEOUT_S = 120  # si una variante se atasca, el shard pierde 2 min y no los 5 del plazo por defecto

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e9(base_settings, seed=9, incidents=12, extra=4, outage_s=OUTAGE_S, run_timeout_s=RUN_TIMEOUT_S)


def _detail(report, variant: str) -> str:
    m = report.metrics["variants"][variant]
    keys = ("open", "escalation_reasons", "outage", "planned_at_cut", "accepted_during_outage",
            "max_pending_during_outage", "published_during_outage", "pending_at_end", "relay_errors", "commit_failures", "max_relay_turn_seconds",
            "drain_seconds",
            "messages", "outbox_rows", "log_rows", "missing_in_log", "repeated_messages", "inbox_duplicates",
            "group_lag", "consumers", "stall")
    return json.dumps({k: m.get(k) for k in keys}, default=str)  # `stall`: pila de cada hilo si quedó algo abierto


def _recovered(report, variant: str) -> None:
    m = report.metrics["variants"][variant]
    assert m["outage"]["cut_seconds"] <= OUTAGE_S + 15, _detail(report, variant)  # docker start incluido
    assert m["published_during_outage"] == 0, _detail(report, variant)
    assert sum(m["max_pending_during_outage"].values()) > 0, _detail(report, variant)
    assert sum(m["relay_errors"].values()) > 0, _detail(report, variant)
    assert sum(m["pending_at_end"].values()) == 0, _detail(report, variant)
    assert m["missing_in_log"] == 0 and m["log_rows"] == m["outbox_rows"], _detail(report, variant)


def test_broker_stopped(report):
    """Broker parado"""
    _recovered(report, "stopped")


def test_broker_paused(report):
    """Broker congelado"""
    _recovered(report, "paused")
    m = report.metrics["variants"]["paused"]
    # run_once y publish_dead_letters esperan cada uno como mucho su plazo de entrega
    assert m["max_relay_turn_seconds"] <= 2 * RELAY_FLUSH_TIMEOUT_S + 1.0, _detail(report, "paused")
    assert m["invariants"]["I2"]["violations"] == 0, _detail(report, "paused")


def test_notices_during_the_outage(report):
    """Avisos durante la caída"""
    for variant in ("stopped", "paused"):
        m = report.metrics["variants"][variant]
        assert m["accepted_during_outage"] == m["extra_notices"] == 4, _detail(report, variant)
        assert m["open"] == 0, _detail(report, variant)


def test_report_with_invariants(report):
    """Informe con invariantes"""
    for m in report.metrics["variants"].values():
        assert set(m["invariants"]) == {"I1", "I2", "I3", "I4", "I5"}
        broken = {name: inv for name, inv in m["invariants"].items() if inv["violations"] != 0}
        assert broken == {}, json.dumps(broken, default=str)


def test_h9(report):
    """Cumplimiento de H9"""
    for variant, m in report.metrics["variants"].items():
        assert m["h9"], _detail(report, variant)
    assert report.hypothesis == {"H9": True}
