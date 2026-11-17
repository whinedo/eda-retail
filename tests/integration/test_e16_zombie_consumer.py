"""E16 Consumidor zombie y rebalanceo (spec experiments) y congelación de un proceso (spec synthetic-data), de punta a
punta en modo multiproceso con dos instancias reales del orquestador. Una sola ejecución, pequeña por el tiempo del
CI (10 incidencias en lugar de 50). Las métricas van en el mensaje de cada assert: el CI no sube informes."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e16_zombie_consumer import run_e16

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e16(base_settings, seed=7, incidents=10)


def _show(metrics: dict) -> str:
    return json.dumps({k: v for k, v in metrics.items() if k != "overlaps"} | {
        "overlapping_incidents": [i for i, v in metrics["overlaps"]["by_incident"].items() if v["overlap"]]},
        default=str)


def test_zombie_consumer(report):
    """Consumidor zombie: con dos instancias en el mismo grupo y una congelada más que el timeout de sesión, el
    experimento registra que Kafka había reasignado sus particiones antes de que reanudara y qué mensajes procesó
    después"""
    m = report.metrics
    assert m["rebalance"]["victim_partitions"], _show(m)
    assert m["rebalance"]["reassigned_before_resume"] is True, _show(m)
    assert isinstance(m["zombie"]["processed_after_resume"], list), _show(m)


def test_rebalance_while_the_instance_is_frozen(report):
    """Rebalanceo mientras la instancia está congelada: B procesa los mensajes pendientes de esas particiones y el
    informe registra el instante del rebalanceo"""
    m = report.metrics
    moved = m["rebalance"]
    assert moved["reassigned_after_s"] is not None, _show(m)
    assert 0 < moved["reassigned_after_s"] < moved["resumed_after_s"], _show(m)
    assert m["taken_over"]["messages"] > 0, _show(m)


def test_resumed_zombie(report):
    """Reanudación del zombie: el inbox descarta el mensaje si B ya lo registró, o la confirmación falla sin dejar
    estado a medias, y `orchestrator.plan_steps` no retrocede ningún paso de `completed` a otro estado"""
    m = report.metrics
    for item in m["double_processed"]:
        assert list(item["outcomes"].values()).count("processed") <= 1, _show(m)
    assert m["invalid_transitions"] == [], _show(m)
    assert m["invariants"]["I1"]["violations"] == 0 and m["invariants"]["I4"]["violations"] == 0, _show(m)


def test_h16_holds(report):
    """Cumplimiento de H16: no hay dos comandos con `command_id` distinto para el mismo paso y la misma versión del
    plan, ni transiciones inválidas, y I1 a I5 no tienen violaciones"""
    m = report.metrics
    assert m["duplicate_commands"] == [], _show(m)
    assert m["open"] == 0, _show(m)
    assert report.hypothesis["H16"] is True, _show(m)


def test_report_gives_overlaps_and_lock_waits_per_incident(report):
    """El informe da, por incidencia, si dos instancias procesaron mensajes de ella solapados en el tiempo y qué
    transacciones esperaron al bloqueo de la incidencia"""
    m = report.metrics
    by_incident = m["overlaps"]["by_incident"]
    assert 0 < len(by_incident) <= 10, _show(m)
    assert all(set(v) == {"instances", "overlap"} for v in by_incident.values()), _show(m)
    assert isinstance(m["lock_waits"], list), _show(m)
