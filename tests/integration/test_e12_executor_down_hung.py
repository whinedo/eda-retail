"""E12 Ejecutor caído y ejecutor colgado (spec experiments) y ejecutor caído o colgado (spec synthetic-data), de punta a
punta en proceso con el dispatcher en su propio hilo. Una sola ejecución de E12 (las cinco variantes) para todos los
escenarios, pequeña por el tiempo del CI: la semilla 7 con 6 incidencias da 2 con `file_claim` y 3 con
`reserve_stock`."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e12_executor_down_hung import run_e12

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e12(base_settings, seed=7, incidents=6)


def _variant(report, name: str) -> dict:
    return report.metrics["variants"][name]


def _show(metrics: dict) -> str:
    return json.dumps(metrics, default=str)


def test_executor_down_forever(report):
    """Ejecutor caído para siempre: al vencer el plazo, cada `ExecuteStep` de `file_claim` pasa a `failed` con
    `timeout` y la saga revisa el plan o escala; las incidencias sin `file_claim` terminan como sin fallo"""
    m, base = _variant(report, "down"), _variant(report, "baseline")
    assert m["file_claim_commands"]["commands"] > 0, _show(m)
    assert set(m["file_claim_steps"]) == {"failed:timeout"}, _show(m)
    assert m["open"] == 0, _show(m)
    assert m["incidents"]["unaffected"]["status"] == base["incidents"]["unaffected"]["status"], _show(m)
    assert set(base["incidents"]["unaffected"]["status"].values()) == {"resolved"}, _show(base)
    assert m["h12a"], _show(m)


def test_hung_executor(report):
    """Ejecutor colgado: el informe da los intentos del dispatcher por comando, el tiempo que esperó a este ejecutor y
    la latencia p95 de las incidencias que no lo usan; el plazo empieza en el primer envío, no en el encolado"""
    m = _variant(report, "hang")
    commands = m["file_claim_commands"]
    assert commands["attempts_max"] > 1 and commands["dispatcher_wait_s"] > 0, _show(m)
    assert m["incidents"]["unaffected"]["p95_latency_s"] is not None, _show(m)
    assert commands["deadline_start_after_enqueue_s"]["min"] >= 0, _show(m)
    assert m["h12a"], _show(m)


def test_executor_recovery(report):
    """Recuperación del ejecutor: los resultados llegan obsoletos o a pasos aún vigentes y ningún efecto se ejecuta
    dos veces (I2)"""
    m = _variant(report, "hang_recovery")
    assert sum(m["recovered_results"].values()) > 0, _show(m)
    assert set(m["recovered_results"]) <= {"stale", "current"}, _show(m)
    assert m["invariants"]["I2"]["violations"] == 0, _show(m)
    assert m["h12a"], _show(m)


def test_compensation_of_a_step_with_an_expired_deadline(report):
    """Compensación de un paso con plazo vencido: con `inventory` caído al vencer el plazo de `reserve_stock`, la
    compensación se invoca con el sufijo `-comp` y se completa cuando el ejecutor vuelve, sin liberar dos veces"""
    m = _variant(report, "compensation")
    c = m["compensation"]
    assert c["compensations"] > 0 and c["with_comp_suffix"] == c["compensations"], _show(m)
    assert c["status"] == {"completed": c["compensations"]}, _show(m)
    assert c["release_attempts_max"] > 1 and c["releases_per_reservation"] <= 1, _show(m)
    assert m["h12a"], _show(m)


def test_h12(report):
    """H12a (vivacidad) se cumple; H12b es exploratoria: si no se cumple, el informe lo dice como un hallazgo"""
    assert report.hypothesis["H12a"] is True, _show(report.metrics)
    assert (report.metrics["h12b_note"] is None) == report.hypothesis["H12b"], _show(report.metrics)
