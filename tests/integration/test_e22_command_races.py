"""E22 Carreras de comandos y ventana efecto–registro (spec experiments) y entrega concurrente de comandos (spec
synthetic-data), de punta a punta. Una sola ejecución de E22 para todos los escenarios, pequeña porque el CI va justo
de tiempo: 4 incidencias y la variante (a) solo con 8 hilos. Con la semilla 7, dos de las tres reservas fallan (fallos
deterministas por clave), así que hay `RevisePlan` e `issue_refund`, y una se completa."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e22_command_races import CRASH_ACTIONS, OTHER_COMMANDS, SAME_KEY_ACTIONS, run_e22

pytestmark = pytest.mark.slow


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e22(base_settings, seed=7, incidents=4, step_threads=(8,))


def _variant(report, name: str) -> dict:
    return report.metrics["variants"][name]


def _show(metrics: dict) -> str:
    return json.dumps(metrics, default=str)


def test_two_executors_at_once(report):
    """Dos ejecutores a la vez: 8 hilos entregan el mismo ExecuteStep con el mismo command_id; solo uno ejecuta el
    efecto, todos reciben la misma respuesta y se escribe un solo StepCompleted"""
    m = _variant(report, "execute_step_8")
    assert m["concurrent_deliveries"]["ExecuteStep"] > 0, _show(m)
    assert all(n <= 1 for n in m["executions_per_key"].values()), _show(m)
    assert m["distinct_responses_per_command_id"] == 1, _show(m)
    assert m["events"]["step_completed_per_key"] == 1, _show(m)


def test_concurrent_command_with_deferred_work(report):
    """Comando concurrente con trabajo diferido: entregas simultáneas del mismo CreatePlan; una fila de
    command_jobs por command_id y un solo PlanCreated por incidencia"""
    m = _variant(report, "other_commands")
    assert all(m["concurrent_deliveries"][t] > 0 for t in OTHER_COMMANDS), _show(m)
    assert m["events"]["jobs_per_command_id"] == 1, _show(m)
    assert m["events"]["plan_created_per_incident"] == 1, _show(m)
    assert m["distinct_responses_per_command_id"] == 1, _show(m)


def test_same_key_other_command_id(report):
    """Misma clave, otro command_id: el ejecutor devuelve el resultado guardado sin repetir el efecto"""
    m = _variant(report, "same_key")
    assert all(m["reinvoked_keys"][a] > 0 for a in SAME_KEY_ACTIONS), _show(m)
    assert all(n <= 1 for n in m["executions_per_key"].values()), _show(m)
    assert all(n <= 1 for n in m["events"]["effect_events_per_key"].values()), _show(m)


def test_crash_after_the_effect_and_before_recording_the_key(report):
    """Caída tras el efecto y antes de registrar la clave: el reintento usa la misma clave hacia el servicio y el
    cliente recibe un solo aviso"""
    m = _variant(report, "effect_record_crash")
    assert all(m["effect_record_crashes"][a] > 0 for a in CRASH_ACTIONS), _show(m)
    assert m["executions_per_key"]["notify_customer"] == 1 and m["executions_per_key"]["issue_refund"] == 1, _show(m)
    assert m["events"]["effect_events_per_key"]["notify_customer"] == 1, _show(m)


def test_same_command_id_with_another_payload(report):
    """Mismo command_id con otro payload: el informe anota la respuesta como observación (fuera de H22)"""
    observation = _variant(report, "other_commands")["same_command_other_payload"]
    assert set(observation) == {"first", "second", "same_response"}, _show(observation)
    assert observation["second"]["replayed"] is True, _show(observation)


def test_h22(report):
    """Cumplimiento de H22: ninguna variante con ejecuciones del servicio por encima de una por clave y la misma
    respuesta para todas las entregas de un command_id"""
    assert report.hypothesis == {"H22": True}, _show(report.metrics)
