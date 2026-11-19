"""E8 Matriz de puntos de caída (spec experiments) de punta a punta, en proceso con el oráculo. Una sola ejecución de E8
para todos los escenarios, pequeña porque el CI va justo de tiempo: `baseline` y una celda por punto (5 ejecuciones de 8
incidencias). Con la semilla 8, tres de las cuatro reservas fallan (fallos deterministas por clave), así que hay
`RevisePlan` e `issue_refund`. La matriz completa (23 celdas) la lanza el usuario con `eda-expexecutor run e8`.
Las celdas de la spec que no están aquí y H8 sobre la matriz completa se prueban en tests/test_e8_crash_matrix.py."""

from __future__ import annotations

import json

import pytest

from eda_retail.experiments.e8_crash_matrix import run_e8

pytestmark = pytest.mark.slow

BEFORE_TX = "planner@before_tx"
OFFSET = "orchestrator@after_commit_before_offset"
RESPONSE = "executor_issue_refund@after_commit_before_response"
RELAY = "interpreter@relay_after_publish_before_mark"


@pytest.fixture(scope="module")
def report(base_settings):
    return run_e8(base_settings, seed=8, incidents=8, cells=[BEFORE_TX, OFFSET, RESPONSE, RELAY])


def _detail(report, cell: str) -> str:
    m = report.metrics["cells"][cell]
    keys = ("crash", "open", "escalation_reasons", "mechanism", "counts", "ok", "executions_per_key_max",
            "repeated_steps", "published_more_than_once", "max_publications_per_event_id", "log_rows_per_event_id_max",
            "missing_in_log", "pending_outbox", "inbox_discards", "projector_applied_events_hits",
            "replayed_responses", "max_replayed_per_command_id", "llm_extra_calls", "plan_created_per_incident_max",
            "saga_repeats", "invariants")
    baseline = report.metrics["baseline"]
    return json.dumps({"cell": {k: m.get(k) for k in keys},
                       "baseline": {k: baseline.get(k) for k in ("units", "open", "saga_repeats", "llm_client_calls")}},
                      default=str)[-8000:]


def _saved(report, cell: str) -> dict:
    """La caída se produjo, se vio su mecanismo y se mantienen las garantías (I1 a I5 incluidas)."""
    m = report.metrics["cells"][cell]
    assert m["crash"]["provoked"] is True and m["crash"]["unit"] >= 1, _detail(report, cell)
    assert m["crash"]["resumed_at"] is not None and m["crash"]["recovery_seconds"] >= 0, _detail(report, cell)
    assert m["mechanism"] is True and m["counts"] is True, _detail(report, cell)
    assert m["open"] == 0 and m["executions_per_key_max"] <= 1 and m["repeated_steps"] == 0, _detail(report, cell)
    assert m["log_rows_per_event_id_max"] == 1 and m["missing_in_log"] == 0, _detail(report, cell)
    assert m["pending_outbox_total"] == 0, _detail(report, cell)
    assert {name: inv["violations"] for name, inv in m["invariants"].items()} == dict.fromkeys(
        ("I1", "I2", "I3", "I4", "I5"), 0), _detail(report, cell)
    assert m["ok"] is True, _detail(report, cell)
    return m


def test_crash_before_opening_the_transaction(report):
    """Caída antes de abrir la transacción"""
    m = _saved(report, BEFORE_TX)
    # La llamada de más es la del trabajo K, que el worker reiniciado retoma al vencer su lease (`JOB_LEASE_S`).
    assert m["llm_extra_calls"].get("planner", 0) >= 1, _detail(report, BEFORE_TX)
    assert m["plan_created_per_incident_max"] == 1, _detail(report, BEFORE_TX)


def test_crash_between_the_commit_and_the_offset(report):
    """Caída entre el commit y el offset"""
    m = _saved(report, OFFSET)
    assert m["inbox_discards"].get("orchestrator", 0) >= 1, _detail(report, OFFSET)
    assert m["saga_repeats"] == report.metrics["baseline"]["saga_repeats"], _detail(report, OFFSET)


def test_crash_between_the_commit_and_the_response(report):
    """Caída entre el commit y la respuesta"""
    m = _saved(report, RESPONSE)
    assert m["replayed_responses"].get("executor_issue_refund", 0) >= 1, _detail(report, RESPONSE)
    assert m["invariants"]["I2"]["violations"] == 0 and m["invariants"]["I3"]["violations"] == 0, \
        _detail(report, RESPONSE)


def test_relay_crash_between_publishing_and_marking(report):
    """Caída del relay entre la publicación y la marca"""
    m = _saved(report, RELAY)
    assert m["published_more_than_once"] >= 1 and m["max_publications_per_event_id"] == 2, _detail(report, RELAY)
    assert sum(m["inbox_discards"].values()) >= 1, _detail(report, RELAY)
    assert m["invariants"]["I5"]["violations"] == 0, _detail(report, RELAY)


def test_e8_report_of_a_subset_of_cells(report):
    """Con un subconjunto de celdas, el informe da `baseline` sin caídas y cada celda con su `ok`, y H8 no se da por
    cumplida porque la matriz no está completa"""
    baseline = report.metrics["baseline"]
    assert baseline["crash"] is None and baseline["open"] == 0, json.dumps(baseline, default=str)[-4000:]
    assert baseline["published_more_than_once"] == 0, json.dumps(baseline, default=str)[-4000:]
    assert report.metrics["cells_ok"] == dict.fromkeys((BEFORE_TX, OFFSET, RESPONSE, RELAY), True)
    assert report.metrics["complete"] is False and report.hypothesis == {"H8": False}
