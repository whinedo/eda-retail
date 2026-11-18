"""E14 Ráfaga y contrapresión (spec experiments): capacidad, colas y H14.

Sin Kafka ni PostgreSQL. La ejecución de punta a punta está en tests/integration/test_e14_burst_backpressure.py.
"""

from __future__ import annotations

from eda_retail.experiments.e14_burst_backpressure import QueueSampler, h14, within_capacity


def _clean() -> dict:
    return {
        "unavailable_503": {"executor_file_claim": 4}, "over_capacity": {},
        "final_queues": {"pending_commands": 0, "open_jobs": {"planner": 0, "executor_file_claim": 0}},
        "duplicate_commands": [], "open": 0,
        "invariants": {name: {"violations": 0} for name in ("I1", "I2", "I3", "I4", "I5")},
    }


def test_executor_over_capacity_is_reported():
    """Las colas se vacían: el informe dice qué ejecutor superó su capacidad"""
    capacity = {"executor_file_claim": 3, "executor_issue_refund": 3}
    assert within_capacity({"executor_file_claim": 3, "executor_issue_refund": 4, "planner": 40}, capacity) == {
        "executor_issue_refund": 4}
    assert within_capacity({"executor_file_claim": 3}, capacity) == {}


def test_sampler_keeps_maxima_and_last_busy_instant():
    """Las colas se vacían: el muestreo guarda los máximos y el último instante con alguna cola no vacía"""
    sampler = QueueSampler(h=None, start=0.0)
    sampler.observe({"open_jobs": {"planner": 5, "executor_file_claim": 2}, "pending_commands": 7}, 1.0)
    sampler.observe({"open_jobs": {"planner": 1, "executor_file_claim": 3}, "pending_commands": 0}, 2.0)
    sampler.observe({"open_jobs": {"planner": 0, "executor_file_claim": 0}, "pending_commands": 0}, 3.0)
    assert sampler.max_open_jobs["planner"] == 5
    assert sampler.max_open_jobs["executor_file_claim"] == 3
    assert sampler.max_pending_commands == 7
    assert sampler.last_busy_s == 2.0


def test_h14_holds_with_a_clean_backpressure_run():
    """Cumplimiento de H14"""
    assert h14(_clean())


def test_h14_needs_a_503():
    """Cumplimiento de H14: sin ningún 503 no cuenta como cumplida"""
    assert not h14({**_clean(), "unavailable_503": {}})


def test_h14_fails_on_any_broken_condition():
    """Cumplimiento de H14: capacidad superada, colas sin vaciar, comandos duplicados, incidencias abiertas o
    invariantes violadas"""
    clean = _clean()
    broken = [
        {"over_capacity": {"executor_file_claim": 4}},
        {"final_queues": {"pending_commands": 1, "open_jobs": {}}},
        {"final_queues": {"pending_commands": 0, "open_jobs": {"planner": 2}}},
        {"duplicate_commands": [{"incident_id": "I-1", "step_id": "S1", "plan_version": 1, "commands": 2}]},
        {"open": 1},
        {"invariants": {**clean["invariants"], "I2": {"violations": 1}}},
    ]
    for change in broken:
        assert not h14({**clean, **change}), change


def test_expexecutor_checks_h14():
    """Cumplimiento de H14 en el catálogo de `eda-expexecutor`: comprueba H14, el 503, la capacidad, las colas, los
    duplicados e I1 e I2 por variante"""
    from eda_retail.expexecutor.catalog import CATALOG

    variant = {**_clean(), "final_queues": {"pending_commands": 0, "open_jobs": {}}}
    ok = {"hypothesis": {"H14": True}, "metrics": {"variants": {"baseline": variant, "backpressure": variant}}}
    assert all(c.ok for c in CATALOG["e14"].checks(ok))
    bad = {"hypothesis": {"H14": False},
           "metrics": {"variants": {"baseline": variant, "backpressure": {**variant, "unavailable_503": {}}}}}
    failed = [c.metric for c in CATALOG["e14"].checks(bad) if not c.ok]
    assert failed == ["hypothesis.H14", "metrics.variants.backpressure.unavailable_503"]


def test_cli_runs_e14():
    """Ejecución de un experimento: E14 está en la CLI con 200 incidencias por defecto"""
    from eda_retail.experiments.cli import DEFAULT_INCIDENTS, EXPERIMENTS

    assert EXPERIMENTS["e14"].__name__ == "run_e14"
    assert DEFAULT_INCIDENTS["e14"] == 200
