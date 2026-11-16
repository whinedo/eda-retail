"""E21 Reproducción total del log (spec experiments): comparación de fotos e hipótesis H21, sin Kafka.

La reproducción de punta a punta está en tests/integration/test_e21_log_replay.py.
"""

from __future__ import annotations

from eda_retail.experiments.e21_replay import Snapshot, changes, h21, summarize

BEFORE = Snapshot(
    executions={"payments:k1": 1, "mail:k2": 1},
    rows={"orchestrator.command_outbox": 10, "orchestrator.inbox": 40, "inventory.reservations": 3},
    fingerprints={"orchestrator.plan_steps": "a", "view_incident_status": "b"},
)


def metrics(**over):
    base = {
        "open": 0,
        "replay": {"changed": False, "lag": 0, "inbox_matches": True},
        "old_commands": {"redelivered": 12, "not_replayed": 0},
        "control": {"changed": True, "new_executions": 4},
        "observations": {"orchestrator": {"changed": False}, "interpreter": {"changed": False}},
    }
    for key, value in over.items():
        base[key] = base[key] | value if isinstance(value, dict) else value
    return base


def test_replay_without_changes():
    """Reproducción con inbox activo: misma foto antes y después, ninguna diferencia"""
    assert changes(BEFORE, BEFORE) == {}


def test_new_effects_rows_and_fingerprints_are_reported():
    """Reproducción con inbox activo: un efecto, una fila o una huella nuevos aparecen en la comparación"""
    after = Snapshot(
        executions={"payments:k1": 2, "mail:k2": 1, "claims:k3": 1},
        rows={**BEFORE.rows, "orchestrator.command_outbox": 11},
        fingerprints={**BEFORE.fingerprints, "view_incident_status": "c"},
    )
    diff = changes(BEFORE, after)
    assert diff == {
        "executions": {"claims:k3": [None, 1], "payments:k1": [1, 2]},
        "rows": {"orchestrator.command_outbox": [10, 11]},
        "fingerprints": ["view_incident_status"],
    }


def test_control_summary_counts_changed_keys():
    """Control sin idempotencia en los efectos: el informe resume cuántos efectos, filas y huellas cambiaron"""
    diff = {"executions": {"payments:k1": [1, 3], "claims:k3": [None, 1]}, "rows": {"x.outbox": [1, 4]},
            "fingerprints": ["view_orders"]}
    assert summarize(diff) == {"changed": True, "new_executions": 3, "rows": {"x.outbox": [1, 4]},
                               "fingerprints": ["view_orders"]}
    assert summarize({}) == {"changed": False, "new_executions": 0, "rows": {}, "fingerprints": []}


def test_h21_holds():
    """Cumplimiento de H21: nada cambia con inbox y el control produce efectos repetidos"""
    assert h21(metrics())


def test_observations_without_inbox_do_not_decide_h21():
    """Reproducción sin el inbox de un consumidor: lo que cambie es una observación, fuera de H21"""
    changed = {"orchestrator": {"changed": True}, "interpreter": {"changed": True}}
    assert h21(metrics(observations=changed))


def test_h21_fails_when_the_replay_changes_something():
    """Cumplimiento de H21: un efecto, una fila o una huella distintos la incumplen"""
    assert not h21(metrics(replay={"changed": True}))


def test_h21_fails_when_the_control_detects_nothing():
    """Control sin idempotencia en los efectos: sin efectos repetidos, el experimento no detecta nada"""
    assert not h21(metrics(control={"changed": False, "new_executions": 0}))
    assert not h21(metrics(control={"changed": True, "new_executions": 0}))


def test_h21_fails_when_the_inbox_did_not_discard_everything():
    """Estado intacto: el inbox descarta tantos mensajes como tenía registrados cada consumidor"""
    assert not h21(metrics(replay={"inbox_matches": False}))


def test_h21_fails_when_an_old_command_is_executed_again():
    """Ejecutores activos ante comandos antiguos: todos devuelven la respuesta guardada"""
    assert not h21(metrics(old_commands={"not_replayed": 1}))


def test_h21_fails_when_incidents_stay_open_or_lag_remains():
    """Cumplimiento de H21: exige las incidencias cerradas y el lag a 0"""
    assert not h21(metrics(open=1))
    assert not h21(metrics(replay={"lag": 3}))
