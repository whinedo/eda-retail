"""E19 Pérdida del topic (spec experiments) y Pérdida del topic (spec synthetic-data): orden de los outbox, H19,
catálogo y CLI.

Sin Kafka ni PostgreSQL. La ejecución de punta a punta está en tests/integration/test_e19_topic_loss.py.
"""

from __future__ import annotations

import pytest

from eda_retail.experiments import harness
from eda_retail.experiments.e19_topic_loss import check_order, h19_variant, reordered


def _row(partition, offset) -> dict:
    return {"kafka_partition": partition, "kafka_offset": offset}


def test_republished_outbox_keeps_its_order():
    """Topic vaciado: cada outbox en su orden dentro de su partición, con offsets posteriores a la pérdida"""
    rows = [_row(0, 10), _row(1, 7), _row(0, 12), _row(1, 9)]
    assert check_order(rows, {0: 10, 1: 7}) == {"not_republished": 0, "unordered": 0}


def test_rows_not_republished_or_out_of_order_are_counted():
    """Topic vaciado: una fila con un offset de antes de la pérdida no se republicó; un offset que no crece está
    desordenado"""
    rows = [_row(0, 3), _row(0, 11), _row(0, 10), _row(None, None)]
    assert check_order(rows, {0: 10}) == {"not_republished": 2, "unordered": 1}


def test_recreated_topic_starts_at_zero():
    """Topic recreado: con el topic nuevo, la primera fila republicada de cada partición puede tener el offset 0"""
    assert check_order([_row(0, 0), _row(2, 0), _row(0, 1)], {0: 0, 2: 0}) == {"not_republished": 0, "unordered": 0}


def test_unknown_topic_loss_mode_is_rejected():
    """Pérdida del topic: solo `truncated` y `recreated`"""
    with pytest.raises(ValueError):
        harness.Harness.lose_topic(object(), "deleted")
    assert harness.TOPIC_LOSS_MODES == ("truncated", "recreated")


def test_partition_keeps_its_order_across_components():
    """Topic recreado: cada partición conserva el orden que tenía en el topic perdido, también entre componentes"""
    before = {("planner", 1): (0, 4), ("orchestrator", 7): (0, 5), ("executor_file_claim", 2): (0, 6),
              ("intake", 3): (1, 0), ("never_published", 9): (None, None)}
    kept = {("planner", 1): (0, 0), ("orchestrator", 7): (0, 1), ("executor_file_claim", 2): (0, 2),
            ("intake", 3): (1, 0), ("never_published", 9): (2, 0)}
    assert reordered(before, kept) == 0
    # Los resultados del ejecutor llegan antes que el paso que los pidió (republicación outbox a outbox).
    swapped = {**kept, ("executor_file_claim", 2): (0, 0), ("planner", 1): (0, 1), ("orchestrator", 7): (0, 2)}
    assert reordered(before, swapped) == 1
    assert reordered(before, {**kept, ("intake", 3): (2, 0)}) == 1  # cambió de partición


def _clean(**changes) -> dict:
    m = {"lag": 0, "reordered": 0, "messages_since_loss": 30, "republished": {"intake": 10, "orchestrator": 20},
         "order": {"intake": {"not_republished": 0, "unordered": 0}}, "changes": {}, "open": 0,
         "invariants": {name: {"violations": 0} for name in ("I1", "I2", "I3", "I4", "I5")}}
    return {**m, **changes}


def test_h19_holds_with_a_clean_recovery():
    """Cumplimiento de H19"""
    assert h19_variant(_clean())


def test_h19_fails_on_any_broken_condition():
    """Cumplimiento de H19: lag, mensajes distintos de las filas, filas sin republicar o desordenadas, cambios,
    incidencias abiertas (también las de los avisos nuevos) o invariantes violadas"""
    broken = [
        {"lag": 3},
        {"messages_since_loss": 29},
        {"order": {"intake": {"not_republished": 1, "unordered": 0}}},
        {"order": {"intake": {"not_republished": 0, "unordered": 1}}},
        {"reordered": 2},
        {"changes": {"executions": {"carrier:K": [1, 2]}}},
        {"open": 1},
        {"invariants": {**_clean()["invariants"], "I5": {"violations": 1}}},
    ]
    for change in broken:
        assert not h19_variant(_clean(**change)), change


def test_expexecutor_checks_h19():
    """Cumplimiento de H19 en el catálogo de `eda-expexecutor`"""
    from eda_retail.expexecutor.catalog import CATALOG

    variant = {**_clean(), "h19": True}
    ok = {"hypothesis": {"H19": True}, "metrics": {"variants": {"truncated": variant, "recreated": variant}}}
    assert all(c.ok for c in CATALOG["e19"].checks(ok))
    bad_variant = {**variant, "h19": False, "changes": {"rows": {"views.x": [1, 2]}}}
    bad = {"hypothesis": {"H19": False}, "metrics": {"variants": {"truncated": variant, "recreated": bad_variant}}}
    assert [c.metric for c in CATALOG["e19"].checks(bad) if not c.ok] == [
        "hypothesis.H19", "metrics.variants.recreated.h19", "metrics.variants.recreated.changes"]


def test_cli_runs_e19():
    """Ejecución de un experimento: E19 está en la CLI con 200 incidencias por defecto"""
    from eda_retail.experiments.cli import DEFAULT_INCIDENTS, EXPERIMENTS

    assert EXPERIMENTS["e19"].__name__ == "run_e19"
    assert DEFAULT_INCIDENTS["e19"] == 200
