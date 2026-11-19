"""E8 Matriz de puntos de caída (spec experiments, «E8 Matriz de puntos de caída (H8)»): celdas, unidad K, recuentos,
mecanismo visible por punto, H8, CLI y catálogo de expexecutor.

Sin Docker, PostgreSQL ni Kafka. La ejecución de punta a punta está en tests/integration/test_e8_crash_matrix.py.
"""

from __future__ import annotations

import uuid
from collections import Counter
from types import SimpleNamespace

import pytest

from eda_retail.config import Settings
from eda_retail.experiments import cli
from eda_retail.experiments import e8_crash_matrix as e8
from eda_retail.experiments.harness import CommandDelivery
from eda_retail.expexecutor.catalog import CATALOG
from eda_retail.synthetic.faults import (
    AFTER_COMMIT_BEFORE_OFFSET,
    AFTER_COMMIT_BEFORE_RESPONSE,
    BEFORE_TX,
    RELAY_AFTER_PUBLISH_BEFORE_MARK,
    CrashRecord,
    FaultPlan,
)

EXECUTORS = ("executor_notify_customer", "executor_file_claim", "executor_reissue_shipment", "executor_issue_refund")
OK = (202, {"status": "accepted"})
CLEAN = {name: {"violations": 0} for name in ("I1", "I2", "I3", "I4", "I5")}


def _result(**kw):
    base = {"closed": 4, "open": 0, "resolved": 3, "escalated": 1, "escalation_reasons": {"no_valid_orders": 1},
            "latencies_ms": [100.0, 200.0, 300.0], "repeated_steps": 0}
    return SimpleNamespace(**(base | kw))


def _summary(cell=("planner", BEFORE_TX), **kw):
    args = {"result": _result(), "records": [], "baseline_units": 4, "executions": {"mail": {"k1": 1}},
            "publications": [("e1", "IncidentOpened")], "projectors_attending": lambda _t: 1, "log_rows_max": 1,
            "missing_in_log": 0, "pending": {"intake": 0}, "outcomes": Counter(), "deliveries": [],
            "llm_calls": {"planner": 4}, "baseline_llm": {"planner": 4}, "invariants": CLEAN,
            "plan_created_max": 1, "saga_repeats": {"commands": 0, "events": 0}}
    return e8.summarize(cell, **(args | kw))


def _record(component="planner", point=BEFORE_TX, unit=2, at=100.0, resumed=103.5):
    return CrashRecord(component, point, unit, at, resumed)


# -- celdas y unidad K -----------------------------------------------------------------------------------
def test_the_matrix_has_the_23_cells_of_the_spec():
    """Las 23 celdas de la spec: 3 before_tx, 2 after_commit_before_offset, 8 after_commit_before_response y 10
    relay_after_publish_before_mark, sin event_log ni proyectores; cada una es un crash_at válido"""
    cells = e8.cells()
    assert len(cells) == 23 and len(set(cells)) == 23
    by_point = Counter(point for _, point in cells)
    assert by_point == {BEFORE_TX: 3, AFTER_COMMIT_BEFORE_OFFSET: 2, AFTER_COMMIT_BEFORE_RESPONSE: 8,
                        RELAY_AFTER_PUBLISH_BEFORE_MARK: 10}
    assert {c for c, p in cells if p == BEFORE_TX} == {"interpreter", "planner", "replanner"}
    assert {c for c, p in cells if p == AFTER_COMMIT_BEFORE_OFFSET} == {"interpreter", "orchestrator"}
    assert {c for c, p in cells if p == AFTER_COMMIT_BEFORE_RESPONSE} == {
        "intake", "inventory", "planner", "replanner", *EXECUTORS}
    assert {c for c, p in cells if p == RELAY_AFTER_PUBLISH_BEFORE_MARK} == {
        "intake", "interpreter", "planner", "replanner", "orchestrator", "inventory", *EXECUTORS}
    assert not any(c in ("event_log", "views") or c.startswith("projector") for c, _ in cells)
    for cell in cells:
        FaultPlan(crash_at={cell: 1})


def test_k_is_the_middle_unit_of_the_baseline():
    """K = ⌈n/2⌉ de las n unidades que el componente atendió en ese punto en baseline; con n = 0 no hay caída"""
    assert [e8.crash_unit(n) for n in (1, 2, 3, 4, 20, 21)] == [1, 1, 2, 2, 10, 11]
    assert e8.crash_unit(0) is None


def test_cells_by_name():
    """Las celdas se nombran `<componente>@<punto>`; un nombre fuera de la matriz se rechaza en español"""
    assert e8.cell_name(("orchestrator", AFTER_COMMIT_BEFORE_OFFSET)) == "orchestrator@after_commit_before_offset"
    assert e8.parse_cells(["planner@before_tx", "intake@after_commit_before_response"]) == [
        ("planner", BEFORE_TX), ("intake", AFTER_COMMIT_BEFORE_RESPONSE)]
    with pytest.raises(ValueError, match="celda desconocida: orchestrator@before_tx"):
        e8.parse_cells(["orchestrator@before_tx"])


# -- recuentos ---------------------------------------------------------------------------------------------
def test_command_target_of_each_delivery():
    """Cada comando se atribuye al sistema que lo atiende (para las respuestas repetidas por el inbox)"""
    assert e8.command_schema("RegisterCarrierNotice", None) == "intake"
    assert e8.command_schema("CreatePlan", None) == "planner"
    assert e8.command_schema("RevisePlan", None) == "replanner"
    assert e8.command_schema("ExecuteStep", "reserve_stock") == "inventory"
    assert e8.command_schema("ExecuteStep", "release_stock") == "inventory"
    assert e8.command_schema("ExecuteStep", "issue_refund") == "executor_issue_refund"
    assert e8.command_schema("SeedSampleData", None) == "inventory"


def test_replayed_responses_per_target_and_command_id():
    """Respuestas que el inbox repitió, por sistema, y el máximo para un mismo command_id"""
    refund = uuid.uuid4()
    deliveries = [
        CommandDelivery("ExecuteStep", refund, "issue_refund", "k1", [OK]),
        CommandDelivery("ExecuteStep", refund, "issue_refund", "k1", [OK], replayed=1),
        CommandDelivery("CreatePlan", uuid.uuid4(), None, None, [OK]),
    ]
    assert e8.replayed_responses(deliveries) == ({"executor_issue_refund": 1}, 1)
    assert e8.replayed_responses([]) == ({}, 0)


def test_command_delivery_keeps_its_previous_shape():
    """CommandDelivery se sigue creando con sus cinco campos de antes (E22); `replayed` vale 0 por defecto"""
    d = CommandDelivery("ExecuteStep", uuid.uuid4(), "notify_customer", "k", [OK])
    assert d.replayed == 0 and d.responses == [OK]


def test_inbox_discards_per_consumer():
    """Descartes del inbox de cada consumidor de Kafka (resultado `duplicate`)"""
    outcomes = Counter({("orchestrator", "duplicate"): 2, ("orchestrator", "processed"): 9,
                        ("interpreter", "ignored"): 3})
    assert e8.inbox_discards(outcomes) == {"orchestrator": 2}


def test_publications_per_event_id():
    """event_id publicados más de una vez en eda.events, el máximo por event_id y los eventos que un proyector ya
    tenía en views.applied_events (una reentrega por cada proyector que atiende su tipo)"""
    publications = [("a", "IncidentOpened"), ("a", "IncidentOpened"), ("b", "PlanCreated"), ("c", "StepFailed"),
                    ("c", "StepFailed"), ("c", "StepFailed")]
    attending = {"IncidentOpened": 2, "PlanCreated": 3, "StepFailed": 0}
    assert e8.publication_counts(publications, lambda t: attending[t]) == {
        "messages": 6, "event_ids": 3, "published_more_than_once": 2, "max_publications_per_event_id": 3,
        "projector_applied_events_hits": 2}


def test_llm_calls_in_excess_of_the_baseline():
    """Llamadas al LLM de más frente a baseline, por agente, contadas en el cliente"""
    assert e8.llm_extra({"planner": 21, "writer": 20, "interpreter": 20},
                        {"planner": 20, "writer": 20, "interpreter": 20}) == {"planner": 1}
    assert e8.llm_extra({"planner": 19}, {"planner": 20}) == {}


def test_crash_and_recovery_of_the_cell():
    """La caída de la celda: unidad, hora y tiempo de recuperación (desde la caída hasta que el componente reiniciado
    atiende su unidad siguiente); sin caída, `provoked` es False"""
    crash = e8.crash_summary([_record(), _record("intake", RELAY_AFTER_PUBLISH_BEFORE_MARK)], ("planner", BEFORE_TX),
                             baseline_units=4)
    assert crash["provoked"] is True and crash["unit"] == 2 and crash["baseline_units"] == 4
    assert crash["recovery_seconds"] == 3.5 and crash["at"].startswith("1970-01-01T00:01:40")
    none = e8.crash_summary([], ("planner", BEFORE_TX), baseline_units=4)
    assert none["provoked"] is False and none["unit"] is None and none["recovery_seconds"] is None


def test_metrics_of_a_cell():
    """El informe de cada celda da las métricas de la spec"""
    m = _summary()
    assert set(e8.CELL_METRICS) <= set(m)
    assert (m["closed"], m["open"], m["resolved"], m["escalated"]) == (4, 0, 3, 1)
    assert m["escalation_reasons"] == {"no_valid_orders": 1}
    assert m["executions_per_key_max"] == 1 and m["log_rows_per_event_id_max"] == 1
    assert m["pending_outbox_total"] == 0 and m["latency_ms"]["median"] == 200.0
    assert m["invariants"] == CLEAN


# -- mecanismo visible y H8 --------------------------------------------------------------------------------
def test_before_tx_counts_with_the_unit_attended_again_and_an_extra_llm_call():
    """before_tx: la unidad se vuelve a atender y el cliente del LLM recibe al menos una llamada más que en baseline"""
    cell = ("planner", BEFORE_TX)
    m = _summary(cell, records=[_record()], llm_calls={"planner": 5})
    assert m["mechanism"] is True and m["counts"] is True and m["ok"] is True
    assert _summary(cell, records=[_record()])["mechanism"] is False  # sin llamada de más
    assert _summary(cell, records=[_record(resumed=None)], llm_calls={"planner": 5})["mechanism"] is False
    assert _summary(cell, llm_calls={"planner": 5})["counts"] is False  # sin caída


def test_after_commit_before_offset_counts_with_an_inbox_discard_of_that_consumer():
    """after_commit_before_offset: al menos un descarte del inbox del consumidor que cayó"""
    cell = ("orchestrator", AFTER_COMMIT_BEFORE_OFFSET)
    record = [_record("orchestrator", AFTER_COMMIT_BEFORE_OFFSET)]
    assert _summary(cell, records=record, outcomes=Counter({("orchestrator", "duplicate"): 1}))["counts"] is True
    assert _summary(cell, records=record, outcomes=Counter({("interpreter", "duplicate"): 1}))["counts"] is False


def test_after_commit_before_response_counts_with_a_replayed_response_of_that_system():
    """after_commit_before_response: al menos una respuesta repetida por el inbox con el mismo command_id"""
    cell = ("executor_issue_refund", AFTER_COMMIT_BEFORE_RESPONSE)
    record = [_record("executor_issue_refund", AFTER_COMMIT_BEFORE_RESPONSE)]
    replayed = [CommandDelivery("ExecuteStep", uuid.uuid4(), "issue_refund", "k", [OK], replayed=1)]
    other = [CommandDelivery("CreatePlan", uuid.uuid4(), None, None, [OK], replayed=1)]
    assert _summary(cell, records=record, deliveries=replayed)["counts"] is True
    assert _summary(cell, records=record, deliveries=other)["counts"] is False


def test_relay_counts_with_an_event_id_published_twice():
    """relay_after_publish_before_mark: al menos un event_id publicado dos veces en eda.events"""
    cell = ("interpreter", RELAY_AFTER_PUBLISH_BEFORE_MARK)
    record = [_record("interpreter", RELAY_AFTER_PUBLISH_BEFORE_MARK)]
    twice = [("e1", "IncidentOpened"), ("e1", "IncidentOpened")]
    assert _summary(cell, records=record, publications=twice)["counts"] is True
    assert _summary(cell, records=record)["counts"] is False


def test_a_cell_that_counts_must_also_keep_the_guarantees():
    """Una celda que cuenta no está bien si deja incidencias abiertas, efectos repetidos, eventos dos veces en
    event_log, outbox pendiente o violaciones de I1 a I5"""
    cell, record, llm = ("planner", BEFORE_TX), [_record()], {"planner": 5}
    good = {"cell": cell, "records": record, "llm_calls": llm}
    assert _summary(**good)["ok"] is True
    assert _summary(**good, result=_result(open=1))["ok"] is False
    assert _summary(**good, executions={"mail": {"k1": 2}})["ok"] is False
    assert _summary(**good, result=_result(repeated_steps=1))["ok"] is False
    assert _summary(**good, log_rows_max=2)["ok"] is False
    assert _summary(**good, pending={"intake": 1})["ok"] is False
    assert _summary(**good, invariants=CLEAN | {"I5": {"violations": 1}})["ok"] is False


def _ok_cells() -> dict:
    return {e8.cell_name(c): {"ok": True, "counts": True} for c in e8.cells()}


def test_h8_is_met():
    """Cumplimiento de H8"""
    cells = _ok_cells()
    assert e8.h8(cells) is True
    not_provoked = cells | {"planner@before_tx": {"ok": False, "counts": False, "crash": {"provoked": False}}}
    assert e8.h8(not_provoked) is False
    assert e8.h8({k: v for k, v in cells.items() if k != "planner@before_tx"}) is False  # matriz incompleta


def test_run_e8_picks_k_from_the_baseline_and_reports_a_cell_without_units_as_not_provoked(monkeypatch):
    """run_e8 ejecuta baseline sin caídas, elige K = ⌈n/2⌉ con sus unidades y la misma semilla, y una celda con n = 0
    no se ejecuta: consta como no provocada y H8 no se cumple"""
    calls = []

    def fake_run(base, *, seed, incidents, crash_at, baseline=None):
        calls.append((seed, incidents, dict(crash_at)))
        if baseline is None:
            units = {("planner", BEFORE_TX): 5, ("interpreter", RELAY_AFTER_PUBLISH_BEFORE_MARK): 40}
            return {"metrics": {"closed": incidents}, "units": units, "llm_calls": {"planner": 5}}
        return {"metrics": {"ok": True, "counts": True}, "units": {}, "llm_calls": {}}

    monkeypatch.setattr(e8, "run_once", fake_run)
    cells = [("planner", BEFORE_TX), ("interpreter", RELAY_AFTER_PUBLISH_BEFORE_MARK),
             ("replanner", AFTER_COMMIT_BEFORE_RESPONSE)]
    report = e8.run_e8(Settings(), seed=3, incidents=6, cells=cells)
    assert calls == [(3, 6, {}), (3, 6, {("planner", BEFORE_TX): 3}),
                     (3, 6, {("interpreter", RELAY_AFTER_PUBLISH_BEFORE_MARK): 20})]
    skipped = report.metrics["cells"]["replanner@after_commit_before_response"]
    assert skipped["crash"] == {"provoked": False, "unit": None, "at": None, "resumed_at": None,
                                "recovery_seconds": None, "baseline_units": 0}
    assert skipped["counts"] is False and skipped["ok"] is False
    assert report.metrics["complete"] is False and report.hypothesis == {"H8": False}
    assert report.config["cells"] == [e8.cell_name(c) for c in cells]
    assert report.config["fail_probability"] == {"reserve_stock": 0.3}


# -- CLI y catálogo ----------------------------------------------------------------------------------------
def test_e8_is_in_the_cli_with_20_incidents():
    """`eda-experiment e8` ejecuta E8 con 20 incidencias por celda por defecto"""
    assert cli.EXPERIMENTS["e8"] is e8.run_e8 and cli.DEFAULT_INCIDENTS["e8"] == 20
    assert cli.experiment_kwargs(cli.parse_args(["e8"])) == {"seed": 42, "incidents": 20}


def test_e8_cells_option():
    """`--cells` ejecuta solo esas celdas de E8 (y baseline); se rechaza en los demás experimentos y con una celda
    fuera de la matriz"""
    kw = cli.experiment_kwargs(cli.parse_args(["e8", "--cells", "planner@before_tx,intake@relay_after_publish_before_mark"]))
    assert kw == {"seed": 42, "incidents": 20, "cells": ["planner@before_tx", "intake@relay_after_publish_before_mark"]}
    with pytest.raises(SystemExit):
        cli.parse_args(["e2", "--cells", "planner@before_tx"])
    with pytest.raises(SystemExit):
        cli.parse_args(["e8", "--cells", "intake@before_tx"])
    with pytest.raises(ValueError, match="celda desconocida: intake@before_tx"):
        e8.run_e8(Settings(), cells=["intake@before_tx"])


def test_the_other_experiments_keep_their_cli_entries():
    """Añadir e8 no cambia las entradas ni los tamaños de los demás experimentos"""
    others = {k: v for k, v in cli.DEFAULT_INCIDENTS.items() if k != "e8"}
    assert others == {"e1": 50, "e2": 50, "e3": 30, "e4": 200, "e5": 40, "e6": 50, "e7": 50, "e10": 50, "e11": 50,
                      "e12": 50, "e13": 50, "e14": 200, "e15": 50, "e16": 50, "e17": 50, "e18": 100, "e19": 200,
                      "e20": 50, "e21": 50, "e22": 50, "e23": 50}
    assert set(cli.EXPERIMENTS) == {*others, "e8"} and "e9" not in cli.EXPERIMENTS
    assert cli.experiment_kwargs(cli.parse_args(["e22"])) == {"seed": 42, "incidents": 50}
    assert set(CATALOG) == {*others, "e8"}


def test_expexecutor_checks_h8_per_cell():
    """expexecutor: E8 está en el catálogo y comprueba H8 y, por celda, la caída, el mecanismo, las incidencias
    abiertas, las ejecuciones por clave, el outbox pendiente e I1 e I2"""
    cell = {"ok": True, "counts": True, "crash": {"provoked": True}, "mechanism": True, "open": 0,
            "executions_per_key_max": 1, "log_rows_per_event_id_max": 1, "pending_outbox_total": 0,
            "invariants": {"I1": {"violations": 0}, "I2": {"violations": 0}}}
    report = {"hypothesis": {"H8": True},
              "metrics": {"cells": {e8.cell_name(c): cell for c in e8.cells()}}}
    checks = CATALOG["e8"].checks(report)
    assert checks and all(c.ok for c in checks)
    assert any(c.metric == "metrics.cells.planner@before_tx.crash.provoked" for c in checks)
    bad = {**report, "metrics": {"cells": {**report["metrics"]["cells"],
                                           "planner@before_tx": cell | {"crash": {"provoked": False}}}}}
    assert not all(c.ok for c in CATALOG["e8"].checks(bad))
