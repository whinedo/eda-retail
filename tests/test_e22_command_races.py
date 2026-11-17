"""E22: variantes, recuentos y H22 (spec experiments, «E22 Carreras de comandos y ventana efecto–registro (H22)»)."""

from __future__ import annotations

import uuid

from eda_retail.experiments.e22_command_races import (
    CRASH_ACTIONS,
    OTHER_COMMANDS,
    SAME_KEY_ACTIONS,
    exercised,
    h22,
    summarize,
    variants,
)
from eda_retail.experiments.harness import CommandDelivery
from eda_retail.expexecutor.catalog import CATALOG
from eda_retail.synthetic.faults import FaultPlan

OK = (202, {"status": "accepted"})


def _step(action: str, key: str, *responses, command_id=None) -> CommandDelivery:
    return CommandDelivery("ExecuteStep", command_id or uuid.uuid4(), action, key, list(responses or [OK]))


def _event(event_type: str, causation, incident: str = "I-1") -> dict:
    return {"event_type": event_type, "correlation_id": incident, "causation_id": causation, "payload": {}}


def _summary(deliveries, executions=None, events=(), jobs=(), busy=0, crashes=None):
    return summarize(deliveries, executions or {}, events, jobs, busy_claims=busy, crashes=crashes or {})


def test_variants_cover_the_four_cases_of_the_spec():
    """El escenario: (a) ExecuteStep desde 2, 4 y 8 hilos; (b) CreatePlan, RevisePlan, RegisterCarrierNotice y
    SeedSampleData concurrentes; (c) dos command_id con la misma clave para cuatro acciones, con dos workers; (d) la
    caída entre efecto y registro en notify_customer e issue_refund. Cada configuración es un FaultPlan válido"""
    v = variants()
    assert [v[f"execute_step_{n}"]["faults"]["concurrent_commands"] for n in (2, 4, 8)] == [
        {"ExecuteStep": 2}, {"ExecuteStep": 4}, {"ExecuteStep": 8}]
    assert set(v["other_commands"]["faults"]["concurrent_commands"]) == set(OTHER_COMMANDS)
    assert v["same_key"]["faults"]["reinvoke_steps"] == SAME_KEY_ACTIONS and v["same_key"]["job_workers"] == 2
    assert v["effect_record_crash"]["faults"]["crash_between_effect_and_record"] == CRASH_ACTIONS
    for setup in v.values():
        FaultPlan(**setup["faults"])


def test_two_executors_at_once():
    """Dos ejecutores a la vez: 8 entregas del mismo ExecuteStep con el mismo command_id; una ejecución del servicio,
    la misma respuesta para todas y un solo StepCompleted"""
    c = uuid.uuid4()
    m = _summary([_step("notify_customer", "K", *[OK] * 8, command_id=c)], {"notify_customer": {"K": 1}},
                 [_event("CustomerNotified", c), _event("StepCompleted", c)])
    assert m["executions_per_key"]["notify_customer"] == 1
    assert m["distinct_responses_per_command_id"] == 1
    assert m["concurrent_deliveries"]["ExecuteStep"] == 1
    assert m["events"]["step_completed_per_key"] == 1
    assert m["events"]["effect_events_per_key"]["notify_customer"] == 1


def test_concurrent_command_with_deferred_work():
    """Comando concurrente con trabajo diferido: dos entregas del mismo CreatePlan; un trabajo por command_id y un solo
    PlanCreated por incidencia"""
    c = uuid.uuid4()
    m = _summary([CommandDelivery("CreatePlan", c, None, None, [OK, OK])],
                 events=[_event("PlanCreated", uuid.uuid4())], jobs=[{"command_id": c}])
    assert m["concurrent_deliveries"]["CreatePlan"] == 1
    assert (m["events"]["jobs_per_command_id"], m["events"]["plan_created_per_incident"]) == (1, 1)


def test_same_key_other_command_id():
    """Misma clave, otro command_id: dos ExecuteStep con la clave K; el segundo devuelve el resultado guardado sin
    repetir el efecto (un StockReserved por clave aunque haya dos StepCompleted)"""
    c1, c2 = uuid.uuid4(), uuid.uuid4()
    deliveries = [_step("reserve_stock", "K", command_id=c1), _step("reserve_stock", "K", command_id=c2)]
    events = [_event("StockReserved", c1), _event("StepCompleted", c1), _event("StepCompleted", c2)]
    m = _summary(deliveries, events=events)
    assert m["reinvoked_keys"]["reserve_stock"] == 1
    assert m["executions_per_key"]["reserve_stock"] == 1
    assert m["events"]["step_completed_per_key"] == 2


def test_crash_after_the_effect_and_before_recording_the_key():
    """Caída tras el efecto y antes de registrar la clave: el reintento usa la misma clave y el cliente recibe un solo
    aviso (una ejecución del servicio de correo)"""
    m = _summary([_step("notify_customer", "K")], {"notify_customer": {"K": 1}}, crashes={"K": "notify_customer"})
    assert m["effect_record_crashes"] == {"notify_customer": 1, "issue_refund": 0}
    assert m["executions_per_key"]["notify_customer"] == 1


def test_repeated_executions_and_different_responses_are_reported():
    c = uuid.uuid4()
    m = _summary([_step("issue_refund", "K", OK, (422, {"status": "rejected"}), command_id=c)],
                 {"issue_refund": {"K": 2}}, busy=3)
    assert (m["executions_per_key"]["issue_refund"], m["distinct_responses_per_command_id"]) == (2, 2)
    assert m["busy_claims"] == 3


def _metrics(**overrides) -> dict:
    m = {"executions_per_key": dict.fromkeys(("notify_customer", "file_claim", "reserve_stock", "issue_refund"), 1),
         "distinct_responses_per_command_id": 1,
         "concurrent_deliveries": {t: 1 for t in ("ExecuteStep", *OTHER_COMMANDS)},
         "reinvoked_keys": dict.fromkeys(SAME_KEY_ACTIONS, 1),
         "effect_record_crashes": dict.fromkeys(CRASH_ACTIONS, 1),
         "invariants": {f"I{i}": {"violations": 0} for i in range(1, 6)}}
    return m | overrides


def test_h22():
    """Cumplimiento de H22: ninguna variante tiene ejecuciones del servicio por encima de una por clave y todas las
    entregas de un mismo command_id reciben la misma respuesta"""
    for variant in variants():
        assert h22(variant, _metrics())
    assert not h22("same_key", _metrics(executions_per_key={"issue_refund": 2}))
    assert not h22("execute_step_8", _metrics(distinct_responses_per_command_id=2))


def test_a_variant_that_does_not_provoke_its_race_does_not_count():
    assert not exercised("same_key", _metrics(reinvoked_keys=dict.fromkeys(SAME_KEY_ACTIONS, 1) | {"issue_refund": 0}))
    assert not exercised("effect_record_crash", _metrics(effect_record_crashes={"notify_customer": 1,
                                                                                "issue_refund": 0}))
    assert not exercised("other_commands", _metrics(concurrent_deliveries={t: 0 for t in ("ExecuteStep",
                                                                                         *OTHER_COMMANDS)}))
    assert not h22("execute_step_2", _metrics(concurrent_deliveries={"ExecuteStep": 0}))


def test_expexecutor_checks_h22_per_variant():
    """expexecutor: E22 está en el catálogo y comprueba H22, ejecuciones por clave y respuestas por variante"""
    variant = _metrics() | {"h22": True, "executions_per_key": dict.fromkeys(
        ("notify_customer", "file_claim", "reserve_stock", "reissue_shipment", "issue_refund"), 1)}
    report = {"metrics": {"variants": {v: variant for v in variants()}}}
    checks = CATALOG["e22"].checks(report)
    assert checks and all(c.ok for c in checks)
    bad = {"metrics": {"variants": {v: variant | {"distinct_responses_per_command_id": 2} for v in variants()}}}
    assert not all(c.ok for c in CATALOG["e22"].checks(bad))
