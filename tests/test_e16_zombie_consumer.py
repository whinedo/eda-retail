"""E16 Consumidor zombie y rebalanceo (spec experiments): análisis de los logs de las instancias, muestreo de la saga
y H16, sin Kafka ni PostgreSQL. La ejecución de punta a punta está en tests/integration/test_e16_zombie_consumer.py.
"""

from __future__ import annotations

from eda_retail.experiments.e16_zombie_consumer import (
    SagaSampler,
    double_processed,
    h16,
    lock_waits,
    overlaps,
    rebalance,
    taken_over,
    zombie,
)
from eda_retail.experiments.processes import Message, ProcessLog

FROZEN, RESUMED = 100.0, 112.0


def _logs() -> dict[str, ProcessLog]:
    a = ProcessLog(
        rebalances=[(90.0, "assigned", [0, 2]), (112.5, "lost", [0, 2]), (113.0, "assigned", [1])],
        messages=[Message("StepCompleted", "I-1", 2, 5, 99.0),  # a medio procesar al congelarse
                  Message("PlanCreated", "I-2", 0, 3, 98.0, 98.5, "processed"),
                  Message("StepFailed", "I-3", 1, 8, 114.0, 114.1, "duplicate")],
        lock_waits=[(99.0, "I-1", 0.001, "MainThread")])
    b = ProcessLog(
        rebalances=[(90.0, "assigned", [1]), (107.0, "revoked", [1]), (107.2, "assigned", [0, 1, 2])],
        messages=[Message("StepCompleted", "I-1", 2, 5, 108.0, 109.0, "processed"),
                  Message("PlanRevised", "I-4", 1, 2, 108.5, 108.6, "processed")],
        lock_waits=[(109.0, "I-1", 0.8, "MainThread"), (110.0, "I-4", 0.01, "saga-deadlines")])
    return {"orchestrator-a": a, "orchestrator-b": b}


def test_rebalance_while_the_instance_is_frozen():
    """Rebalanceo mientras la instancia está congelada: B recibe las particiones de A, procesa sus mensajes pendientes
    y el informe registra el instante del rebalanceo"""
    logs = _logs()
    moved = rebalance(logs, "orchestrator-a", FROZEN, RESUMED)
    assert moved == {"victim": "orchestrator-a", "victim_partitions": [0, 2], "reassigned_after_s": 7.2,
                     "resumed_after_s": 12.0, "reassigned_before_resume": True}
    assert taken_over(logs, "orchestrator-a", [0, 2], FROZEN, RESUMED) == {"messages": 1,
                                                                           "outcomes": {"processed": 1}}


def test_reassignment_after_the_resume_is_not_before():
    logs = _logs()
    logs["orchestrator-b"].rebalances[-1] = (112.4, "assigned", [0, 1, 2])
    assert rebalance(logs, "orchestrator-a", FROZEN, RESUMED)["reassigned_before_resume"] is False
    logs["orchestrator-b"].rebalances.pop()
    assert rebalance(logs, "orchestrator-a", FROZEN, RESUMED)["reassigned_after_s"] is None


def test_resumed_zombie():
    """Reanudación del zombie: el informe da el mensaje a medio procesar y lo que procesó después (un `duplicate` es
    un descarte del inbox)"""
    z = zombie(_logs()["orchestrator-a"], FROZEN, RESUMED)
    assert z["in_flight_at_freeze"] == [{"event_type": "StepCompleted", "incident": "I-1", "partition": 2,
                                         "offset": 5, "outcome": None}]
    assert [m["incident"] for m in z["processed_after_resume"]] == ["I-3"]
    assert z["outcomes_after_resume"] == {"duplicate": 1}
    assert [kind for _, kind, _ in z["rebalances_after_resume"]] == ["lost", "assigned"]


def test_message_processed_by_both_instances():
    """Reanudación del zombie: el mensaje que A tenía a medio procesar y B procesó aparece con el resultado de cada
    instancia"""
    assert double_processed(_logs()) == [{"partition": 2, "offset": 5,
                                          "outcomes": {"orchestrator-a": "unfinished", "orchestrator-b": "processed"}}]


def test_overlap_and_lock_waits_per_incident():
    """El informe da, por incidencia, si dos instancias procesaron mensajes de ella solapados en el tiempo y qué
    transacciones esperaron al bloqueo de la incidencia"""
    by_incident = overlaps(_logs(), end_of_run=200.0)
    assert by_incident["I-1"] == {"instances": ["orchestrator-a", "orchestrator-b"], "overlap": True}
    assert by_incident["I-2"] == {"instances": ["orchestrator-a"], "overlap": False}
    assert by_incident["I-4"] == {"instances": ["orchestrator-b"], "overlap": False}
    assert lock_waits(_logs(), FROZEN) == [{"instance": "orchestrator-b", "incident": "I-1", "wait_s": 0.8,
                                            "thread": "MainThread", "at_s": 9.0}]


def test_sequential_messages_of_two_instances_do_not_overlap():
    a = ProcessLog(messages=[Message("PlanCreated", "I-1", 0, 1, 1.0, 2.0, "processed")])
    b = ProcessLog(messages=[Message("StepCompleted", "I-1", 0, 2, 2.5, 3.0, "processed")])
    assert overlaps({"a": a, "b": b}, end_of_run=10.0)["I-1"]["overlap"] is False


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def fetchall(self):
        return self._rows


class _Conn:
    def __init__(self):
        self.steps: list[dict] = []
        self.incidents: list[dict] = []

    def execute(self, sql, *_args):
        return _Rows(self.steps if "plan_steps" in sql else self.incidents)

    def commit(self):
        pass


class _Harness:
    def __init__(self):
        self.conn = _Conn()


def test_sampler_flags_steps_that_leave_completed():
    """Reanudación del zombie: `orchestrator.plan_steps` no retrocede ningún paso de `completed` a otro estado (la
    compensación de un paso completado sí es válida)"""
    h = _Harness()
    sampler = SagaSampler(h)  # type: ignore[arg-type]
    h.conn.steps = [{"incident_id": "I-1", "step_id": "S1", "status": "completed"},
                    {"incident_id": "I-1", "step_id": "S3", "status": "completed"}]
    h.conn.incidents = [{"incident_id": "I-1", "status": "open"}]
    sampler()
    h.conn.steps = [{"incident_id": "I-1", "step_id": "S1", "status": "requested"},
                    {"incident_id": "I-1", "step_id": "S3", "status": "compensating"}]
    sampler()
    h.conn.steps[1]["status"] = "compensated"
    h.conn.incidents = [{"incident_id": "I-1", "status": "resolved"}]
    sampler()
    h.conn.incidents = [{"incident_id": "I-1", "status": "open"}]
    sampler()
    assert sampler.invalid == [{"incident": "I-1", "step": "S1", "from": "completed", "to": "requested"},
                               {"incident": "I-1", "step": None, "from": "resolved", "to": "open"}]
    assert sampler.samples == 4


def _clean() -> dict:
    return {f"I{i}": {"violations": 0} for i in range(1, 6)}


def test_h16_holds():
    """Cumplimiento de H16: sin comandos duplicados con `command_id` distinto para el mismo paso y versión, sin
    transiciones inválidas e I1 a I5 sin violaciones"""
    ok = {"duplicate_commands": [], "invalid_transitions": [], "invariants": _clean()}
    assert h16(ok)
    assert not h16(ok | {"duplicate_commands": [{"incident_id": "I-1", "step_id": "S2", "plan_version": 1,
                                                 "commands": 2}]})
    assert not h16(ok | {"invalid_transitions": [{"incident": "I-1", "step": "S1", "from": "completed",
                                                  "to": "requested"}]})
    assert not h16(ok | {"invariants": _clean() | {"I4": {"violations": 1}}})


def test_expexecutor_checks_of_e16():
    from eda_retail.expexecutor.catalog import CATALOG

    report = {"hypothesis": {"H16": True},
              "metrics": {"rebalance": {"reassigned_before_resume": True}, "duplicate_commands": [],
                          "invalid_transitions": [], "invariants": _clean()}}
    assert all(c.ok for c in CATALOG["e16"].checks(report))
    report["metrics"]["rebalance"]["reassigned_before_resume"] = False
    assert [c.metric for c in CATALOG["e16"].checks(report) if not c.ok] == ["metrics.rebalance.reassigned_before_resume"]
