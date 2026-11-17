"""E18 Mensajes veneno en mitad de la partición (spec experiments): reconciliación y veredicto de H18.

Sin Kafka ni PostgreSQL: las filas de las dead letters se construyen a mano. La ejecución de punta a
punta está en tests/integration/test_e18_poison.py.
"""

from __future__ import annotations

from eda_retail.experiments.e18_poison import h18, pick_targets, reconcile
from eda_retail.synthetic import poison
from eda_retail.synthetic.poison import InjectedPoison

INCIDENTS = [f"I-{n}" for n in range(6)]
CONSUMERS = [
    ("interpreter", "interpreter", lambda t: t is None or t in {"CarrierNoticeReceived", "OrderRegistered"}),
    ("orchestrator", "orchestrator", lambda t: t is None or t == "ShipmentDelayed"),
    ("event_log", "event_log", lambda t: True),
    ("orders", "views", lambda t: t is None or t == "OrderRegistered"),
]


def _injected() -> list[InjectedPoison]:
    return [InjectedPoison(p, i % 3, 100 + i) for i, p in enumerate(poison.poison_set(18, INCIDENTS))]


def _rows(injected: list[InjectedPoison]) -> list[dict]:
    """Lo que deja un sistema correcto: una dead letter publicada por cada consumidor que atiende el veneno."""
    rows = []
    for item in injected:
        p = item.poison
        if p.kind == poison.UNKNOWN_TYPE:
            continue
        for name, _, attends in CONSUMERS:
            if attends(p.event_type):
                rows.append({"consumer": name, "event_id": str(p.event_id) if p.event_id else None,
                             "reason": "invalid_schema", "raw_message": poison.raw_dead_letter(p), "published": True})
    return rows


def _metrics(rows, *, in_log=None, lag=0, open_=0) -> dict:
    injected = _injected()
    unknown = next(i.poison for i in injected if i.poison.kind == poison.UNKNOWN_TYPE)
    in_log = {str(unknown.event_id)} if in_log is None else in_log
    statuses = {inc: ("escalated", "invalid_schema") for inc in INCIDENTS}
    return {"lag": {"orchestrator": lag, "event_log": 0}, "open": open_,
            **reconcile(injected, CONSUMERS, rows, retries={}, in_log=in_log, statuses=statuses)}


def test_dlq_reconciliation():
    """Reconciliación de la DLQ: cada veneno aparece exactamente una vez por cada consumidor que atiende su
    tipo, y en ninguno más, y la ejecución no tiene ningún otro mensaje en la DLQ"""
    metrics = _metrics(_rows(_injected()))
    assert metrics["unexpected_dead_letters"] == 0
    poisons = metrics["poisons"]
    assert all(p["ok"] for p in poisons.values())
    assert {n for n, c in poisons[poison.INVALID_JSON]["consumers"].items() if c["expected"]} == {
        "interpreter", "orchestrator", "event_log", "orders"}
    assert {n for n, c in poisons[poison.PERSONAL_DATA]["consumers"].items() if c["expected"]} == {
        "orchestrator", "event_log"}
    assert poisons[poison.INVALID_JSON]["escalated"] is None
    assert poisons[poison.MISSING_FIELD]["escalated"] is True


def test_h18_holds():
    """Cumplimiento de H18: lag 0, cada veneno en la DLQ de quien atiende su tipo, el tipo desconocido en el log
    y todas las incidencias terminan"""
    assert h18(_metrics(_rows(_injected())))


def test_unknown_type_in_a_dead_letter_fails():
    """Evento de un tipo desconocido: ninguno lo lleva a la dead letter queue"""
    rows = _rows(_injected())
    unknown = next(i.poison for i in _injected() if i.poison.kind == poison.UNKNOWN_TYPE)
    rows.append({"consumer": "orchestrator", "event_id": str(unknown.event_id), "reason": "invalid_schema",
                 "raw_message": {}, "published": True})
    metrics = _metrics(rows)
    assert not metrics["poisons"][poison.UNKNOWN_TYPE]["ok"] and not h18(metrics)


def test_unknown_type_missing_from_the_log_fails():
    """Evento de un tipo desconocido: event_log lo guarda en event_log.events"""
    assert not h18(_metrics(_rows(_injected()), in_log=set()))


def test_poison_written_twice_fails():
    rows = _rows(_injected())
    rows.append(dict(rows[0]))
    assert not h18(_metrics(rows))


def test_poison_missing_from_an_attending_consumer_fails():
    """Evento con datos personales: va a la dead letter queue de cada consumidor que atiende su tipo"""
    rows = [r for r in _rows(_injected()) if not (r["consumer"] == "event_log" and r["event_id"]
            and r["event_id"] == str(next(i.poison.event_id for i in _injected()
                                         if i.poison.kind == poison.PERSONAL_DATA)))]
    metrics = _metrics(rows)
    assert not metrics["poisons"][poison.PERSONAL_DATA]["ok"] and not h18(metrics)


def test_personal_data_in_the_log_fails():
    """Evento con datos personales: no se escribe en el log"""
    pii = next(i.poison for i in _injected() if i.poison.kind == poison.PERSONAL_DATA)
    unknown = next(i.poison for i in _injected() if i.poison.kind == poison.UNKNOWN_TYPE)
    assert not h18(_metrics(_rows(_injected()), in_log={str(pii.event_id), str(unknown.event_id)}))


def test_unpublished_dead_letter_fails():
    rows = _rows(_injected())
    rows[0]["published"] = False
    assert not h18(_metrics(rows))


def test_other_dead_letters_fail():
    rows = _rows(_injected()) + [{"consumer": "orchestrator", "event_id": "x", "reason": "retries_exhausted",
                                  "raw_message": {}, "published": True}]
    metrics = _metrics(rows)
    assert metrics["unexpected_dead_letters"] == 1 and not h18(metrics)


def test_lag_or_open_incidents_fail():
    assert not h18(_metrics(_rows(_injected()), lag=3))
    assert not h18(_metrics(_rows(_injected()), open_=1))


def test_targets_repeat_when_there_are_few_open_incidents():
    assert pick_targets(1, ["I-a", "I-b"], 6) == pick_targets(1, ["I-a", "I-b"], 6)
    assert len(pick_targets(1, ["I-a", "I-b"], 6)) == 6 and set(pick_targets(1, ["I-a", "I-b"], 6)) == {"I-a", "I-b"}
    assert len(set(pick_targets(1, INCIDENTS, 6))) == 6


def test_expexecutor_checks_of_e18():
    """El expexecutor comprueba lag, venenos, otras dead letters e incidencias abiertas de E18"""
    from eda_retail.expexecutor.catalog import CATALOG

    clean = {f"I{i}": {"violations": 0} for i in range(1, 6)}
    metrics = _metrics(_rows(_injected())) | {"invariants": clean}
    assert all(c.ok for c in CATALOG["e18"].checks({"metrics": metrics}))
    broken = _metrics(_rows(_injected())[1:], lag=2) | {"invariants": clean}
    failing = [c.metric for c in CATALOG["e18"].checks({"metrics": broken}) if not c.ok]
    assert failing == ["metrics.lag (todos los grupos)", "metrics.poisons (en la DLQ de quien atiende su tipo)"]
