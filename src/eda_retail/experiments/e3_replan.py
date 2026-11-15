"""E3 Fallo de un paso y replanificación (hipótesis H3, spec experiments).

Solo incidencias de pérdida (`loss`: aviso, reclamación, reserva y reenvío) con los dos
almacenes sin existencias (supuesto A5). La reserva falla, el replanner quita la reserva y
el reenvío y propone un reembolso. H3: todas las incidencias terminan con un reembolso o
con un escalado, ningún paso completado se repite y ningún reembolso supera el importe del
pedido ni el máximo configurado.
"""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from eda_retail.config import Settings
from eda_retail.experiments.harness import Harness, RunConfig, started
from eda_retail.experiments.report import Report
from eda_retail.rules import BusinessRules
from eda_retail.synthetic.faults import FaultPlan
from eda_retail.synthetic.generator import VARIANTS, WAREHOUSES, Dataset, generate

CAUSE = "loss"


def only_cause(seed: int, cause: str, incidents: int) -> Dataset:
    """Las primeras `incidents` incidencias de una causa en un conjunto generado con la semilla."""
    data = generate(seed, incidents * len(VARIANTS))
    notices = tuple(n for n in data.notices if n.cause == cause)[:incidents]
    orders = {n.order_id for n in notices}
    return replace(data, notices=notices, orders=tuple(o for o in data.orders if o.order_id in orders))


def repeated_completions(h: Harness) -> int:
    """Pasos completados más de una vez en una incidencia (mismo `step_id`)."""
    row = h.conn.execute(
        "SELECT count(*) - count(DISTINCT (correlation_id, payload->>'step_id')) AS n "
        "FROM event_log.events WHERE event_type = 'StepCompleted'"
    ).fetchone()
    h.conn.commit()
    return row["n"]


def run_e3(base: Settings, *, seed: int = 42, incidents: int = 30) -> Report:
    data = only_cause(seed, CAUSE, incidents)
    config = RunConfig(seed=seed, incidents=incidents, faults=FaultPlan(seed=seed, empty_warehouses=WAREHOUSES))
    with started(base, config, data=data) as h:
        result = h.result(h.elapsed)
        refunds = h.conn.execute(
            "SELECT correlation_id, payload->>'order_id' AS order_id, (payload->>'amount')::numeric AS amount "
            "FROM event_log.events WHERE event_type = 'RefundIssued'"
        ).fetchall()
        replans = h.conn.execute("SELECT count(*) AS n FROM event_log.events WHERE event_type = 'PlanRevised'").fetchone()["n"]
        h.conn.commit()
        repeated = repeated_completions(h) + result.repeated_steps
    amounts = {o.order_id: o.amount for o in data.orders}
    limit = BusinessRules().max_refund_amount
    over = sum(1 for r in refunds if r["amount"] > min(Decimal(amounts[r["order_id"]]), limit))
    refunded = len({r["correlation_id"] for r in refunds})
    metrics = {
        "closed": result.closed, "open": result.open, "refunded": refunded, "escalated": result.escalated,
        "replans": replans, "repeated_steps": repeated, "refunds_over_limit": over,
    }
    h3 = result.open == 0 and refunded + result.escalated == incidents and repeated == 0 and over == 0
    return Report(
        experiment="E3", seed=seed, model=result.model,
        config={"incidents": incidents, "cause": CAUSE, "empty_warehouses": list(WAREHOUSES),
                "max_refund_amount": str(limit)},
        metrics=metrics, hypothesis={"H3": h3},
    )
