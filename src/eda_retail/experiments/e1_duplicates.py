"""E1 Duplicados en el bus (hipótesis H1, spec experiments).

Entrega cada evento 2 y 3 veces y compara el sistema con idempotencia (`full`) y sin la
idempotencia de los efectos (`no_effect_keys`: los ejecutores sin inbox, `effect_keys` ni
servicios con clave; supuesto A3). H1: con idempotencia, 0 efectos duplicados en todas las
ejecuciones; sin ella, más de 0. El modo `none` (todo desactivado) se informa como
observación y no entra en H1.
"""

from __future__ import annotations

from eda_retail.config import Settings
from eda_retail.experiments.harness import IDEMPOTENCY_MODES, RunConfig, RunResult, run
from eda_retail.experiments.report import Report
from eda_retail.synthetic.faults import FaultPlan


def _row(deliveries: int, result: RunResult) -> dict:
    return {
        "deliveries": deliveries, "idempotency": result.config.idempotency, "closed": result.closed,
        "open": result.open, "escalated": result.escalated, "duplicates": result.duplicates, "duplicates_total": result.repeated_steps,
        "consumer_crashes": result.consumer_crashes, "poison_messages": result.poison_messages,
    }


def run_e1(base: Settings, *, seed: int = 42, incidents: int = 50, deliveries: tuple[int, ...] = (2, 3)) -> Report:
    rows = []
    for n in deliveries:
        for idempotency in IDEMPOTENCY_MODES:
            config = RunConfig(seed=seed, incidents=incidents, faults=FaultPlan(seed=seed, duplicates=n),
                               idempotency=idempotency)
            rows.append(_row(n, run(base, config)))
    with_keys = [r for r in rows if r["idempotency"] == "full"]
    without = [r for r in rows if r["idempotency"] == "no_effect_keys"]
    h1 = all(r["duplicates_total"] == 0 for r in with_keys) and all(r["duplicates_total"] > 0 for r in without)
    return Report(
        experiment="E1", seed=seed, model="oracle",
        config={"incidents": incidents, "deliveries": list(deliveries), "modes": list(IDEMPOTENCY_MODES)},
        metrics={"runs": rows}, hypothesis={"H1": h1},
    )


__all__ = ["run_e1"]
