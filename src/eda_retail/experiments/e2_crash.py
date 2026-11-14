"""E2 Caída de un ejecutor (hipótesis H2, spec experiments).

El ejecutor de `notify_customer` (todas las incidencias tienen ese paso) cae tras completar
la mitad de sus pasos, sin confirmar el último offset, y se reinicia. H2: todas las
incidencias terminan en `IncidentResolved` o `IncidentEscalated` y no se repite ningún paso.
"""

from __future__ import annotations

from eda_retail.config import Settings
from eda_retail.experiments.harness import RunConfig, run
from eda_retail.experiments.report import Report, summary
from eda_retail.synthetic.faults import FaultPlan

CRASHING = "executor-notify_customer"


def run_e2(base: Settings, *, seed: int = 42, incidents: int = 50) -> Report:
    faults = FaultPlan(seed=seed, crash_after={CRASHING: max(1, incidents // 2)})
    result = run(base, RunConfig(seed=seed, incidents=incidents, faults=faults))
    metrics = {
        "closed": result.closed, "open": result.open, "resolved": result.resolved, "escalated": result.escalated,
        "consumer_crashes": result.consumer_crashes, "repeated_steps": result.repeated_steps,
        "latency_ms": summary(result.latencies_ms),
    }
    h2 = result.open == 0 and result.repeated_steps == 0
    return Report(
        experiment="E2", seed=seed, model=result.model,
        config={"incidents": incidents, "crashing": CRASHING, "crash_after": incidents // 2},
        metrics=metrics, hypothesis={"H2": h2},
    )
