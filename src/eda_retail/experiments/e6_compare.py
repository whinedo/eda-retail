"""E6 Eventos frente a síncrono (hipótesis H6, spec experiments).

Cada repetición ejecuta los mismos avisos, con el mismo LLM, en la versión de eventos (el
arnés) y en la base síncrona (`baseline_sync`), en dos escenarios (supuesto A10):

- `no_faults`: sin fallos inyectados.
- `faults`: cada evento entregado 2 veces (en la base síncrona, cada aviso recibido 2 veces)
  y un 30 % de fallos en `reserve_stock`. La caída de un consumidor no entra: no tiene
  equivalente síncrono.

Por versión y escenario, el informe da la latencia por incidencia (media, mediana y p95 de
todas las repeticiones), los tokens, las llamadas y el coste en dinero (tokens por los
precios de la configuración), las incidencias resueltas, reembolsadas y escaladas, y los
efectos duplicados. H6 no fija umbral: el informe describe las diferencias.
"""

from __future__ import annotations

from collections.abc import Callable
from decimal import Decimal
from typing import Any

from eda_retail.baseline_sync.runner import sync_started
from eda_retail.config import Settings
from eda_retail.experiments.harness import RunConfig, started
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report, summary
from eda_retail.llm.generation import LlmClient
from eda_retail.synthetic.faults import FaultPlan
from eda_retail.synthetic.generator import Dataset, generate

SCENARIOS = ("no_faults", "faults")
DUPLICATES = 2
FAILING = {"reserve_stock": 0.3}


def _faults(scenario: str, seed: int) -> FaultPlan:
    if scenario == "faults":
        return FaultPlan(seed=seed, duplicates=DUPLICATES, fail_probability=FAILING)
    return FaultPlan(seed=seed)


# Reembolsos de cada versión: el ejecutor de issue_refund y la base síncrona tienen su propia tabla.
REFUNDS = {"events": "executor_issue_refund.refunds", "sync": "baseline_sync.refunds"}


def _refunded(conn: Any, version: str) -> int:
    with conn.transaction():
        return conn.execute(f"SELECT count(DISTINCT incident_id) AS n FROM {REFUNDS[version]}").fetchone()["n"]


def _row(version: str, result: Any, refunded: int, base: Settings) -> dict[str, Any]:
    cost = (Decimal(result.tokens_in) / 1000 * base.llm_price_in_per_1k
            + Decimal(result.tokens_out) / 1000 * base.llm_price_out_per_1k)
    return {
        "version": version, "closed": result.closed, "resolved": result.resolved, "escalated": result.escalated,
        "refunded": refunded, "duplicates": sum(result.duplicates.values()), "latencies_ms": result.latencies_ms,
        "llm_calls": result.llm_calls, "tokens_in": result.tokens_in, "tokens_out": result.tokens_out,
        "cost": float(cost), "elapsed_seconds": result.elapsed_seconds,
    }


def _run_events(base: Settings, config: RunConfig, data: Dataset, llm: LlmClient) -> dict[str, Any]:
    with started(base, config, data=data, llm=llm) as h:
        result = h.result(h.elapsed)
        return _row("events", result, _refunded(h.conn, "events"), base)


def _run_sync(base: Settings, config: RunConfig, data: Dataset, llm: LlmClient) -> dict[str, Any]:
    with sync_started(base, config, data=data, llm=llm) as run:
        result = run.result()
        return _row("sync", result, _refunded(run.conn, "sync"), base)


def _spread(values: list[float]) -> dict[str, float]:
    return {"mean": sum(values) / len(values), "min": min(values), "max": max(values)}


def _aggregate(rows: list[dict[str, Any]]) -> dict[str, Any]:
    out: dict[str, Any] = {"latency_ms": summary([x for r in rows for x in r["latencies_ms"]]),
                           "duplicates": sum(r["duplicates"] for r in rows)}
    for key in ("closed", "resolved", "escalated", "refunded", "llm_calls", "tokens_in", "tokens_out", "cost"):
        out[key] = _spread([r[key] for r in rows])
    return out


def run_e6(base: Settings, *, seed: int = 42, incidents: int = 50, repetitions: int = 3,
           llm: Callable[[Dataset, int], LlmClient] | None = None, scenarios: tuple[str, ...] = SCENARIOS) -> Report:
    data = generate(seed, incidents)
    factory = llm or (lambda d, n: Oracle(d).client())
    runs, model = [], "unknown"
    for n in range(1, repetitions + 1):
        for scenario in scenarios:
            config = RunConfig(seed=seed, incidents=incidents, faults=_faults(scenario, seed))
            for version, runner in (("events", _run_events), ("sync", _run_sync)):
                client = factory(data, n)
                model = getattr(client, "model", "unknown")
                row = runner(base, config, data, client)
                runs.append({"repetition": n, "scenario": scenario, "llm_seed": getattr(client, "seed", None),
                             "temperature": getattr(client, "temperature", None), **row})
    metrics = {s: {v: _aggregate([r for r in runs if r["scenario"] == s and r["version"] == v])
                   for v in ("events", "sync")} for s in scenarios}
    return Report(
        experiment="E6", seed=seed, model=model,
        config={"incidents": incidents, "repetitions": repetitions, "scenarios": list(scenarios),
                "temperature": runs[0]["temperature"] if runs else None,
                "faults": {"duplicates": DUPLICATES, "fail_probability": FAILING},
                "prices": {"in_per_1k": str(base.llm_price_in_per_1k), "out_per_1k": str(base.llm_price_out_per_1k)}},
        metrics=metrics, hypothesis={},
        runs=[{k: v for k, v in r.items() if k != "latencies_ms"} for r in runs],
    )
