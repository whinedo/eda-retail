"""E5 Calidad del plan (hipótesis H5, spec experiments).

El planificador aislado (decisión del usuario, L15): recibe la interpretación etiquetada del
generador (causa, gravedad y retraso) y los pedidos, con el mismo prompt, las mismas
comprobaciones de código (`validate_plan`), el mismo harness de agentes (grounding de los
pedidos) y los mismos reintentos que el agente. Su plan
acierta si propone las mismas acciones que el plan esperado (supuesto A1), sin importar el
orden ni los `step_id`.

Supuesto A9: con gravedad `low` el plan lo fija el código sin llamar al LLM; esas
incidencias se cuentan aparte (`code_planned`) y no entran en el acierto. H5 se cumple si la
media del acierto de las repeticiones alcanza el umbral (0,8 por defecto).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Callable
from decimal import Decimal
from typing import Any

from eda_retail.config import Settings
from eda_retail.events.ids import incident_id_for_notice
from eda_retail.experiments.oracle import Oracle
from eda_retail.experiments.report import Report
from eda_retail.llm.generation import Budget, LlmClient
from eda_retail.llm.harness import AgentHarness
from eda_retail.llm.prompts import planner
from eda_retail.llm.schemas import PlanProposal
from eda_retail.planner.handler import check_plan, order_lines
from eda_retail.rules import BusinessRules
from eda_retail.synthetic.generator import Dataset, SyntheticNotice
from eda_retail.synthetic.generator import generate as generate_data

LlmFactory = Callable[[Dataset, int], LlmClient]  # (datos, número de repetición)


def _oracle(data: Dataset, repetition: int) -> LlmClient:
    return Oracle(data).client()


class _Evaluation:
    def __init__(self, data: Dataset, llm: LlmClient, rules: BusinessRules) -> None:
        self.llm, self.rules = llm, rules
        self.harness = AgentHarness(llm, max_generations=rules.max_generations)
        self._orders = {o.order_id: o for o in data.orders}
        self.calls: list[Any] = []

    def propose(self, notice: SyntheticNotice) -> tuple[list[str], str | None]:
        """El plan con el prompt y las reglas del componente `planner` (pedidos con su importe)."""
        o = self._orders[notice.order_id]
        amounts = {o.order_id: Decimal(o.amount)}

        def check(plan: PlanProposal) -> str | None:
            result = check_plan(plan.command_steps(), self.rules, amounts)
            return result if isinstance(result, str) else None

        request = planner.plan_request(incident_id_for_notice(notice.carrier_notice_id), notice.cause,
                                       notice.severity, notice.delay_hours, order_lines(amounts))
        budget = Budget(0, 0, self.rules.max_tokens_per_incident, self.rules.max_llm_calls_per_incident)
        gen = self.harness.run(request, budget=budget, check=check, ground=planner.ground_plan(list(amounts)))
        self.calls += gen.calls
        if gen.value is None:
            return [], gen.reason
        return [s.action for s in gen.value.steps], None


def _repetition(n: int, data: Dataset, llm: LlmClient, rules: BusinessRules) -> dict[str, Any]:
    ev = _Evaluation(data, llm, rules)
    hits: dict[str, list[bool]] = defaultdict(list)
    mismatches, code_planned = [], 0
    for notice in data.notices:
        if notice.severity == "low":
            code_planned += 1
            continue
        expected = [s["action"] for s in notice.expected_steps]
        proposed, reason = ev.propose(notice)
        ok = sorted(proposed) == sorted(expected)
        hits[notice.cause].append(ok)
        if not ok:
            mismatches.append({"order_id": notice.order_id, "cause": notice.cause, "severity": notice.severity,
                               "expected": expected, "proposed": proposed, "reason": reason})
    total = [h for v in hits.values() for h in v]
    return {
        "repetition": n, "llm_seed": getattr(llm, "seed", None), "temperature": getattr(llm, "temperature", None),
        "accuracy": sum(total) / len(total) if total else None,
        "by_cause": {c: sum(v) / len(v) for c, v in sorted(hits.items())},
        "llm_planned": len(total), "code_planned": code_planned, "mismatches": mismatches,
        "llm_calls": len(ev.calls), "tokens_in": sum(c.tokens_in for c in ev.calls),
        "tokens_out": sum(c.tokens_out for c in ev.calls),
    }


def run_e5(base: Settings | None = None, *, seed: int = 42, incidents: int = 40, repetitions: int = 3,
           threshold: float = 0.8, llm: LlmFactory | None = None) -> Report:
    data = generate_data(seed, incidents)
    factory = llm or _oracle
    rules = BusinessRules()
    runs, model = [], "unknown"
    for n in range(1, repetitions + 1):
        client = factory(data, n)
        model = getattr(client, "model", "unknown")
        runs.append(_repetition(n, data, client, rules))
    accuracies = [r["accuracy"] for r in runs if r["accuracy"] is not None]
    mean = sum(accuracies) / len(accuracies) if accuracies else 0.0
    metrics = {
        "accuracy_mean": mean, "accuracy_range": [min(accuracies), max(accuracies)] if accuracies else None,
        "accuracy_per_repetition": [r["accuracy"] for r in runs], "threshold": threshold,
    }
    return Report(
        experiment="E5", seed=seed, model=model,
        config={"incidents": incidents, "repetitions": repetitions, "threshold": threshold,
                "temperature": runs[0]["temperature"] if runs else None},
        metrics=metrics, hypothesis={"H5": mean >= threshold}, runs=runs,
    )
