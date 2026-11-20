"""E5 Calidad del plan: el planificador aislado frente a los planes esperados (cliente LLM falso)."""

from __future__ import annotations

import re

from eda_retail.experiments.e5_plan_quality import run_e5
from eda_retail.experiments.oracle import Oracle
from eda_retail.llm.fake import FakeLlmClient
from eda_retail.synthetic.generator import generate

_ORDER = re.compile(r"P-S\d+N\d{4}")


def _wrong_on(cause: str, seed: int, incidents: int) -> FakeLlmClient:
    """Responde el plan esperado salvo en una causa, donde solo avisa al cliente."""
    data = generate(seed, incidents)
    oracle = Oracle(data)
    causes = {n.order_id: n.cause for n in data.notices}

    def respond(request):
        order = _ORDER.search(request.prompt).group(0)
        if causes[order] == cause:
            return {"steps": [{"step_id": "S1", "action": "notify_customer", "params": {"order_id": order},
                               "depends_on": []}]}
        return oracle(request)
    client = FakeLlmClient(responder=respond)
    client.model = "fake-partial"
    return client


def test_planner_evaluation():
    """Evaluación del planificador"""
    report = run_e5(seed=4, incidents=40, repetitions=1, llm=lambda data, n: _wrong_on("wrong_address", 4, 40))
    run = report.runs[0]
    assert set(run["by_cause"]) == {"delay", "loss", "damage", "wrong_address", "strike", "unknown"}
    assert run["by_cause"]["wrong_address"] == 0.0 and run["by_cause"]["loss"] == 1.0
    assert run["mismatches"] and {m["cause"] for m in run["mismatches"]} == {"wrong_address"}
    m = run["mismatches"][0]
    assert m["expected"] == ["notify_customer", "reserve_stock", "reissue_shipment"]
    assert m["proposed"] == ["notify_customer"]
    assert 0.0 < run["accuracy"] < 1.0 and report.config["incidents"] == 40


def test_quality_gate():
    """Puerta de calidad"""
    report = run_e5(seed=4, incidents=40, repetitions=1, threshold=0.95,
                    llm=lambda data, n: _wrong_on("loss", 4, 40))
    assert report.metrics["accuracy_mean"] < 0.95
    assert report.hypothesis == {"H5": False}
    assert run_e5(seed=4, incidents=12, repetitions=1).hypothesis == {"H5": True}  # oráculo: acierto total


def test_three_repetitions_of_e5():
    """Tres repeticiones de E5"""
    seen = []

    def factory(data, n):
        seen.append(n)
        return Oracle(data).client()
    report = run_e5(seed=4, incidents=12, repetitions=3, llm=factory)
    assert seen == [1, 2, 3] and [r["repetition"] for r in report.runs] == [1, 2, 3]
    assert all("accuracy" in r for r in report.runs)
    assert report.metrics["accuracy_range"] == [1.0, 1.0] and report.config["repetitions"] == 3


def test_e5_with_the_planner_harness():
    """E5 con el harness del planificador"""
    client = FakeLlmClient(responder=lambda request: {"steps": [
        {"step_id": "S1", "action": "notify_customer", "params": {"order_id": "P-S9N9999"}, "depends_on": []}]})
    report = run_e5(seed=4, incidents=6, repetitions=1, llm=lambda data, n: client)
    run = report.runs[0]
    assert run["llm_planned"] > 0 and run["accuracy"] == 0.0
    assert {m["reason"] for m in run["mismatches"]} == {"llm_output_invalid"}
    assert len(run["mismatches"]) == run["llm_planned"] and run["llm_calls"] == 3 * run["llm_planned"]
    retries = [r.prompt for r in client.requests if "se rechazó" in r.prompt]
    assert retries and all("ungrounded_reference" in p for p in retries)
