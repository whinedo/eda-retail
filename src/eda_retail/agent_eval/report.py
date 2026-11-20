"""Evaluación por agente (spec agent-harness, «Evaluación por agente»): lee las ejecuciones del harness grabadas en un
directorio (`outcomes-*.jsonl`, `llm/replay.py`) y las resume por agente y `prompt_version`. No escribe nada."""

from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from eda_retail.llm.replay import read_jsonl


def agent_report(directory: str | Path) -> dict[str, dict[str, Any]]:
    """Un resumen por `<agente>@<prompt_version>`."""
    groups: dict[str, list[dict[str, Any]]] = {}
    for outcome in read_jsonl(directory, "outcomes"):
        groups.setdefault(f"{outcome['agent']}@{outcome['prompt_version']}", []).append(outcome)
    return {name: _summary(runs) for name, runs in sorted(groups.items())}


def _summary(runs: list[dict[str, Any]]) -> dict[str, Any]:
    attempts = [a for run in runs for a in run["attempts"]]
    with_calls = [run for run in runs if run["attempts"]]
    first = sum(1 for run in with_calls if run["attempts"][0]["validation_passed"])
    return {
        "runs": len(runs),
        "first_attempt_rate": round(first / len(with_calls), 4) if with_calls else None,
        "mean_attempts": round(len(attempts) / len(with_calls), 4) if with_calls else None,
        "rejections": dict(sorted(Counter(a["rejection"] for a in attempts if a["rejection"]).items())),
        "escalations": dict(sorted(Counter(run["reason"] for run in runs if run["reason"]).items())),
        "tokens_in": sum(a["tokens_in"] for a in attempts),
        "tokens_out": sum(a["tokens_out"] for a in attempts),
    }
