"""Informe reproducible de un experimento (spec experiments, «Informe reproducible»).

Cada informe guarda la semilla, la configuración, el modelo, la versión de los prompts
(sha256 de los prompts de sistema de los agentes) y las métricas, en
`<base>/<exp>-<fecha>/report.json` y `report.md`.
"""

from __future__ import annotations

import hashlib
import json
import statistics
from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from eda_retail.llm.prompts import interpreter, planner, replanner, writer


def prompts_version() -> str:
    digest = hashlib.sha256()
    for module in (interpreter, planner, replanner, writer):
        digest.update(module.SYSTEM.encode())
    return digest.hexdigest()


def summary(values: list[float]) -> dict[str, float | None]:
    """Media, mediana y percentil 95 (spec E6)."""
    if not values:
        return {"mean": None, "median": None, "p95": None}
    ordered = sorted(values)
    p95 = ordered[min(len(ordered) - 1, round(0.95 * (len(ordered) - 1)))]
    return {"mean": statistics.fmean(ordered), "median": statistics.median(ordered), "p95": p95}


@dataclass
class Report:
    experiment: str
    seed: int
    config: dict[str, Any]
    model: str
    metrics: dict[str, Any]
    hypothesis: dict[str, bool]
    runs: list[dict[str, Any]] = field(default_factory=list)
    prompts_version: str = field(default_factory=prompts_version)


def _plain(value: Any) -> Any:
    if is_dataclass(value) and not isinstance(value, type):
        return _plain(asdict(value))
    if isinstance(value, dict):
        return {str(k): _plain(v) for k, v in value.items()}
    if isinstance(value, list | tuple | set):
        return [_plain(v) for v in value]
    return value


def write_report(report: Report, base: Path | str) -> Path:
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%S")
    path = Path(base) / f"{report.experiment.lower()}-{stamp}"
    path.mkdir(parents=True, exist_ok=True)
    data = _plain(asdict(report))
    (path / "report.json").write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str) + "\n")
    lines = [
        f"# {report.experiment}", "",
        f"- Semilla: {report.seed}",
        f"- Modelo: {report.model}",
        f"- Versión de los prompts: `{report.prompts_version[:12]}`", "",
        "## Hipótesis", "",
        *[f"- {name}: {'cumplida' if ok else 'no cumplida'}" for name, ok in report.hypothesis.items()], "",
        "## Métricas", "", "```json", json.dumps(data["metrics"], indent=2, ensure_ascii=False, default=str), "```",
        "", "## Configuración", "", "```json", json.dumps(data["config"], indent=2, ensure_ascii=False, default=str),
        "```",
    ]
    if report.runs:
        lines += ["", "## Ejecuciones", "", "```json", json.dumps(data["runs"], indent=2, ensure_ascii=False,
                                                                  default=str), "```"]
    (path / "report.md").write_text("\n".join(lines) + "\n")
    return path
