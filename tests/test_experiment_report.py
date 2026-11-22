"""Formato del informe de un experimento (spec experiments, «Informe reproducible»), sin base de datos ni modelo."""

from __future__ import annotations

from eda_retail.experiments.report import NO_HYPOTHESIS, Report, write_report


def _report(hypothesis: dict[str, bool]) -> Report:
    return Report(experiment="E6", seed=42, config={}, model="m", metrics={}, hypothesis=hypothesis,
                  prompts_version="0" * 64)


def _hypothesis_section(path) -> list[str]:
    lines = (path / "report.md").read_text().splitlines()
    start = lines.index("## Hipótesis") + 1
    return [line for line in lines[start:lines.index("## Métricas")] if line]


def test_a_report_without_a_hypothesis_says_why(tmp_path):
    """Informe de un experimento sin umbral"""
    assert _hypothesis_section(write_report(_report({}), tmp_path)) == [NO_HYPOTHESIS]


def test_a_report_with_hypotheses_lists_each_one(tmp_path):
    path = write_report(_report({"H1": True, "H2": False}), tmp_path)

    assert _hypothesis_section(path) == ["- H1: cumplida", "- H2: no cumplida"]
