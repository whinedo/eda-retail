"""E6: every finished run is saved and an interrupted E6 resumes (spec experiments, E6)."""

from __future__ import annotations

import json
from decimal import Decimal
from types import SimpleNamespace

import pytest

from eda_retail.experiments import cli, e6_compare
from eda_retail.experiments.e6_compare import RESUME_HEADER, RESUME_RUNS, run_e6

BASE = SimpleNamespace(llm_price_in_per_1k=Decimal(0), llm_price_out_per_1k=Decimal(0))
RUNS = 2 * 2 * 2  # repetitions x scenarios x versions


class Crash(Exception):
    pass


def _fake_runners(monkeypatch, calls: list, crash_at: int | None = None) -> None:
    def runner(version):
        def run(base, config, data, llm):
            if crash_at is not None and len(calls) == crash_at:
                raise Crash("the Mac went to sleep")
            calls.append((llm.repetition, config.faults.duplicates > 1, version))
            return {"version": version, "closed": 3, "resolved": 3, "escalated": 0, "refunded": 0,
                    "duplicates": 0, "latencies_ms": [float(len(calls))], "llm_calls": 1, "tokens_in": 1,
                    "tokens_out": 1, "cost": 0.0, "elapsed_seconds": 1.0, "escalation_reasons": {}}
        return run

    monkeypatch.setattr(e6_compare, "_run_events", runner("events"))
    monkeypatch.setattr(e6_compare, "_run_sync", runner("sync"))


def _llm(model: str = "qwen", temperature: float = 0.7):
    return lambda data, n: SimpleNamespace(model=model, temperature=temperature, seed=42 + n, repetition=n)


def _e6(path, **kwargs):
    options = {"seed": 42, "incidents": 3, "repetitions": 2, "llm": _llm()} | kwargs
    return run_e6(BASE, checkpoint=path, **options)


def test_resume_after_an_interruption(tmp_path, monkeypatch):
    """Reanudación tras un corte"""
    first: list = []
    _fake_runners(monkeypatch, first, crash_at=5)
    with pytest.raises(Crash):
        _e6(tmp_path)
    saved = (tmp_path / RESUME_RUNS).read_text().splitlines()
    assert len(saved) == 5  # each finished run is saved as soon as it ends

    second: list = []
    _fake_runners(monkeypatch, second)
    report = _e6(tmp_path)

    assert len(second) == RUNS - 5  # only the missing runs
    assert set(first).isdisjoint(second)
    assert len(report.runs) == RUNS  # the report has the saved runs and the new ones
    assert sorted((r["repetition"], r["scenario"], r["version"]) for r in report.runs) == sorted(
        (n, s, v) for n in (1, 2) for s in e6_compare.SCENARIOS for v in ("events", "sync"))
    # the latencies of the saved runs count in the metrics
    assert report.metrics["no_faults"]["events"]["latency_ms"]["mean"] is not None
    assert len((tmp_path / RESUME_RUNS).read_text().splitlines()) == RUNS


@pytest.mark.parametrize("change", [
    {"seed": 7}, {"incidents": 4}, {"scenarios": ("no_faults",)}, {"llm": _llm(model="llama")},
    {"llm": _llm(temperature=0.0)},
])
def test_resume_with_another_configuration(tmp_path, monkeypatch, change):
    """Reanudación con otra configuración"""
    calls: list = []
    _fake_runners(monkeypatch, calls, crash_at=2)
    with pytest.raises(Crash):
        _e6(tmp_path)
    calls.clear()
    _fake_runners(monkeypatch, calls)

    with pytest.raises(ValueError, match="otra configuración"):
        _e6(tmp_path, **change)
    assert calls == []


def test_resume_with_other_prompts(tmp_path, monkeypatch):
    """Reanudación con otra configuración: otros prompts"""
    calls: list = []
    _fake_runners(monkeypatch, calls)
    _e6(tmp_path)
    header = json.loads((tmp_path / RESUME_HEADER).read_text())
    (tmp_path / RESUME_HEADER).write_text(json.dumps(header | {"prompts_version": "old"}))
    calls.clear()

    with pytest.raises(ValueError, match="otra configuración"):
        _e6(tmp_path)
    assert calls == []


def test_a_half_written_last_line_is_run_again(tmp_path, monkeypatch):
    """Una ejecución cortada a medias no se guarda y se repite entera"""
    calls: list = []
    _fake_runners(monkeypatch, calls, crash_at=3)
    with pytest.raises(Crash):
        _e6(tmp_path)
    with (tmp_path / RESUME_RUNS).open("a") as f:
        f.write('{"repetition": 1, "scen')
    calls.clear()
    _fake_runners(monkeypatch, calls)

    assert len(_e6(tmp_path).runs) == RUNS
    assert len(calls) == RUNS - 3


def test_without_a_resume_directory_nothing_is_saved(tmp_path, monkeypatch):
    calls: list = []
    _fake_runners(monkeypatch, calls)
    monkeypatch.chdir(tmp_path)
    assert len(run_e6(BASE, seed=42, incidents=3, repetitions=2, llm=_llm()).runs) == RUNS
    assert list(tmp_path.iterdir()) == []


# CLI


def test_e6_saves_its_runs_in_a_new_resume_directory(tmp_path):
    args = cli.parse_args(["e6", "--llm", "oracle", "--out", str(tmp_path)])
    path = cli.experiment_kwargs(args)["checkpoint"]
    assert path.parent == tmp_path and path.name.startswith("e6-runs-")


def test_resume_uses_the_given_directory(tmp_path):
    (tmp_path / RESUME_HEADER).write_text("{}")
    args = cli.parse_args(["e6", "--resume", str(tmp_path)])
    assert cli.experiment_kwargs(args)["checkpoint"] == tmp_path


def test_resume_needs_a_resume_directory_of_e6(tmp_path, capsys):
    with pytest.raises(SystemExit):
        cli.parse_args(["e6", "--resume", str(tmp_path / "nope")])
    assert "directorio de reanudación" in capsys.readouterr().err


def test_resume_only_in_e6(tmp_path, capsys):
    with pytest.raises(SystemExit):
        cli.parse_args(["e5", "--resume", str(tmp_path)])
    assert "--resume solo se usa en e6" in capsys.readouterr().err
