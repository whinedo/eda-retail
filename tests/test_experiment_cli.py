"""Punto de entrada de los experimentos: `eda-experiment <exp>`."""

from __future__ import annotations

import pytest

from eda_retail.experiments.cli import LLMS, experiment_kwargs, incidents_for, parse_args


def test_one_command_per_experiment():
    """Cada experimento se ejecuta con un único comando, con semilla e incidencias configurables"""
    args = parse_args(["e1"])
    assert (args.experiment, args.seed, args.out) == ("e1", 42, "experiments/results")
    assert incidents_for(args) == 50
    assert parse_args(["e2", "--seed", "7", "--incidents", "10"]).incidents == 10
    with pytest.raises(SystemExit):
        parse_args(["e9"])


def test_each_experiment_defaults_to_its_spec_size():
    """Sin --incidents, cada experimento usa el tamaño de su escenario en la spec"""
    assert {e: incidents_for(parse_args([e])) for e in ("e1", "e2", "e3", "e4", "e5", "e7")} == {
        "e1": 50, "e2": 50, "e3": 30, "e4": 200, "e5": 40, "e7": 50}
    assert incidents_for(parse_args(["e4", "--incidents", "20"])) == 20


def test_e5_uses_ollama_with_three_repetitions_by_default():
    """E5 se lanza con Ollama y tres repeticiones; --llm oracle lo ejecuta sin coste"""
    kw = experiment_kwargs(parse_args(["e5"]))
    assert kw["llm"] is LLMS["ollama"] and kw["repetitions"] == 3 and "threshold" not in kw
    kw = experiment_kwargs(parse_args(["e5", "--llm", "oracle", "--repetitions", "5", "--threshold", "0.9"]))
    assert kw["llm"] is LLMS["oracle"] and kw["repetitions"] == 5 and kw["threshold"] == 0.9
    assert experiment_kwargs(parse_args(["e1"])) == {"seed": 42, "incidents": 50}


def test_llm_options_only_for_llm_experiments():
    """--llm, --repetitions y --threshold se rechazan en los experimentos con oráculo"""
    with pytest.raises(SystemExit):
        parse_args(["e1", "--llm", "ollama"])
