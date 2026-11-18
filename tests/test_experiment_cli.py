"""Punto de entrada de los experimentos: `eda-experiment <exp>`."""

from __future__ import annotations

import pytest

from eda_retail.experiments.cli import LLMS, experiment_kwargs, incidents_for, parse_args
from eda_retail.llm.ollama_client import OllamaClient


def test_one_command_per_experiment():
    """Cada experimento se ejecuta con un único comando, con semilla e incidencias configurables"""
    args = parse_args(["e1"])
    assert (args.experiment, args.seed, args.out) == ("e1", 42, "experiments/results")
    assert incidents_for(args) == 50
    assert parse_args(["e2", "--seed", "7", "--incidents", "10"]).incidents == 10
    with pytest.raises(SystemExit):
        parse_args(["e8"])


def test_each_experiment_defaults_to_its_spec_size():
    """Sin --incidents, cada experimento usa el tamaño de su escenario en la spec"""
    assert {e: incidents_for(parse_args([e])) for e in ("e1", "e2", "e3", "e4", "e5", "e6", "e7")} == {
        "e1": 50, "e2": 50, "e3": 30, "e4": 200, "e5": 40, "e6": 50, "e7": 50}
    assert incidents_for(parse_args(["e4", "--incidents", "20"])) == 20


def test_e5_uses_ollama_with_three_repetitions_by_default():
    """E5 se lanza con Ollama y tres repeticiones; --llm oracle lo ejecuta sin coste"""
    kw = experiment_kwargs(parse_args(["e5"]))
    assert isinstance(kw["llm"](None, 1), OllamaClient) and kw["repetitions"] == 3 and "threshold" not in kw
    kw = experiment_kwargs(parse_args(["e5", "--llm", "oracle", "--repetitions", "5", "--threshold", "0.9"]))
    assert kw["llm"] is LLMS["oracle"] and kw["repetitions"] == 5 and kw["threshold"] == 0.9
    assert experiment_kwargs(parse_args(["e1"])) == {"seed": 42, "incidents": 50}


def test_llm_options_only_for_llm_experiments():
    """--llm, --repetitions y --threshold se rechazan en los experimentos con oráculo"""
    with pytest.raises(SystemExit):
        parse_args(["e1", "--llm", "ollama"])


def test_e6_uses_ollama_with_three_repetitions_and_no_threshold():
    """E6 se lanza con Ollama y tres repeticiones; H6 no tiene umbral"""
    kw = experiment_kwargs(parse_args(["e6"]))
    assert isinstance(kw["llm"](None, 1), OllamaClient) and kw["repetitions"] == 3 and kw["incidents"] == 50
    with pytest.raises(SystemExit):
        parse_args(["e6", "--threshold", "0.9"])


def test_each_repetition_uses_its_own_seed_and_the_temperature():
    """Cada repetición con Ollama usa su propia semilla y la temperatura indicada (0.7 por defecto)"""
    factory = experiment_kwargs(parse_args(["e5", "--seed", "10"]))["llm"]
    clients = [factory(None, n) for n in (1, 2, 3)]
    assert [c.seed for c in clients] == [11, 12, 13] and {c.temperature for c in clients} == {0.7}
    factory = experiment_kwargs(parse_args(["e6", "--temperature", "0.2"]))["llm"]
    assert factory(None, 1).temperature == 0.2
    with pytest.raises(SystemExit):
        parse_args(["e1", "--temperature", "0.5"])


def test_experiments_use_the_llm_model_of_the_env_file(tmp_path, monkeypatch):
    """Sin `--env-file`, E5 y E6 usan el LLM_MODEL del `.env` del proyecto, no el valor por defecto"""
    from eda_retail.experiments import cli

    (tmp_path / ".env").write_text("LLM_MODEL=modelo-del-env\n")
    monkeypatch.setenv("LLM_MODEL", "placeholder")  # monkeypatch deshace también lo que cargue `.env`
    monkeypatch.delenv("LLM_MODEL")
    monkeypatch.chdir(tmp_path)
    seen = {}

    def experiment(settings, **kwargs):
        seen["model"] = kwargs["llm"](None, 1).model
        raise SystemExit(0)

    monkeypatch.setitem(cli.EXPERIMENTS, "e5", experiment)
    with pytest.raises(SystemExit):
        cli.main(["e5"])
    assert seen["model"] == "modelo-del-env"
