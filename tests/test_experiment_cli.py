"""Punto de entrada de los experimentos: `eda-experiment <exp>`."""

from __future__ import annotations

import pytest

from eda_retail.experiments.cli import incidents_for, parse_args


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
    assert {e: incidents_for(parse_args([e])) for e in ("e1", "e2", "e3", "e4", "e7")} == {
        "e1": 50, "e2": 50, "e3": 30, "e4": 200, "e7": 50}
    assert incidents_for(parse_args(["e4", "--incidents", "20"])) == 20
