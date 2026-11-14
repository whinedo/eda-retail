"""Punto de entrada de los experimentos: `eda-experiment <exp>`."""

from __future__ import annotations

import pytest

from eda_retail.experiments.cli import parse_args


def test_one_command_per_experiment():
    """Cada experimento se ejecuta con un único comando, con semilla e incidencias configurables"""
    args = parse_args(["e1"])
    assert (args.experiment, args.seed, args.incidents, args.out) == ("e1", 42, 50, "experiments/results")
    assert parse_args(["e2", "--seed", "7", "--incidents", "10"]).incidents == 10
    with pytest.raises(SystemExit):
        parse_args(["e9"])
