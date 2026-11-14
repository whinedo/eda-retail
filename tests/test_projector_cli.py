"""Punto de entrada de los proyectores: `eda-projector` y `eda-projector rebuild <vista>`."""

from __future__ import annotations

import pytest

from eda_retail.projectors.cli import parse_args
from eda_retail.projectors.registry import PROJECTORS, build_all


def test_runs_the_four_projectors_each_with_its_position():
    """eda-projector sin argumentos ejecuta los cuatro proyectores, cada uno con su posición"""
    assert sorted(PROJECTORS) == ["audit_trail", "carrier_dashboard", "cost_per_incident", "incident_status"]
    assert sorted(p.name for p in build_all()) == sorted(PROJECTORS)
    args = parse_args([])
    assert args.command is None and args.idle_seconds == 0.2


def test_rebuild_command_takes_a_known_view():
    """eda-projector rebuild solo acepta una de las cuatro vistas"""
    assert parse_args(["rebuild", "audit_trail"]).view == "audit_trail"
    with pytest.raises(SystemExit):
        parse_args(["rebuild", "view_orders"])
